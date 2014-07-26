/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_THREAD_LOG_BUFFER_IMPL_HPP_
#define FOEDUS_LOG_THREAD_LOG_BUFFER_IMPL_HPP_
#include <stdint.h>

#include <iosfwd>
#include <list>
#include <mutex>
#include <vector>

#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace log {
/**
 * @brief A thread-local log buffer.
 * @ingroup LOG
 * @details
 * This is a private implementation-details of \ref LOG, thus file name ends with _impl.
 * Do not include this header from a client program unless you know what you are doing.
 *
 * @section CIR Circular Log Buffer
 * This class forms a circular buffer used by log appender (Thread, which we call "the thread" or
 * "this thread" all over the place), log writer (Logger),
 * and log gleaner (LogGleaner). We maintain four offsets on the buffer.
 * <table>
 * <tr><th>Marker</th><th>Read by</th><th>Written by</th><th>Description</th></tr>
 * <tr><td> get_offset_head() </td><td>Thread</td><td>Thread, LogGleaner</td>
 *   <td> @copydoc get_offset_head() </td></tr>
 * <tr><td> get_offset_durable() </td><td>Thread, LogGleaner</td><td>Logger</td>
 *   <td> @copydoc get_offset_durable() </td></tr>
 * <tr><td> get_offset_committed() </td><td>Logger</td><td>Thread</td>
 *   <td> @copydoc get_offset_committed() </td></tr>
 * <tr><td> get_offset_tail() </td><td>Thread</td><td>Thread</td>
 *   <td> @copydoc get_offset_tail() </td></tr>
 * </table>
 *
 * @section MARKER Epoch Marker
 * @copydoc ThreadEpockMark
 */
class ThreadLogBuffer final : public DefaultInitializable {
 public:
  friend class Logger;
  /**
   * @brief Subtract operator, considering wrapping around.
   * @attention Be careful. As this is a circular buffer, from and to are \b NOT commutative.
   * actually, distance(from, to) = buffer_size - distance(to, from).
   */
  static uint64_t distance(uint64_t buffer_size, uint64_t from, uint64_t to) ALWAYS_INLINE {
    ASSERT_ND(from < buffer_size);
    ASSERT_ND(to < buffer_size);
    if (to >= from) {
      return to - from;
    } else {
      return to + buffer_size - from;  // wrap around
    }
  }
  /** Addition operator, considering wrapping around. */
  static void advance(uint64_t buffer_size, uint64_t *target, uint64_t advance) ALWAYS_INLINE {
    ASSERT_ND(*target < buffer_size);
    ASSERT_ND(advance < buffer_size);
    *target += advance;
    if (*target >= buffer_size) {
      *target -= buffer_size;
    }
  }
  /**
   * @brief Indicates where this thread switched an epoch.
   * @details
   * When the thread publishes a commited log with new epoch, it adds this mark for logger.
   * Unlike logger's epoch mark, we don't write out actual log entry for this.
   * Epoch mark is stored for only non-durable regions. Thus, the logger doesn't have to
   * worry about whether the marked offset is still valid or not.
   */
  struct ThreadEpockMark {
    /**
     * The value of the thread's last_epoch_ before the switch.
     * This is not currently used except sanity checks.
     */
    Epoch       old_epoch_;
    /**
     * The value of the thread's last_epoch_ after the switch.
     */
    Epoch       new_epoch_;
    /**
     * Where the new epoch starts.
     * @invariant offset_durable_ <= offset_epoch_begin_ < offset_committed_.
     */
    uint64_t    offset_epoch_begin_;

    friend std::ostream& operator<<(std::ostream& o, const ThreadEpockMark& v);
  };

  ThreadLogBuffer(Engine* engine, thread::ThreadId thread_id);
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ThreadLogBuffer() = delete;
  ThreadLogBuffer(const ThreadLogBuffer &other) = delete;
  ThreadLogBuffer& operator=(const ThreadLogBuffer &other) = delete;

  void        assert_consistent() const;
  thread::ThreadId get_thread_id() const { return thread_id_; }

  /**
   * @brief Reserves a space for a new (uncommitted) log entry at the tail.
   * @param[in] log_length byte size of the log. You have to give it beforehand.
   * @details
   * If the circular buffer's tail reaches the head, this method might block.
   * But it will be rare as we release a large region of buffer at each time.
   */
  char*       reserve_new_log(uint16_t log_length) ALWAYS_INLINE {
    if (UNLIKELY(log_length + offset_tail_ >= buffer_size_)) {
      // now we need wrap around. to simplify, let's avoid having a log entry spanning the
      // end of the buffer. put a filler log to fill the rest.
      fillup_tail();
      ASSERT_ND(offset_tail_ == 0);
    } else if (UNLIKELY(
      head_to_tail_distance() + log_length >= buffer_size_safe_)) {
      wait_for_space(log_length);
    }
    ASSERT_ND(head_to_tail_distance() + log_length < buffer_size_safe_);
    char *buffer = buffer_ + offset_tail_;
    advance(buffer_size_, &offset_tail_, log_length);
    return buffer;
  }
  uint64_t    head_to_tail_distance() const ALWAYS_INLINE {
    return distance(buffer_size_, offset_head_, offset_tail_);
  }

  /**
   * Called when the current transaction is successfully committed.
   */
  void        publish_committed_log(Epoch commit_epoch) ALWAYS_INLINE {
    ASSERT_ND(commit_epoch >= last_epoch_);
    if (UNLIKELY(commit_epoch > last_epoch_)) {
      on_new_epoch_observed(commit_epoch);  // epoch switches!
    } else if (UNLIKELY(commit_epoch < logger_epoch_)) {
      // This MUST not happen because it means an already durable epoch received a new log!
      crash_stale_commit_epoch(commit_epoch);
    }
    offset_committed_ = offset_tail_;
  }

  /** Called when the current transaction aborts. */
  void        discard_current_xct_log() {
    offset_tail_ = offset_committed_;
  }

  /** Called when we have to wait till offset_head_ advances so that we can put new logs. */
  void        wait_for_space(uint16_t required_space);

  /**
   * @brief This marks the position where log entries start.
   * @details
   * This private log buffer is a circular buffer where the \e head is eaten by log gleaner.
   * However, log gleaner is okay to get behind, reading from log file instead (but slower).
   * Thus, offset_head_ is advanced either by log gleaner ohttps://drive.google.com/?ddrp=1#my-driver this thread.
   * If the latter happens, log gleaner has to give up using in-memory logs and instead
   * read from log files.
   */
  uint64_t    get_offset_head() const { return offset_head_; }
  /** @copydoc get_offset_head() */
  void        set_offset_head(uint64_t value) { offset_head_ = value; }

  /**
   * @brief This marks the position upto which the log writer durably wrote out to log files.
   * @details
   * Everything after this position must not be discarded because they are not yet durable.
   * When the log writer reads log entries after here, writes them to log file, and calls fsync,
   * this variable is advanced by the log writer.
   * This variable is read by this thread to check the end of the circular buffer.
   */
  uint64_t    get_offset_durable() const { return offset_durable_; }
  /** @copydoc get_offset_durable() */
  void        set_offset_durable(uint64_t value) { offset_durable_ = value; }

  /**
   * @brief This marks the position upto which transaction logs are committed by the thread.
   * @details
   * Log writers can safely read log entries and write them to log files up to this place.
   * When the transaction commits, this value is advanced by the thread.
   * The only possible update pattern to this variable is \b advance by this thread.
   * Thus, the log writer can safely read this variable without any fence or lock
   * thanks to regular (either old value or new value, never garbage) read of 64-bit.
   */
  uint64_t    get_offset_committed() const { return offset_committed_; }

  /**
   * @brief The current cursor to which next log will be written.
   * @details
   * This is the location the current transaction of this thread is writing to \b before commit.
   * When the transaction commits, offset_committed_ catches up with this.
   * When the transaction aborts, this value rolls back to offset_committed_.
   * Only this thread reads/writes to this variable. No other threads access this.
   */
  uint64_t    get_offset_tail() const { return offset_tail_; }

  /**
   * @brief Iterates over the log entries after committed marker to return the list of log
   * entries.
   * @details
   * This assumes the caller is the thread, so no race is possible.
   */
  void        list_uncommitted_logs(std::vector<char*> *out);

  friend std::ostream& operator<<(std::ostream& o, const ThreadLogBuffer& v);

 private:
  /**
   * called from publish_committed_log whenever the thread observes a commit_epoch that is
   * larger than last_epoch_.
   */
  void        on_new_epoch_observed(Epoch commit_epoch);

  /** Crash with a detailed report. */
  void        crash_stale_commit_epoch(Epoch commit_epoch);

  /**
   * Called from logger to eat an epoch mark to advance logger_epoch_.
   * @return whether any epoch mark was consumed.
   * @pre logger_epoch_open_ended_ || logger_epoch_ends_ == offset_durable_
   * meaning the logger has to be waiting for the thread to publish an epoch mark.
   */
  bool        consume_epoch_mark();
  /** Loop consume_epoch_mark() until the logger doesn't have to. */
  void        consume_epoch_mark_as_many();

  /** Called from reserve_new_log() to fillup the end of the circular buffer with padding. */
  void        fillup_tail();

  void        advance_offset_durable(uint64_t amount) {
    advance(buffer_size_, &offset_durable_, amount);
  }

  Engine*                         engine_;
  thread::ThreadId                thread_id_;

  memory::AlignedMemorySlice      buffer_memory_;
  /**
   * @brief The in-memory log buffer given to this thread.
   * @details
   * This forms a circular buffer to which \e this thread (the owner of this buffer)
   * will append log entries, and from which log writer will read from head.
   * This is a piece of NumaNodeMemory#thread_buffer_memory_.
   */
  char*                           buffer_;
  /** Size of the buffer assigned to this thread. */
  uint64_t                        buffer_size_;
  /**
   * @brief buffer_size_ - 64.
   * @details
   * We always leave some \e hole between offset_tail_ and offset_head_
   * to avoid the case offset_tail_ == offset_head_ (log empty? or log full?).
   * One classic way to handle this case is to store \e count rather than offsets, but
   * it makes synchronization between log writer and this thread expensive.
   * Rather, we sacrifice a negligible space.
   */
  uint64_t                        buffer_size_safe_;

  /** @copydoc get_offset_head() */
  uint64_t                        offset_head_;
  /** @copydoc get_offset_durable() */
  uint64_t                        offset_durable_;
  /** @copydoc get_offset_committed() */
  uint64_t                        offset_committed_;
  /** @copydoc get_offset_tail() */
  uint64_t                        offset_tail_;

  /**
   * @brief The epoch of the last transaction on \e this thread.
   * @invariant last_epoch_.is_valid() (initial value is obtained from savepoint.current)
   * @details
   * It might be older than the global current epoch. Especially, when the thread was idle for
   * a while, this might by WAY older than the global current epoch.
   * This is only read/written by this thread.
   */
  Epoch                           last_epoch_;

  /**
   * @brief The epoch the logger is currently flushing.
   * @invariant logger_epoch_.is_valid() (initial value is obtained from savepoint.current)
   * @invariant last_epoch_ >= logger_epoch_
   * @details
   * The logger writes out the log entries in this epoch.
   * This is only read/written by the logger and updated when the logger consumes ThreadEpockMark.
   *
   * This value might be stale when the worker thread is idle for a while. The logger can't
   * advance this value because the worker can't publish epoch marks. In that case, the logger
   * leaves this value stale, but it reports a larger durable epoch by detecting that
   * the thread is idle with the in_commit_log_epoch guard. see Logger::update_durable_epoch().
   */
  Epoch                           logger_epoch_;
  /**
   * @brief Whether the logger is aware of where log entries for logger_epoch_ ends.
   * @details
   * For example, when the global current epoch is 3 and this thread has already written some
   * log in epoch-3, the logger will be aware of where log entries for epoch-2 end via the
   * epoch mark. However, the logger has no idea where log entries for epoch-3
   * will end because this thread will still write out more logs in the epoch!
   * In other words, this value is false if the logger is lagging behind, true if it's catching
   * up well.
   */
  bool                            logger_epoch_open_ended_;
  /**
   * The position where log entries for logger_epoch_ ends (exclusive).
   * The value is undefined when logger_epoch_open_ended_ is true.
   * @invariant logger_epoch_ends_ >= offset_durable_
   */
  uint64_t                        logger_epoch_ends_;

  /**
   * @brief Currently active epoch marks that are waiting to be consumed by the logger.
   * @details
   * The older marks come first. For example, it might be like this:
   * \li offset_head_=0, offset_durable_=128, last_epoch_=6, logger_epoch_=3.
   * \li Mark 0: Switched from epoch-3 to epoch-4 at offset=128.
   * \li Mark 1: Switched from epoch-4 to epoch-5 at offset=1024.
   * \li Mark 2: Switched from epoch-5 to epoch-6 at offset=4096.
   * Then, logger comes by and consumes/removes Mark-0, writes out until offset 1024, setting
   * offset_durable_=1024, logger_epoch_=4.
   *
   * In another example where the logger is well catching up with this thread, this list
   * might be empty. In that case, logger_epoch_open_ended_ is true.
   */
  std::list< ThreadEpockMark >    thread_epoch_marks_;

  /**
   * @brief Protects all accesses to thread_epoch_marks_.
   * @details
   * We don't have to access thread_epoch_marks_ so often; only when an epoch switches and
   * when a logger comes by, which handles a bulk of log entries at once. Thus, this mutex and
   * list above won't be a bottleneck.
   */
  std::mutex                      thread_epoch_marks_mutex_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_THREAD_LOG_BUFFER_IMPL_HPP_

