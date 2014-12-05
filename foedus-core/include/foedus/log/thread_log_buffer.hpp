/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_THREAD_LOG_BUFFER_HPP_
#define FOEDUS_LOG_THREAD_LOG_BUFFER_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace log {
/**
 * @brief A thread-buffer's epoch marker, which indicates where a thread switched an epoch.
 * @ingroup LOG
 * @see foedus::log::ThreadLogBuffer
 * @see foedus::log::ThreadLogBufferMeta
 * @details
 * When the thread publishes a commited log with new epoch, it adds this mark for logger.
 * Unlike logger's epoch mark, we don't write out actual log entry for this.
 * Epoch mark is stored for only non-durable regions. Thus, the logger doesn't have to
 * worry about whether the marked offset is still valid or not.
 */
struct ThreadEpockMark {
  /**
    * The value of new_epoch_ of the previous mark.
    * This is not currently used except sanity checks.
    * Populated as soon as this mark becomes the current and immutable since then.
    * @invariant old_epoch_.is_valid()
    */
  Epoch       old_epoch_;
  /**
    * The epoch of log entries this mark represents.
    * Because a thread might have no log in some epoch, this might be larger than old_epoch+1.
    * It might be old_epoch+1000 etc.
    * Populated as soon as this mark becomes the current and immutable since then.
    * @invariant new_epoch_.is_valid()
    * @invariant old_epoch < new_epoch_
    */
  Epoch       new_epoch_;
  /**
    * Where the new epoch starts.
    * Populated as soon as this mark becomes the current and immutable since then.
    */
  uint64_t    offset_begin_;
  /**
   * Where the new epoch ends.
   * Populated \e 1) when the thread retires the epoch ("0" while this mark is current)
   * \b OR \e 2) when the logger aggressively determines that the thread is idle and will not
   * emit any more logs in this epoch. Thus, when the thread writes this value to move on to next
   * entry, the old value must be 0 or the same value it were to write.
   */
  uint64_t    offset_end_;

  ThreadEpockMark() {
    old_epoch_.reset();
    new_epoch_.reset();
    offset_begin_ = 0;
    offset_end_ = 0;
  }
  ThreadEpockMark(Epoch old_epoch, Epoch new_epoch, uint64_t offset_begin) {
    old_epoch_ = old_epoch;
    new_epoch_ = new_epoch;
    offset_begin_ = offset_begin;
    offset_end_ = 0;
  }
  friend std::ostream& operator<<(std::ostream& o, const ThreadEpockMark& v);
};

/**
 * @brief Metadata part of ThreadLogBuffer, without the actual buffer which is way way larger.
 * @ingroup LOG
 * @details
 * @par Circular Log Buffer
 * This class forms a circular buffer used by log appender (Thread, which we call "the thread" or
 * "this thread" all over the place), log writer (Logger),
 * and log gleaner (LogGleaner). We maintain four offsets on the buffer.
 * <table>
 * <tr><th>Marker</th><th>Read by</th><th>Written by</th><th>Description</th></tr>
 * <tr><td> offset_head_ (\b H) </td><td>Thread</td><td>Thread, LogGleaner</td>
 *   <td> @copydoc offset_head_ </td></tr>
 * <tr><td> offset_durable_ (\b D) </td><td>Thread, LogGleaner</td><td>Logger</td>
 *   <td> @copydoc offset_durable_ </td></tr>
 * <tr><td> offset_committed_ (\b C) </td><td>Thread, Logger</td><td>Thread</td>
 *   <td> @copydoc offset_committed_ </td></tr>
 * <tr><td> offset_tail_ (\b T) </td><td>Thread</td><td>Thread</td>
 *   <td> @copydoc offset_tail_ </td></tr>
 * </table>
 *
 * @par Circular Epoch Mark array
 * This class also forms a circulary array of ThreadEpockMark to mark where the thread switched
 * epochs to help the logger. oldest_mark_index_ (inclusive) to current_mark_index_ (exclusive) are
 * the loggable entries that can be written out by the logger. current_mark_index_ is now being
 * written by the thread, so the logger avoids writing logs in that epoch (it's doable to
 * write out logs in this epoch, but we avoid it to simplify and also maximize the batch-size
 * of I/O in the logger). Further, the following section details one special case.
 *
 * @par Epoch Mark while the thread is idle
 * This is complicated, thus worth a detailed explanation.
 * As described in the xct manager, we use the in-commit epoch guard to advance epochs even when
 * some thread is idle and cannot advance the thread's latest_epoch. And, the logger write out
 * logs up to current global epoch - 2, so this class is guaranteed to not have any real race (eg
 * the thread now writing a new log entry or a new epoch mark for the written_epoch).
 * Hence, get_logs_to_write() and on_log_written() handle the following situations:
 *  \li Case 1) The oldest mark has \b new_epoch>written_epoch :
 * This means the thread is going faster than the logger, and there is no log in the written_epoch.
 * Empty log range by get_logs_to_write() and nothing to do in on_log_written().
 *  \li Case 2) The oldest mark has \b new_epoch==written_epoch :
 * The mark is exactly the one the logger is looking for, but there are two cases.
 *  \li Case 2-a) The oldest mark is not current mark (\b oldest!=current) : Quite usual.
 * Valid log range by get_logs_to_write() and on_log_written() increments oldest_mark_index_.
 *  \li Case 2-b) The oldest mark is the current mark (\b oldest==current) : This is the case
 * where the thread is idle for a while after writing a log in written_epoch.
 * Because oldest mark is the current mark, offset_end_ is not populated yet.
 * However, because the thread never emits new logs in written_epoch, the logger can safely populate
 * it with offset_committed_ (with a few fences). on_log_written() does \b nothing in this case.
 *  \li Case 3) The oldest mark has \b new_epoch<written_epoch : This happens in the next logger
 * visit after 2-b. If still oldest==current, empty log range and nothing to do in on_log_written().
 * If oldest!=current, we advance oldest as much as possible to determine log range.
 * Don't worry about synchronization here. Again, the thread emits log only in current or current-1
 * epoch, so there is no chance it adds a new epoch mark with new_epoch<=writen_epoch.
 *
 * @see foedus::log::ThreadEpockMark
 * @see foedus::log::ThreadLogBuffer
 */
struct ThreadLogBufferMeta final {
  enum Constants {
    /**
     * @brief Each thread can have at most this number of epochs in this log buffer.
     * @details
     * For example, if the assigned logger made the logs of this thread durable up to ep-3,
     * the thread must not write out further logs in ep-67. It has to wait until the logger
     * catches up. As far as the user assigns a sufficient number of loggers, it shouldn't cause
     * any issue.
     */
    kMaxNonDurableEpochs = 64,
  };

  ThreadLogBufferMeta();

  void  assert_consistent() const;
  friend std::ostream& operator<<(std::ostream& o, const ThreadLogBufferMeta& v);

  static uint32_t increment_mark_index(uint32_t index) {
    return (index + 1) % kMaxNonDurableEpochs;
  }

  thread::ThreadId                thread_id_;
  uint16_t                        padding1_;
  uint32_t                        padding2_;

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

  /**
   * @brief This marks the position where log entries start.
   * @details
   * This private log buffer is a circular buffer where the \e head is eaten by log gleaner.
   * However, log gleaner is okay to get behind, reading from log file instead (but slower).
   * Thus, offset_head_ is advanced either by log gleaner or this thread.
   * If the latter happens, log gleaner has to give up using in-memory logs and instead
   * read from log files.
   */
  uint64_t                        offset_head_;

  /**
   * @brief This marks the position upto which the log writer durably wrote out to log files.
   * @details
   * Everything after this position must not be discarded because they are not yet durable.
   * When the log writer reads log entries after here, writes them to log file, and calls fsync,
   * this variable is advanced by the log writer.
   * This variable is read by this thread to check the end of the circular buffer.
   */
  uint64_t                        offset_durable_;

  /**
   * @brief This marks the position upto which transaction logs are committed by the thread.
   * @details
   * Log writers can safely read log entries and write them to log files up to this place.
   * When the transaction commits, this value is advanced by the thread.
   * The only possible update pattern to this variable is \b advance by this thread.
   * Thus, the log writer can safely read this variable without any fence or lock
   * thanks to regular (either old value or new value, never garbage) read of 64-bit.
   */
  uint64_t                        offset_committed_;

  /**
   * @brief The current cursor to which next log will be written.
   * @details
   * This is the location the current transaction of this thread is writing to \b before commit.
   * When the transaction commits, offset_committed_ catches up with this.
   * When the transaction aborts, this value rolls back to offset_committed_.
   * Only this thread reads/writes to this variable. No other threads access this.
   */
  uint64_t                        offset_tail_;

  /** Circular array of epoch marks of a thread log buffer. */
  ThreadEpockMark                 thread_epoch_marks_[kMaxNonDurableEpochs];
  /**
   * Array index in thread_epoch_marks_ that indicates the oldest epoch mark
   * to be consumed by the logger.
   * @invariant 0 <= oldest_mark_index_ < kMaxNonDurableEpochs
   * @note Written by logger only. Read by logger and thread.
   */
  uint32_t                        oldest_mark_index_;
  /**
   * Array index in thread_epoch_marks_ that indicates the current epoch being appended by the
   * thread, so the logger must avoid touching the epoch.
   * @invariant 0 <= oldest_mark_index_< kMaxNonDurableEpochs
   * @note ThreadLogBuffer guarantees that the thread does not create a new log when
   * current_mark_index_ might reach "oldest_mark_index_ + kMaxNonDurableEpochs".
   * Thus, "current_mark_index_ == oldest_mark_index_" means empty, not full.
   * @note Written by thread only. Read by logger and thread.
   */
  uint32_t                        current_mark_index_;
};

/**
 * @brief A thread-local log buffer.
 * @ingroup LOG
 * @details
 * This is a private implementation-details of \ref LOG, thus file name ends with _impl.
 * Do not include this header from a client program unless you know what you are doing.
 *
 * @par Circular buffers
 * @copydoc foedus::log::ThreadLogBufferMeta
 * @par Epoch marks
 * @copydoc foedus::log::ThreadEpockMark
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
  static void advance(uint64_t buffer_size, uint64_t *target, uint64_t amount) ALWAYS_INLINE {
    ASSERT_ND(*target < buffer_size);
    ASSERT_ND(amount < buffer_size);
    *target += amount;
    if (*target >= buffer_size) {
      *target -= buffer_size;
    }
  }

  ThreadLogBuffer(Engine* engine, thread::ThreadId thread_id);
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ThreadLogBuffer() = delete;
  ThreadLogBuffer(const ThreadLogBuffer &other) = delete;
  ThreadLogBuffer& operator=(const ThreadLogBuffer &other) = delete;

  void        assert_consistent() const {
#ifndef NDEBUG
    meta_.assert_consistent();
#endif  // NDEBUG
  }
  thread::ThreadId get_thread_id() const { return meta_.thread_id_; }

  /**
   * @brief Reserves a space for a new (uncommitted) log entry at the tail.
   * @param[in] log_length byte size of the log. You have to give it beforehand.
   * @details
   * If the circular buffer's tail reaches the head, this method might block.
   * But it will be rare as we release a large region of buffer at each time.
   */
  char*       reserve_new_log(uint16_t log_length) ALWAYS_INLINE {
    if (UNLIKELY(log_length + meta_.offset_tail_ >= meta_.buffer_size_)) {
      // now we need wrap around. to simplify, let's avoid having a log entry spanning the
      // end of the buffer. put a filler log to fill the rest.
      fillup_tail();
      ASSERT_ND(meta_.offset_tail_ == 0);
    }
    // also make sure tail isn't too close to head (full). in that case, we wait for loggers
    if (UNLIKELY(
      head_to_tail_distance() + log_length >= meta_.buffer_size_safe_)) {
      wait_for_space(log_length);
      ASSERT_ND(head_to_tail_distance() + log_length < meta_.buffer_size_safe_);
    }
    ASSERT_ND(head_to_tail_distance() + log_length < meta_.buffer_size_safe_);
    char *buffer = buffer_ + meta_.offset_tail_;
    advance(meta_.buffer_size_, &meta_.offset_tail_, log_length);
    return buffer;
  }
  uint64_t    head_to_tail_distance() const ALWAYS_INLINE {
    return distance(meta_.buffer_size_, meta_.offset_head_, meta_.offset_tail_);
  }

  /**
   * Called when the current transaction is successfully committed.
   */
  void        publish_committed_log(Epoch commit_epoch) ALWAYS_INLINE {
    Epoch last_epoch = get_last_epoch();
    ASSERT_ND(commit_epoch >= last_epoch);
    if (UNLIKELY(commit_epoch > last_epoch)) {
      on_new_epoch_observed(commit_epoch);  // epoch switches!
    } else if (UNLIKELY(commit_epoch < last_epoch)) {
      // This MUST not happen because it means an already durable epoch received a new log!
      crash_stale_commit_epoch(commit_epoch);
    }
    meta_.offset_committed_ = meta_.offset_tail_;
  }

  /** Called when the current transaction aborts. */
  void        discard_current_xct_log() {
    meta_.offset_tail_ = meta_.offset_committed_;
  }
  Epoch       get_last_epoch() const {
    return meta_.thread_epoch_marks_[meta_.current_mark_index_].new_epoch_;
  }

  /** Called when we have to wait till offset_head_ advances so that we can put new logs. */
  void        wait_for_space(uint16_t required_space);

  /** @copydoc foedus::log::ThreadLogBufferMeta::offset_head_ */
  uint64_t    get_offset_head() const { return meta_.offset_head_; }

  /** @copydoc foedus::log::ThreadLogBufferMeta::offset_durable_ */
  uint64_t    get_offset_durable() const { return meta_.offset_durable_; }

  /** @copydoc foedus::log::ThreadLogBufferMeta::offset_committed_ */
  uint64_t    get_offset_committed() const { return meta_.offset_committed_; }

  /** @copydoc foedus::log::ThreadLogBufferMeta::offset_tail_ */
  uint64_t    get_offset_tail() const { return meta_.offset_tail_; }


  /** Returns the state of this buffer. */
  const ThreadLogBufferMeta& get_meta() const { return meta_; }
  const char* get_buffer() const { return buffer_; }

  struct OffsetRange {
    uint64_t begin_;
    uint64_t end_;
    bool is_empty() const { return begin_ == end_; }
  };
  /** Returns begin/end offsets of logs in the given epoch. It might be empty (0/0). */
  OffsetRange get_logs_to_write(Epoch written_epoch);
  /** Called when the logger wrote out all logs in the given epoch, advancing oldest_mark_index_ */
  void        on_log_written(Epoch written_epoch);


  friend std::ostream& operator<<(std::ostream& o, const ThreadLogBuffer& v);

 private:
  /**
   * called from publish_committed_log whenever the thread observes a commit_epoch that is
   * larger than last_epoch_.
   */
  void        on_new_epoch_observed(Epoch commit_epoch);

  /** Crash with a detailed report. */
  void        crash_stale_commit_epoch(Epoch commit_epoch);

  /** Called from reserve_new_log() to fillup the end of the circular buffer with padding. */
  void        fillup_tail();

  Engine* const             engine_;
  ThreadLogBufferMeta       meta_;


  /**
   * @brief The in-memory log buffer given to this thread.
   * @details
   * This forms a circular buffer to which \e this thread (the owner of this buffer)
   * will append log entries, and from which log writer will read from head.
   * This is a piece of NumaNodeMemory#thread_buffer_memory_.
   */
  char*                     buffer_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_THREAD_LOG_BUFFER_HPP_

