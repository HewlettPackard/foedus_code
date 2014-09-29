/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOGGER_IMPL_HPP_
#define FOEDUS_LOG_LOGGER_IMPL_HPP_
#include <stdint.h>

#include <atomic>
#include <iosfwd>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "foedus/attachable.hpp"
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/fs/fwd.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/log/epoch_history.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/log/logger_ref.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/savepoint/fwd.hpp"
#include "foedus/soc/shared_cond.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace log {

/** Shared data of Logger */
struct LoggerControlBlock {
  enum Constants {
    /**
     * Max number of active epoch histories.
     * 64k * 24b = 1536 kb. adjust kLoggerMemorySize according to this.
     */
    kMaxEpochHistory = 1 << 16,
  };

  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  LoggerControlBlock() = delete;
  ~LoggerControlBlock() = delete;

  void initialize() {
    wakeup_cond_.initialize();
    epoch_history_mutex_.initialize();
    stop_requested_ = false;
    marked_epoch_update_requested_ = false;
    epoch_history_head_ = 0;
    epoch_history_count_ = 0;
  }
  void uninitialize() {
    epoch_history_mutex_.uninitialize();
    wakeup_cond_.uninitialize();
  }


  bool is_epoch_history_empty() const { return epoch_history_count_ == 0; }
  uint32_t get_tail_epoch_history() const {
    return wrap_epoch_history_index(epoch_history_head_ + epoch_history_count_ - 1U);
  }

  static uint32_t wrap_epoch_history_index(uint32_t index) { return index % kMaxEpochHistory; }

  /**
   * @brief Upto what epoch the logger flushed logs in \b all buffers assigned to it.
   * @invariant durable_epoch_.is_valid()
   * @invariant global durable epoch <= durable_epoch_ < global current epoch
   * @details
   * Unlike buffer.logger_epoch, this value is continuously maintained by the logger, thus
   * no case of stale values. Actually, the global durable epoch does not advance until all
   * loggers' durable_epoch_ advance.
   * Hence, if some thread is idle or running a long transaction, this value could be larger
   * than buffer.logger_epoch_. Otherwise (when the worker thread is running normal), this value
   * is most likely smaller than buffer.logger_epoch_.
   */
  std::atomic< Epoch::EpochInteger >  durable_epoch_;
  /**
   * The logger sleeps on this conditional.
   * When someone else (whether in same SOC or other SOC) wants to wake up this logger,
   * they fire this. There is no 'real' condition variable as this is not about correctness.
   * The logger simply wakes on this cond and resumes as far as it wakes up.
   * It might be a spurrious wakeup, but doesn't matter.
   */
  soc::SharedCond                     wakeup_cond_;

  /**
   * @brief Upto what epoch the logger has put epoch marker in the log file.
   * @invariant marked_epoch_.is_valid()
   * @invariant marked_epoch_ <= durable_epoch_.one_more()
   * @details
   * Usually, this value is always same as durable_epoch_.one_more().
   * This value becomes smaller than that if the logger had no log to write out
   * when it advanced durable_epoch_. In that case, writing out an epoch marker is a waste
   * (eg when the system is idle for long time, there will be tons of empty epochs),
   * so we do not write out the epoch marker and let this value remain same.
   * When the logger later writes out a log, it checks this value and writes out an epoch mark.
   * @see no_log_epoch_
   */
  Epoch                           marked_epoch_;
  /**
   * Occasionally (only during log gleaning), we need a recent marked epoch immediately.
   * The logger creates one if it sees this true.
   */
  std::atomic<bool>               marked_epoch_update_requested_;

  /** Whether so far this logger has not written out any log since previous epoch switch. */
  bool                            no_log_epoch_;

  // the followings are also shared just for savepoint.

  /**
   * @brief Ordinal of the oldest active log file of this logger.
   * @invariant oldest_ordinal_ <= current_ordinal_
   */
  std::atomic< LogFileOrdinal >   oldest_ordinal_;
  /**
   * @brief Inclusive beginning of active region in the oldest log file.
   * @invariant oldest_file_offset_begin_ % kLogWriteUnitSize == 0 (because we pad)
   */
  std::atomic< uint64_t >         oldest_file_offset_begin_;
  /**
   * @brief Ordinal of the log file this logger is currently appending to.
   */
  std::atomic< LogFileOrdinal >   current_ordinal_;

  /**
   * We called fsync on current file up to this offset.
   * @invariant current_file_durable_offset_ <= current_file->get_current_offset()
   * @invariant current_file_durable_offset_ % kLogWriteUnitSize == 0 (because we pad)
   */
  std::atomic< uint64_t >         current_file_durable_offset_;

  /** Whether this logger should terminate */
  std::atomic<bool>               stop_requested_;

  /** the followings are covered this mutex */
  soc::SharedMutex  epoch_history_mutex_;

  /** index of the oldest history in epoch_histories_ */
  uint32_t          epoch_history_head_;
  /** number of active entries in epoch_histories_ . */
  uint32_t          epoch_history_count_;

  /**
   * Remembers all epoch switching in this logger.
   * This forms a circular buffer starting from epoch_history_head_.
   * After log gleaning, we can move head forward and reduce epoch_history_count_.
   */
  EpochHistory      epoch_histories_[kMaxEpochHistory];
};

/**
 * @brief A log writer that writes out buffered logs to stable storages.
 * @ingroup LOG
 * @details
 * This is a private implementation-details of \ref LOG, thus file name ends with _impl.
 * Do not include this header from a client program unless you know what you are doing.
 */
class Logger final : public DefaultInitializable, public LoggerRef {
 public:
  Logger(
    Engine* engine,
    LoggerControlBlock* control_block,
    LoggerId id,
    thread::ThreadGroupId numa_node,
    uint8_t in_node_ordinal,
    const fs::Path &log_folder,
    const std::vector< thread::ThreadId > &assigned_thread_ids)
  : LoggerRef(engine, control_block, id, numa_node, in_node_ordinal),
    log_folder_(log_folder),
    assigned_thread_ids_(assigned_thread_ids) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  Logger() = delete;
  Logger(const Logger &other) = delete;
  Logger& operator=(const Logger &other) = delete;

  LogFileOrdinal get_current_ordinal() const { return control_block_->current_ordinal_; }
  bool is_stop_requested() const { return control_block_->stop_requested_; }

  std::string             to_string() const;
  friend std::ostream&    operator<<(std::ostream& o, const Logger& v);

 private:
  /**
   * @brief Main routine for logger_thread_.
   * @details
   * This method keeps writing out logs in assigned threads' private buffers.
   * When there are no logs in all the private buffers for a while, it goes into sleep.
   * This method exits when this object's uninitialize() is called.
   */
  void        handle_logger();
  /** handle_logger() keeps calling this with sleeps. */
  ErrorStack  handle_logger_once(bool *more_log_to_process);

  /**
   * Check if we can advance the durable epoch of this logger, invoking fsync BEFORE actually
   * advancing it in that case.
   */
  ErrorStack  update_durable_epoch();
  /**
   * Subroutine of update_durable_epoch to get the minimum durable epoch of assigned loggers.
   * This function is assured to be no-error and instantaneous.
   */
  Epoch calculate_min_durable_epoch();

  /**
   * Moves on to next file if the current file exceeds the configured max size.
   */
  ErrorStack  switch_file_if_required();

  /**
   * Adds a log entry to annotate the switch of epoch.
   * Individual log entries do not have epoch information, relying on this.
   */
  ErrorStack  log_epoch_switch(Epoch new_epoch);

  /**
   * Whenever we restart or switch to a new file, we call this method to write out an epoch marker
   * so that all log files start with an epoch marker.
   */
  ErrorStack  write_dummy_epoch_mark();

  /**
   * Writes out the given buffer upto the offset.
   * This method handles non-aligned starting/ending offsets by padding, and also handles wrap
   * around.
   */
  ErrorStack  write_log(ThreadLogBuffer* buffer, uint64_t upto_offset);

  /** Check invariants. This method should be wiped in NDEBUG. */
  void        assert_consistent();

  const fs::Path                  log_folder_;
  const std::vector< thread::ThreadId > assigned_thread_ids_;

  std::thread                     logger_thread_;

  /**
   * @brief A local and very small aligned buffer to pad log entries to 4kb.
   * @details
   * The logger directly reads from the assigned threads' own buffer when it writes out to file
   * because we want to avoid doubling the overhead. As the buffer is exclusively assigned to
   * this logger, there is no risk to directly pass the thread's buffer \e except the case
   * where we are writing out less than 4kb, which happens on:
   *  \li at the beginning/end of an epoch log block
   *  \li when the logger is really catching up well (the thread has less than 4kb
   * commited-but-non-durable log)
   * In these cases, we need to pad it to 4kb. So, we copy the thread's buffer's content to this
   * buffer and fill the rest (at the end or at the beginning, or both).
   */
  memory::AlignedMemory           fill_buffer_;

  /**
   * @brief The log file this logger is currently appending to.
   */
  fs::DirectIoFile*               current_file_;
  /**
   * [log_folder_]/[id_]_[current_ordinal_].log.
   */
  fs::Path                        current_file_path_;

  std::vector< thread::Thread* >  assigned_threads_;

  /** protects log_epoch_switch() from concurrent accesses. */
  std::mutex                      epoch_switch_mutex_;
};
static_assert(
  sizeof(LoggerControlBlock) <= soc::NodeMemoryAnchors::kLoggerMemorySize,
  "LoggerControlBlock is too large.");
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOGGER_IMPL_HPP_
