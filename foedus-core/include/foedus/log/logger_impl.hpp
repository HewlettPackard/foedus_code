/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/soc/shared_polling.hpp"
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
    epoch_history_head_ = 0;
    epoch_history_count_ = 0;
  }
  void uninitialize() {
    epoch_history_mutex_.uninitialize();
  }


  bool is_epoch_history_empty() const { return epoch_history_count_ == 0; }
  uint32_t get_tail_epoch_history() const {
    return wrap_epoch_history_index(epoch_history_head_ + epoch_history_count_ - 1U);
  }

  static uint32_t wrap_epoch_history_index(uint32_t index) { return index % kMaxEpochHistory; }

  /**
   * @brief Upto what epoch the logger flushed logs in \b all buffers assigned to it.
   * @invariant durable_epoch_.is_valid()
   * @invariant global durable epoch <= durable_epoch_ < global current epoch - 1
   * @details
   * The logger advances this value when it writes out all logs in all buffers in an epoch.
   * The global durable epoch is an aggregate (min) of this value from all loggers.
   * Even when some thread is idle, this value could be advanced upto "global current epoch - 2"
   * because of the way the epoch-chime starts a new epoch. It is guaranteed that no thread
   * will emit any log in that epoch or older.
   */
  std::atomic< Epoch::EpochInteger >  durable_epoch_;
  /**
   * The logger sleeps on this conditional.
   * When someone else (whether in same SOC or other SOC) wants to wake up this logger,
   * they fire this. There is no 'real' condition variable as this is not about correctness.
   * The logger simply wakes on this cond and resumes as far as it wakes up.
   * It might be a spurrious wakeup, but doesn't matter.
   */
  soc::SharedPolling              wakeup_cond_;

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
   */
  Epoch                           marked_epoch_;

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

  /**
   * Check if we can advance the durable epoch of this logger, invoking fsync BEFORE actually
   * advancing it in that case.
   */
  ErrorStack  update_durable_epoch(Epoch new_durable_epoch, bool had_any_log);
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
   * Makes sure no thread is still writing out logs in epochs older than the given epoch
   * (same epoch is fine). Waits (spins) if needed, and also writes out the logs of lagging
   * threads up to new_epoch.one_less().
   */
  ErrorStack  check_lagging_threads(Epoch new_epoch);

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
   * Write out all logs in all buffers for the given epoch.
   * @pre write_epoch == logger's durable_epoch + 1
   * @pre write_epoch < global current epoch - 1 (must not write out logs in current/grace ep)
   * @post logger's durable_epoch is updated to write_epoch if this method successfully returns
   */
  ErrorStack  write_one_epoch(Epoch write_epoch);
  /**
   * Sub-routine of write_one_epoch().
   * Writes out the given piece of the given buffer.
   * This method handles non-aligned starting/ending offsets by padding.
   * @pre from_offset < upto_offset (no wrap around)
   */
  ErrorStack  write_one_epoch_piece(
    const ThreadLogBuffer& buffer,
    Epoch write_epoch,
    uint64_t from_offset,
    uint64_t upto_offset);

  /** Check invariants. This method is wiped out in NDEBUG. */
  void        assert_consistent();
  /** Sanity check on logs to write out. This method is wiped out in NDEBUG. */
  void        assert_written_logs(Epoch write_epoch, const char* logs, uint64_t bytes) const;

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
