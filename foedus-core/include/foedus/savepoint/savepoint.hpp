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
#ifndef FOEDUS_SAVEPOINT_SAVEPOINT_HPP_
#define FOEDUS_SAVEPOINT_SAVEPOINT_HPP_
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/snapshot/snapshot_id.hpp"

namespace foedus {
namespace savepoint {
/**
 * @brief The information we maintain in savepoint manager and externalize to a file.
 * @ingroup SAVEPOINT
 * @details
 * This object is a very compact representation of \e progress of the entire engine.
 * This object holds only a few integers per module to denote upto what we are \e surely done.
 * We update this object for each epoch-based group commit and write out this as an xml
 * durably and atomically.
 */
struct Savepoint CXX11_FINAL : public virtual externalize::Externalizable {
  /**
   * Constructs an empty savepoint.
   */
  Savepoint();

  /**
   * @brief Current epoch of the entire engine.
   * @details
   * This value is advanced by transaction manager periodically.
   * This is equal or larger than all other epoch values below.
   * @invariant Epoch(current_epoch_).is_valid()
   */
  Epoch::EpochInteger             current_epoch_;

  /**
   * @brief Latest epoch whose logs were all flushed to disk.
   * @details
   * Upto this epoch, we can guarantee durability. In other words, transactions
   * are not deemed as "done" until this value reaches their epoch values issued on commit time.
   * While the engine restarts, all log entries in log files after this epoch are \b truncated
   * because there might be some logger that did not finish its writing.
   * @invariant Epoch(durable_epoch_).is_valid()
   * @invariant Epoch(current_epoch_) > Epoch(durable_epoch_)
   */
  Epoch::EpochInteger             durable_epoch_;

  /**
   * The earliest epoch that can exist in this system. it advances only after we
   * do a system-wide compaction. Otherwise, it's kEpochInitialDurable, which is indeed
   * the earliest until the epoch wraps around.
   */
  Epoch::EpochInteger             earliest_epoch_;

  /**
   * The most recent complete snapshot.
   * kNullSnapshotId if no snapshot has been taken yet.
   */
  snapshot::SnapshotId            latest_snapshot_id_;
  /**
   * The most recently snapshot-ed epoch, all logs upto this epoch is safe to delete.
   * If not snapshot has been taken, invalid epoch.
   * This is equivalent to snapshots_.back().valid_entil_epoch_ with empty check.
   */
  Epoch::EpochInteger             latest_snapshot_epoch_;

  /** Offset from which metadata log entries are not gleaned yet */
  uint64_t                        meta_log_oldest_offset_;
  /** Offset upto which metadata log entries are fsynced */
  uint64_t                        meta_log_durable_offset_;

  // for all the following, index is LoggerId

  /**
   * @brief Ordinal of the oldest active log file in each logger.
   * @invariant oldest_log_files_[x] <= current_log_files_[x]
   * @details
   * Each logger writes out files suffixed with ordinal (eg ".0", ".1"...).
   * The older logs files are deactivated and deleted after log gleaner consumes them.
   * This variable indicates the oldest active file for each logger.
   */
  std::vector<log::LogFileOrdinal>    oldest_log_files_;

  /** Indicates the inclusive beginning of active region in the oldest log file. */
  std::vector<uint64_t>               oldest_log_files_offset_begin_;

  /** Indicates the log file each logger is currently appending to. */
  std::vector<log::LogFileOrdinal>    current_log_files_;

  /**
   * Indicates the exclusive end of durable region in the current log file.
   * In other words, epochs are larger than durable_epoch_ from this offset.
   * During restart, current log files are truncated to this size to discard incomplete logs.
   */
  std::vector<uint64_t>               current_log_files_offset_durable_;

  EXTERNALIZABLE(Savepoint);

  /** Populate variables as an initial state. */
  void                                populate_empty(log::LoggerId logger_count);
  /** Tells if the variables are consistent. */
  bool                                consistent(log::LoggerId logger_count) const {
    assert_epoch_values();
    return (current_epoch_ >= durable_epoch_
      && current_epoch_ >= earliest_epoch_
      && durable_epoch_ >= earliest_epoch_
      && meta_log_oldest_offset_ <= meta_log_durable_offset_
      && oldest_log_files_.size() == logger_count
      && oldest_log_files_offset_begin_.size() == logger_count
      && current_log_files_.size() == logger_count
      && current_log_files_offset_durable_.size() == logger_count);
  }

  Epoch  get_durable_epoch() const { return Epoch(durable_epoch_); }
  Epoch  get_current_epoch() const { return Epoch(current_epoch_); }
  Epoch  get_earliest_epoch() const { return Epoch(earliest_epoch_); }
  /** Check invariants on current_epoch_/durable_epoch_ */
  void        assert_epoch_values() const;
};
/** Information in savepoint for one logger. */
struct LoggerSavepointInfo {
  /**
  * @brief Ordinal of the oldest active log file in each logger.
  * @invariant oldest_log_files_[x] <= current_log_files_[x]
  * @details
  * Each logger writes out files suffixed with ordinal (eg ".0", ".1"...).
  * The older logs files are deactivated and deleted after log gleaner consumes them.
  * This variable indicates the oldest active file for each logger.
  */
  log::LogFileOrdinal oldest_log_file_;

  /** Indicates the log file each logger is currently appending to. */
  log::LogFileOrdinal current_log_file_;

  /** Indicates the inclusive beginning of active region in the oldest log file. */
  uint64_t            oldest_log_file_offset_begin_;

  /**
  * Indicates the exclusive end of durable region in the current log file.
  * In other words, epochs are larger than durable_epoch_ from this offset.
  * During restart, current log files are truncated to this size to discard incomplete logs.
  */
  uint64_t            current_log_file_offset_durable_;
};

/**
 * @brief Savepoint that can be stored in shared memory.
 * @ingroup SAVEPOINT
 */
struct FixedSavepoint CXX11_FINAL {
  // only for reinterpret_cast
  FixedSavepoint() CXX11_FUNC_DELETE;
  ~FixedSavepoint() CXX11_FUNC_DELETE;

  Epoch::EpochInteger             current_epoch_;
  Epoch::EpochInteger             durable_epoch_;
  Epoch::EpochInteger             earliest_epoch_;
  snapshot::SnapshotId            latest_snapshot_id_;
  Epoch::EpochInteger             latest_snapshot_epoch_;

  /** Number of NUMA nodes. the information is availble elsewhere, but easier to duplicate here. */
  uint16_t                        node_count_;
  /** Number of loggers per node. same above. */
  uint16_t                        loggers_per_node_count_;

  uint64_t                        meta_log_oldest_offset_;
  uint64_t                        meta_log_durable_offset_;

  /**
   * Stores all loggers' information. We allocate memory enough for the largest number of loggers.
   * In reality, we are just reading/writing a small piece of it.
   * 24b * 64k = 1.5MB.
   */
  LoggerSavepointInfo             logger_info_[1U << 16];

  uint32_t                        get_total_logger_count() const {
    return static_cast<uint32_t>(node_count_) * loggers_per_node_count_;
  }

  /** Write out the content of the given Savepoint to this object */
  void update(uint16_t node_count, uint16_t loggers_per_node_count, const Savepoint& src);
};

}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_HPP_
