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
#include "foedus/externalize/externalizable.hpp"
#include "foedus/savepoint/savepoint.hpp"
namespace foedus {
namespace savepoint {
Savepoint::Savepoint() {
}

void Savepoint::assert_epoch_values() const {
  ASSERT_ND(Epoch(current_epoch_).is_valid());
  ASSERT_ND(Epoch(durable_epoch_).is_valid());
  ASSERT_ND(Epoch(current_epoch_) > Epoch(durable_epoch_));
  ASSERT_ND(!Epoch(latest_snapshot_epoch_).is_valid() ||
    Epoch(latest_snapshot_epoch_) <= Epoch(durable_epoch_));
}

ErrorStack Savepoint::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, current_epoch_);
  EXTERNALIZE_LOAD_ELEMENT(element, durable_epoch_);
  EXTERNALIZE_LOAD_ELEMENT(element, latest_snapshot_id_);
  EXTERNALIZE_LOAD_ELEMENT(element, latest_snapshot_epoch_);
  EXTERNALIZE_LOAD_ELEMENT(element, meta_log_oldest_offset_);
  EXTERNALIZE_LOAD_ELEMENT(element, meta_log_durable_offset_);
  EXTERNALIZE_LOAD_ELEMENT(element, oldest_log_files_);
  EXTERNALIZE_LOAD_ELEMENT(element, oldest_log_files_offset_begin_);
  EXTERNALIZE_LOAD_ELEMENT(element, current_log_files_);
  EXTERNALIZE_LOAD_ELEMENT(element, current_log_files_offset_durable_);
  assert_epoch_values();
  return kRetOk;
}

ErrorStack Savepoint::save(tinyxml2::XMLElement* element) const {
  assert_epoch_values();
  CHECK_ERROR(insert_comment(element, "progress of the entire engine"));

  EXTERNALIZE_SAVE_ELEMENT(element, current_epoch_, "Current epoch of the entire engine.");
  EXTERNALIZE_SAVE_ELEMENT(element, durable_epoch_,
               "Latest epoch whose logs were all flushed to disk");
  EXTERNALIZE_SAVE_ELEMENT(element, latest_snapshot_id_, "The most recent complete snapshot.");
  EXTERNALIZE_SAVE_ELEMENT(element, latest_snapshot_epoch_,
              "The most recently snapshot-ed epoch, all logs upto this epoch is safe to delete.");
  EXTERNALIZE_SAVE_ELEMENT(element, meta_log_oldest_offset_,
               "Offset from which metadata log entries are not gleaned yet");
  EXTERNALIZE_SAVE_ELEMENT(element, meta_log_durable_offset_,
               "Offset upto which metadata log entries are fsynced");
  EXTERNALIZE_SAVE_ELEMENT(element, oldest_log_files_,
               "Ordinal of the oldest active log file in each logger");
  EXTERNALIZE_SAVE_ELEMENT(element, oldest_log_files_offset_begin_,
          "Indicates the inclusive beginning of active region in the oldest log file");
  EXTERNALIZE_SAVE_ELEMENT(element, current_log_files_,
               "Indicates the log file each logger is currently appending to");
  EXTERNALIZE_SAVE_ELEMENT(element, current_log_files_offset_durable_,
            "Indicates the exclusive end of durable region in the current log file");
  return kRetOk;
}

void Savepoint::populate_empty(log::LoggerId logger_count) {
  current_epoch_ = Epoch::kEpochInitialCurrent;
  durable_epoch_ = Epoch::kEpochInitialDurable;
  latest_snapshot_id_ = snapshot::kNullSnapshotId;
  latest_snapshot_epoch_ = Epoch::kEpochInvalid;
  meta_log_oldest_offset_ = 0;
  meta_log_durable_offset_ = 0;
  oldest_log_files_.resize(logger_count, 0);
  oldest_log_files_offset_begin_.resize(logger_count, 0);
  current_log_files_.resize(logger_count, 0);
  current_log_files_offset_durable_.resize(logger_count, 0);
  assert_epoch_values();
}

void FixedSavepoint::update(
  uint16_t node_count,
  uint16_t loggers_per_node_count,
  const Savepoint& src) {
  node_count_ = node_count;
  loggers_per_node_count_ = loggers_per_node_count;
  current_epoch_ = src.current_epoch_;
  durable_epoch_ = src.durable_epoch_;
  latest_snapshot_id_ = src.latest_snapshot_id_;
  latest_snapshot_epoch_ = src.latest_snapshot_epoch_;
  meta_log_oldest_offset_ = src.meta_log_oldest_offset_;
  meta_log_durable_offset_ = src.meta_log_durable_offset_;
  uint32_t count = get_total_logger_count();
  for (uint32_t i = 0; i < count; ++i) {
    logger_info_[i].oldest_log_file_ = src.oldest_log_files_[i];
    logger_info_[i].oldest_log_file_offset_begin_ = src.oldest_log_files_offset_begin_[i];
    logger_info_[i].current_log_file_ = src.current_log_files_[i];
    logger_info_[i].current_log_file_offset_durable_ = src.current_log_files_offset_durable_[i];
  }
}

}  // namespace savepoint
}  // namespace foedus
