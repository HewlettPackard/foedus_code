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
#include "foedus/snapshot/log_gleaner_ref.hpp"

#include "foedus/snapshot/snapshot_manager_pimpl.hpp"
#include "foedus/soc/soc_manager.hpp"

namespace foedus {
namespace snapshot {

LogGleanerRef::LogGleanerRef() : Attachable<LogGleanerControlBlock>() {
  partitioner_metadata_ = nullptr;
  partitioner_data_ = nullptr;
}
LogGleanerRef::LogGleanerRef(Engine* engine)
  : Attachable<LogGleanerControlBlock>() {
  engine_ = engine;
  soc::GlobalMemoryAnchors* anchors
    = engine_->get_soc_manager()->get_shared_memory_repo()->get_global_memory_anchors();
  control_block_ =  &anchors->snapshot_manager_memory_->gleaner_;
  partitioner_metadata_ = anchors->partitioner_metadata_;
  partitioner_data_ = anchors->partitioner_data_;
}

uint16_t LogGleanerRef::increment_completed_count() {
  ASSERT_ND(control_block_->completed_count_ < control_block_->all_count_);
  return ++control_block_->completed_count_;
}
uint16_t LogGleanerRef::increment_completed_mapper_count() {
  ASSERT_ND(control_block_->completed_mapper_count_ < control_block_->mappers_count_);
  return ++control_block_->completed_mapper_count_;
}
uint16_t LogGleanerRef::increment_error_count() {
  ASSERT_ND(control_block_->error_count_ < control_block_->all_count_);
  return ++control_block_->error_count_;
}
uint16_t LogGleanerRef::increment_exit_count() {
  ASSERT_ND(control_block_->exit_count_ < control_block_->all_count_);
  return ++control_block_->exit_count_;
}

bool LogGleanerRef::is_all_exitted() const {
  return control_block_->exit_count_ >= control_block_->all_count_;
}

bool LogGleanerRef::is_all_completed() const {
  return control_block_->completed_count_ >= control_block_->all_count_;
}
bool LogGleanerRef::is_all_mappers_completed() const {
  return control_block_->completed_mapper_count_ >= control_block_->mappers_count_;
}
uint16_t LogGleanerRef::get_mappers_count() const { return control_block_->mappers_count_; }
uint16_t LogGleanerRef::get_reducers_count() const { return control_block_->reducers_count_; }
uint16_t LogGleanerRef::get_all_count() const { return control_block_->all_count_; }

bool LogGleanerRef::is_error() const { return control_block_->is_error(); }
void LogGleanerRef::wakeup() {
}

const Snapshot& LogGleanerRef::get_cur_snapshot() const { return control_block_->cur_snapshot_; }
SnapshotId LogGleanerRef::get_snapshot_id() const { return get_cur_snapshot().id_; }
Epoch LogGleanerRef::get_base_epoch() const { return get_cur_snapshot().base_epoch_; }
Epoch LogGleanerRef::get_valid_until_epoch() const { return get_cur_snapshot().valid_until_epoch_; }


}  // namespace snapshot
}  // namespace foedus
