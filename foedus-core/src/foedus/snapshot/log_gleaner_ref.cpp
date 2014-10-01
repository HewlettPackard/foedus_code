/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/log_gleaner_ref.hpp"

#include "foedus/snapshot/snapshot_manager_pimpl.hpp"
#include "foedus/soc/soc_manager.hpp"

namespace foedus {
namespace snapshot {

LogGleanerRef::LogGleanerRef() : Attachable<LogGleanerControlBlock>() {}
LogGleanerRef::LogGleanerRef(Engine* engine)
  : Attachable<LogGleanerControlBlock>() {
  engine_ = engine;
  control_block_ =  &engine_->get_soc_manager()->get_shared_memory_repo()->
    get_global_memory_anchors()->snapshot_manager_memory_->gleaner_;
}
LogGleanerRef::LogGleanerRef(Engine* engine, LogGleanerControlBlock* block)
  : Attachable<LogGleanerControlBlock>(engine, block) {}

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
SnapshotId LogGleanerRef::get_snapshot_id() const { return control_block_->snapshot_id_; }
Epoch LogGleanerRef::get_base_epoch() const { return Epoch(control_block_->base_epoch_); }
Epoch LogGleanerRef::get_valid_until_epoch() const {
  return Epoch(control_block_->valid_until_epoch_);
}


}  // namespace snapshot
}  // namespace foedus
