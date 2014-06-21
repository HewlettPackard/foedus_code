/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/snapshot/snapshot_manager_pimpl.hpp"
namespace foedus {
namespace snapshot {
SnapshotManager::SnapshotManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new SnapshotManagerPimpl(engine);
}
SnapshotManager::~SnapshotManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  SnapshotManager::initialize() { return pimpl_->initialize(); }
bool        SnapshotManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  SnapshotManager::uninitialize() { return pimpl_->uninitialize(); }

Epoch SnapshotManager::get_snapshot_epoch() const { return pimpl_->get_snapshot_epoch(); }
Epoch SnapshotManager::get_snapshot_epoch_weak() const {
  return pimpl_->get_snapshot_epoch_weak();
}

SnapshotId SnapshotManager::get_previous_snapshot_id() const {
  return pimpl_->get_previous_snapshot_id();
}

SnapshotId SnapshotManager::get_previous_snapshot_id_weak() const {
  return pimpl_->get_previous_snapshot_id_weak();
}

void SnapshotManager::trigger_snapshot_immediate(bool wait_completion) {
  pimpl_->trigger_snapshot_immediate(wait_completion);
}

}  // namespace snapshot
}  // namespace foedus
