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
#include "foedus/savepoint/savepoint_manager.hpp"

#include "foedus/savepoint/savepoint_manager_pimpl.hpp"

namespace foedus {
namespace savepoint {
SavepointManager::SavepointManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new SavepointManagerPimpl(engine);
}
SavepointManager::~SavepointManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  SavepointManager::initialize() { return pimpl_->initialize(); }
bool        SavepointManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  SavepointManager::uninitialize() { return pimpl_->uninitialize(); }

Epoch SavepointManager::get_initial_current_epoch() const {
  return pimpl_->get_initial_current_epoch();
}
Epoch SavepointManager::get_initial_durable_epoch() const {
  return pimpl_->get_initial_durable_epoch();
}
Epoch SavepointManager::get_earliest_epoch() const {
  return pimpl_->get_earliest_epoch();
}

Epoch SavepointManager::get_saved_durable_epoch() const {
  return pimpl_->get_saved_durable_epoch();
}
snapshot::SnapshotId SavepointManager::get_latest_snapshot_id() const {
  return pimpl_->get_latest_snapshot_id();
}
Epoch SavepointManager::get_latest_snapshot_epoch() const {
  return pimpl_->get_latest_snapshot_epoch();
}

ErrorStack SavepointManager::take_savepoint(Epoch new_global_durable_epoch) {
  return pimpl_->take_savepoint(new_global_durable_epoch);
}
ErrorStack SavepointManager::take_savepoint_after_snapshot(
  snapshot::SnapshotId new_snapshot_id,
  Epoch new_snapshot_epoch) {
  return pimpl_->take_savepoint_after_snapshot(new_snapshot_id, new_snapshot_epoch);
}

LoggerSavepointInfo SavepointManager::get_logger_savepoint(log::LoggerId logger_id) {
  return pimpl_->get_logger_savepoint(logger_id);
}

void SavepointManager::get_meta_logger_offsets(
  uint64_t* oldest_offset,
  uint64_t* durable_offset) const {
  *oldest_offset = pimpl_->control_block_->savepoint_.meta_log_oldest_offset_;
  *durable_offset = pimpl_->control_block_->savepoint_.meta_log_durable_offset_;
}

}  // namespace savepoint
}  // namespace foedus
