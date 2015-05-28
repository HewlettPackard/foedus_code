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

ErrorStack SnapshotManager::read_snapshot_metadata(SnapshotId snapshot_id, SnapshotMetadata* out) {
  return pimpl_->read_snapshot_metadata(snapshot_id, out);
}


void SnapshotManager::trigger_snapshot_immediate(
  bool wait_completion,
  Epoch suggested_snapshot_epoch) {
  pimpl_->trigger_snapshot_immediate(wait_completion, suggested_snapshot_epoch);
}

}  // namespace snapshot
}  // namespace foedus
