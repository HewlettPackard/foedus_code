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
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_HPP_
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
namespace foedus {
namespace snapshot {
/**
 * @brief Snapshot manager that atomically and durably writes out a snapshot file.
 * @ingroup SNAPSHOT
 */
class SnapshotManager CXX11_FINAL : public virtual Initializable {
 public:
  explicit SnapshotManager(Engine* engine);
  ~SnapshotManager();

  // Disable default constructors
  SnapshotManager() CXX11_FUNC_DELETE;
  SnapshotManager(const SnapshotManager&) CXX11_FUNC_DELETE;
  SnapshotManager& operator=(const SnapshotManager&) CXX11_FUNC_DELETE;

  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  /**
   * Returns the most recently snapshot-ed epoch, all logs upto this epoch is safe to delete.
   * If not snapshot has been taken, invalid epoch.
   */
  Epoch get_snapshot_epoch() const;
  /** Non-atomic version. */
  Epoch get_snapshot_epoch_weak() const;

  /** Returns the most recent snapshot's ID. kNullSnapshotId if no snapshot is taken. */
  SnapshotId get_previous_snapshot_id() const;
  /** Non-atomic version. */
  SnapshotId get_previous_snapshot_id_weak() const;

  /**
   * Read the snapshot metadata file that contains storages as of the snapshot.
   * This is used only when the engine starts up.
   */
  ErrorStack read_snapshot_metadata(SnapshotId snapshot_id, SnapshotMetadata* out);

  /**
   * @brief Immediately take a snapshot
   * @param[in] wait_completion whether to block until the completion of entire snapshotting
   * @param[in] suggested_snapshot_epoch the epoch up to which we will snapshot.
   * Must be a durable epoch that is after the previous snapshot epoch.
   * If not specified, the latest durable epoch is used, which is in most cases what you want.
   * @details
   * This method is used to immediately take snapshot for either recovery or memory-saving
   * purpose.
   */
  void    trigger_snapshot_immediate(
    bool wait_completion,
    Epoch suggested_snapshot_epoch = INVALID_EPOCH);

  /** Do not use this unless you know what you are doing. */
  SnapshotManagerPimpl* get_pimpl() { return pimpl_; }

 private:
  SnapshotManagerPimpl *pimpl_;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_HPP_
