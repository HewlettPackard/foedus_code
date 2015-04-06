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
#ifndef FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_
#define FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_

#include <atomic>
#include <thread>

#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/savepoint/fwd.hpp"
#include "foedus/savepoint/savepoint.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/soc/shared_polling.hpp"

namespace foedus {
namespace savepoint {

/** Shared data in SavepointManagerPimpl. */
struct SavepointManagerControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  SavepointManagerControlBlock() = delete;
  ~SavepointManagerControlBlock() = delete;

  void initialize() {
    save_wakeup_.initialize();
    save_done_event_.initialize();
    savepoint_mutex_.initialize();
    new_snapshot_id_ = snapshot::kNullSnapshotId;
    new_snapshot_epoch_ = Epoch::kEpochInvalid;
  }
  void uninitialize() {
    savepoint_mutex_.uninitialize();
  }

  std::atomic<bool>   master_initialized_;
  Epoch::EpochInteger initial_current_epoch_;
  Epoch::EpochInteger initial_durable_epoch_;

  /**
   * savepoint thread sleeps on this condition variable.
   * The real variable is saved_durable_epoch_.
   */
  soc::SharedPolling  save_wakeup_;
  /** The durable epoch that has been made persistent in previous savepoint-ing. */
  Epoch::EpochInteger saved_durable_epoch_;
  /**
   * Client SOC sets this value and then wakes up the savepoint thread.
   * The value indicates the epoch upto which loggers made sure all log files are durable.
   * So, as soon as we take a savepoint, this officially becomes the new global durable epoch.
   */
  Epoch::EpochInteger requested_durable_epoch_;

  /**
   * Whenever a savepoint has been taken, this event is fired.
   * The thread that has requested the savepoint sleeps on this.
   */
  soc::SharedPolling  save_done_event_;

  /**
   * Read/write to savepoint_ is protected with this mutex.
   * There is anyway only one writer to savepoint_, but this is required to make sure
   * readers don't see half-updated garbage.
   */
  soc::SharedMutex    savepoint_mutex_;

  /**
   * The ID of the new snapshot to remember.
   * Set from take_savepoint_after_snapshot to request taking a new snapshot just for
   * updating snapshot ID in savepoint file.
   * kNullSnapshotId if not requested.
   */
  snapshot::SnapshotId  new_snapshot_id_;
  /** Set with new_snapshot_id_ */
  Epoch::EpochInteger   new_snapshot_epoch_;

  /**
   * The content of latest savepoint.
   * This is kind of big, but most part of it is unused.
   */
  FixedSavepoint      savepoint_;
};

/**
 * @brief Pimpl object of SavepointManager.
 * @ingroup SAVEPOINT
 * @details
 * A private pimpl object for SavepointManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class SavepointManagerPimpl final : public DefaultInitializable {
 public:
  SavepointManagerPimpl() = delete;
  explicit SavepointManagerPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  Epoch get_initial_current_epoch() const { return Epoch(control_block_->initial_current_epoch_); }
  Epoch get_initial_durable_epoch() const { return Epoch(control_block_->initial_durable_epoch_); }
  Epoch get_saved_durable_epoch() const { return Epoch(control_block_->saved_durable_epoch_); }
  snapshot::SnapshotId get_latest_snapshot_id() const {
    return control_block_->savepoint_.latest_snapshot_id_;
  }
  Epoch get_latest_snapshot_epoch() const {
    return Epoch(control_block_->savepoint_.latest_snapshot_epoch_);
  }
  Epoch get_requested_durable_epoch() const {
    return Epoch(control_block_->requested_durable_epoch_);
  }
  void                update_shared_savepoint(const Savepoint& src);
  LoggerSavepointInfo get_logger_savepoint(log::LoggerId logger_id);
  ErrorStack          take_savepoint(Epoch new_global_durable_epoch);
  ErrorStack          take_savepoint_after_snapshot(
    snapshot::SnapshotId new_snapshot_id,
    Epoch new_snapshot_epoch);

  Engine* const           engine_;
  SavepointManagerControlBlock* control_block_;


  // the followings are used only in master. child SOCs just request the master to take savepoint.
  void savepoint_main();
  bool is_stop_requested() { return savepoint_thread_stop_requested_; }

  /** The thread to take savepoints */
  std::thread             savepoint_thread_;

  std::atomic<bool>       savepoint_thread_stop_requested_;

  /** Path of the savepoint file. */
  fs::Path                savepoint_path_;

  /**
   * The current progress of the entire engine.
   */
  Savepoint               savepoint_;
};

static_assert(
  sizeof(SavepointManagerControlBlock) <= soc::GlobalMemoryAnchors::kSavepointManagerMemorySize,
  "SavepointManagerControlBlock is too large.");

}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_
