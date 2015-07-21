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
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_

#include <atomic>
#include <chrono>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/log_gleaner_resource.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/soc/shared_polling.hpp"
#include "foedus/thread/condition_variable_impl.hpp"

namespace foedus {
namespace snapshot {

/** Shared data for LogGleaner. */
struct LogGleanerControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  LogGleanerControlBlock() = delete;
  ~LogGleanerControlBlock() = delete;

  void initialize() {
    clear_counts();
    mappers_count_ = 0;
    reducers_count_ = 0;
    all_count_ = 0;
    terminating_ = false;
  }
  void uninitialize() {
  }
  void clear_counts() {
    cur_snapshot_.clear();
    completed_count_ = 0;
    completed_mapper_count_ = 0;
    error_count_ = 0;
    exit_count_ = 0;
    gleaning_ = false;
    cancelled_ = false;
  }

  /**
  * If this returns true, all mappers and reducers should exit as soon as possible.
  * Gleaner 'does its best' to wait for the exit of them, and then exit asap, too.
  */
  bool is_error() const { return error_count_ > 0 || cancelled_ || terminating_; }

  /** Whether the log gleaner is now running. */
  std::atomic<bool>               gleaning_;
  /** Whether the log gleaner has been cancalled. */
  std::atomic<bool>               cancelled_;
  /** Whether the engine is being terminated. */
  std::atomic<bool>               terminating_;

  /** The snapshot we are now taking. */
  Snapshot                        cur_snapshot_;

  /**
   * count of mappers/reducers that have completed processing the current epoch.
   * the gleaner thread is woken up when this becomes mappers_.size() + reducers_.size().
   * the gleaner thread sets this to zero and starts next epoch.
   */
  std::atomic<uint16_t>           completed_count_;

  /**
  * We also have a separate count for mappers only to know if all mappers are done.
  * Reducers can go into sleep only after all mappers went into sleep (otherwise reducers
  * might receive more logs!), so they have to also check this.
  */
  std::atomic<uint16_t>           completed_mapper_count_;

  /**
  * count of mappers/reducers that have exitted with some error.
  * if there happens any error, gleaner cancels all mappers/reducers.
  */
  std::atomic<uint16_t>           error_count_;

  /**
  * count of mappers/reducers that have exitted.
  * for sanity check only.
  */
  std::atomic<uint16_t>           exit_count_;

  /** Total number of mappers. Not a mutable information, just for convenience. */
  uint16_t                        mappers_count_;
  /** Total number of mappers. Not a mutable information, just for convenience. */
  uint16_t                        reducers_count_;
  /** mappers_count_ + reducers_count_. Not a mutable information, just for convenience. */
  uint16_t                        all_count_;
};

/** Shared data in SnapshotManagerPimpl. */
struct SnapshotManagerControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  SnapshotManagerControlBlock() = delete;
  ~SnapshotManagerControlBlock() = delete;

  void initialize() {
    snapshot_taken_.initialize();
    snapshot_wakeup_.initialize();
    snapshot_children_wakeup_.initialize();
    gleaner_.initialize();
    requested_snapshot_epoch_.store(Epoch::kEpochInvalid);
  }
  void uninitialize() {
    gleaner_.uninitialize();
  }

  Epoch get_snapshot_epoch() const { return Epoch(snapshot_epoch_.load()); }
  Epoch get_snapshot_epoch_weak() const {
    return Epoch(snapshot_epoch_.load(std::memory_order_relaxed));
  }
  SnapshotId get_previous_snapshot_id() const { return previous_snapshot_id_.load(); }
  SnapshotId get_previous_snapshot_id_weak() const {
    return previous_snapshot_id_.load(std::memory_order_relaxed);
  }
  Epoch get_requested_snapshot_epoch() const { return Epoch(requested_snapshot_epoch_.load()); }

  /**
   * Fires snapshot_children_wakeup_.
   * This is n-to-n condition variable, so expect spurrious wakeups.
   */
  void wakeup_snapshot_children() {
    assorted::memory_fence_release();
    snapshot_children_wakeup_.signal();
  }

  /**
   * The most recently snapshot-ed epoch, all logs upto this epoch is safe to delete.
   * If not snapshot has been taken, invalid epoch.
   * This is equivalent to snapshots_.back().valid_entil_epoch_ with empty check.
   */
  std::atomic< Epoch::EpochInteger >  snapshot_epoch_;

  /**
   * When a caller wants to immediately invoke snapshot, it calls trigger_snapshot_immediate(),
   * which sets this value and then wakes up snapshot_thread_.
   * snapshot_thread_ sees this value then immediately starts snapshotting if the value is larger
   * than the current snapshot_epoch_.
   */
  std::atomic< Epoch::EpochInteger >  requested_snapshot_epoch_;


  /**
   * ID of previously completed snapshot. kNullSnapshotId if no snapshot has been taken.
   * Used to issue a next snapshot ID.
   */
  std::atomic<SnapshotId>         previous_snapshot_id_;

  /** Fired (notify_all) whenever snapshotting is completed. */
  soc::SharedPolling              snapshot_taken_;

  /**
   * Snapshot thread sleeps on this condition variable.
   * The real variable is requested_snapshot_epoch_.
   */
  soc::SharedPolling              snapshot_wakeup_;

  /**
   * Child snapshot managers (the ones in SOC engines) sleep on this condition until
   * the master snapshot manager requests them to launch mappers/reducers for snapshot.
   * The real condition is the various status flag in gleaner_.
   */
  soc::SharedPolling              snapshot_children_wakeup_;

  /** Gleaner-related variables */
  LogGleanerControlBlock          gleaner_;
};

/**
 * @brief Pimpl object of SnapshotManager.
 * @ingroup SNAPSHOT
 * @details
 * A private pimpl object for SnapshotManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class SnapshotManagerPimpl final : public DefaultInitializable {
 public:
  SnapshotManagerPimpl() = delete;
  explicit SnapshotManagerPimpl(Engine* engine)
    : engine_(engine), local_reducer_(nullptr) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  /** shorthand for engine_->get_options().snapshot_. */
  const SnapshotOptions& get_option() const;

  Epoch get_snapshot_epoch() const { return control_block_->get_snapshot_epoch(); }
  Epoch get_snapshot_epoch_weak() const  { return control_block_->get_snapshot_epoch_weak(); }

  SnapshotId get_previous_snapshot_id() const { return control_block_->get_previous_snapshot_id(); }
  SnapshotId get_previous_snapshot_id_weak() const  {
    return control_block_->get_previous_snapshot_id_weak();
  }

  ErrorStack read_snapshot_metadata(SnapshotId snapshot_id, SnapshotMetadata* out);

  void    trigger_snapshot_immediate(
    bool wait_completion,
    Epoch suggested_snapshot_epoch);
  /**
   * This is a hidden API called at the beginning of engine shutdown (namely restart manager).
   * Snapshot Manager initializes before Storage because it must \e read previous snapshot,
   * but it must stop snapshot thread before Storage because the snapshot thread relies on
   * storage module. To solve the issue, we call this method from restart manager's uninit to
   * stop snapshot thread.
   * This method is also called by snapshot manager's own uninit() in case restart manager
   * didn't initialize. Thus, this method must be idempotent.
   */
  void    stop_snapshot_thread();

  SnapshotId issue_next_snapshot_id() {
    if (control_block_->previous_snapshot_id_ == kNullSnapshotId) {
      control_block_->previous_snapshot_id_ = 1;
    } else {
      control_block_->previous_snapshot_id_ = increment(control_block_->previous_snapshot_id_);
    }
    return control_block_->previous_snapshot_id_;
  }

  void wakeup();
  void sleep_a_while();
  bool is_stop_requested() const { return stop_requested_; }
  bool is_gleaning() const { return control_block_->gleaner_.gleaning_; }

  /**
   * @brief Main routine for snapshot_thread_ in master engine.
   * @details
   * This method keeps taking snapshot periodically.
   * When there are no logs in all the private buffers for a while, it goes into sleep.
   * This method exits when this object's uninitialize() is called.
   */
  void        handle_snapshot();
  /**
   * handle_snapshot() calls this when it should start snapshotting.
   * In other words, this function is the main routine of snapshotting.
   */
  ErrorStack  handle_snapshot_triggered(Snapshot *new_snapshot);

  /**
   * @brief Main routine for snapshot_thread_ in child engines.
   * @details
   * All this does is to launch mappers/reducers threads when asked by master engine.
   */
  void        handle_snapshot_child();

  /**
   * Sub-routine of handle_snapshot_triggered().
   * Read log files, distribute them to each partition, and construct snapshot files at
   * each partition.
   * After successful completion, all snapshot files become also durable
   * (LogGleaner's uninitialize() makes it sure).
   * Thus, now we can start installing pointers to the new snapshot file pages.
   */
  ErrorStack  glean_logs(
    const Snapshot& new_snapshot,
    std::map<storage::StorageId, storage::SnapshotPagePointer>* new_root_page_pointers);

  /**
   * Sub-routine of handle_snapshot_triggered().
   * Write out a snapshot metadata file that contains metadata of all storages
   * and a few other global metadata.
   */
  ErrorStack  snapshot_metadata(
    const Snapshot& new_snapshot,
    const std::map<storage::StorageId, storage::SnapshotPagePointer>& new_root_page_pointers);

  /**
   * Sub-routine of handle_snapshot_triggered().
   * Invokes the savepoint module to take savepoint pointing to this snapshot.
   * Until that, the snapshot is not yet deemed as "happened".
   */
  ErrorStack  snapshot_savepoint(const Snapshot& new_snapshot);

  /**
   * Sub-routine of handle_snapshot_triggered().
   * Drop pointers to volatile pages based on the already-installed snapshot pointers.
   */
  ErrorStack  drop_volatile_pages(
    const Snapshot& new_snapshot,
    const std::map<storage::StorageId, storage::SnapshotPagePointer>& new_root_page_pointers);
  /** subroutine invoked by one thread for one node. */
  void        drop_volatile_pages_parallel(
    const Snapshot& new_snapshot,
    const std::map<storage::StorageId, storage::SnapshotPagePointer>& new_root_page_pointers,
    void* result_memory,
    uint16_t parallel_id);

  /**
   * each snapshot has a snapshot-metadata file "snapshot_metadata_<SNAPSHOT_ID>.xml"
   * in first node's first partition folder. */
  fs::Path    get_snapshot_metadata_file_path(SnapshotId snapshot_id) const;

  Engine* const           engine_;

  SnapshotManagerControlBlock*  control_block_;

  /**
   * All previously taken snapshots.
   * Access to this data must be protected with mutex.
   * This is populated only in master engine.
   */
  std::vector< Snapshot >   snapshots_;

  /** To locally shutdown snapshot_thread_. This is not a shared memory. */
  std::atomic<bool>         stop_requested_;

  /**
   * The daemon thread of snapshot manager.
   * In master engine, this occasionally wakes up and serves as the main managing thread for
   * snapshotting, which consists of several child threads and multiple phases.
   * In child engine, this receives requests from master engine's snapshot_thread_ and launch
   * mappers/reducers in this node.
   */
  std::thread               snapshot_thread_;

  /**
   * When snapshot_thread_ took snapshot last time.
   * Read and written only by snapshot_thread_.
   */
  std::chrono::system_clock::time_point   previous_snapshot_time_;

  /** Mappers in this node. Index is logger ordinal. Empty in master engine. */
  std::vector<LogMapper*>     local_mappers_;
  /** Reducer in this node. Null in master engine. */
  LogReducer*                 local_reducer_;

  /** Local resources for gleaner, which runs only in the master node. Empty in child nodes. */
  LogGleanerResource          gleaner_resource_;
};

static_assert(
  sizeof(SnapshotManagerControlBlock) <= soc::GlobalMemoryAnchors::kSnapshotManagerMemorySize,
  "SnapshotManagerControlBlock is too large.");

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_
