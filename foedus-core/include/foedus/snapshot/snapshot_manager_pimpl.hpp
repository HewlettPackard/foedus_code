/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_

#include <atomic>
#include <chrono>
#include <string>
#include <vector>

#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/soc/shared_cond.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/thread/condition_variable_impl.hpp"
#include "foedus/thread/stoppable_thread_impl.hpp"

namespace foedus {
namespace snapshot {

/** Shared data in SnapshotManagerPimpl. */
struct SnapshotManagerControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  SnapshotManagerControlBlock() = delete;
  ~SnapshotManagerControlBlock() = delete;

  void initialize() {
    snapshot_taken_.initialize();
  }
  void uninitialize() {
    snapshot_taken_.uninitialize();
  }

  /**
   * The most recently snapshot-ed epoch, all logs upto this epoch is safe to delete.
   * If not snapshot has been taken, invalid epoch.
   * This is equivalent to snapshots_.back().valid_entil_epoch_ with empty check.
   */
  std::atomic< Epoch::EpochInteger >  snapshot_epoch_;

  /**
   * When a caller wants to immediately invoke snapshot, it calls (),
   * which sets this value and then wakes up snapshot_thread_.
   * snapshot_thread_ sees this value, unsets it, then immediately start snapshotting.
   */
  std::atomic<bool>               immediate_snapshot_requested_;

  /**
   * When snapshot_thread_ took snapshot last time.
   * Read and written only by snapshot_thread_.
   */
  std::chrono::system_clock::time_point   previous_snapshot_time_;

  /**
   * ID of previously completed snapshot. kNullSnapshotId if no snapshot has been taken.
   * Used to issue a next snapshot ID.
   */
  std::atomic<SnapshotId>         previous_snapshot_id_;

  /** Fired (notify_all) whenever snapshotting is completed. */
  soc::SharedCond                 snapshot_taken_;
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
  explicit SnapshotManagerPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  /** shorthand for engine_->get_options().snapshot_. */
  const SnapshotOptions& get_option() const;

  Epoch get_snapshot_epoch() const { return Epoch(snapshot_epoch_.load()); }
  Epoch get_snapshot_epoch_weak() const {
    return Epoch(snapshot_epoch_.load(std::memory_order_relaxed));
  }

  SnapshotId get_previous_snapshot_id() const { return previous_snapshot_id_.load(); }
  SnapshotId get_previous_snapshot_id_weak() const {
    return previous_snapshot_id_.load(std::memory_order_relaxed);
  }

  void    trigger_snapshot_immediate(bool wait_completion);

  SnapshotId issue_next_snapshot_id() {
    if (previous_snapshot_id_ == kNullSnapshotId) {
      previous_snapshot_id_ = 1;
    } else {
      previous_snapshot_id_ = increment(previous_snapshot_id_);
    }
    return previous_snapshot_id_;
  }

  /**
   * @brief Main routine for snapshot_thread_.
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
   * Phase-2 of handle_snapshot_triggered().
   * Read log files, distribute them to each partition, and construct snapshot files at
   * each partition.
   * After successful completion, all snapshot files become also durable
   * (LogGleaner's uninitialize() makes it sure).
   * Thus, now we can start installing pointers to the new snapshot file pages.
   */
  ErrorStack  glean_logs(Snapshot *new_snapshot);

  /**
   * Phase-3 of handle_snapshot_triggered().
   * Write out a snapshot metadata file that contains metadata of all storages
   * and a few other global metadata.
   */
  ErrorStack  snapshot_metadata(Snapshot *new_snapshot);

  // @todo Phase-4 to install pointers to snapshot pages and drop volatile pages.

  /**
   * each snapshot has a snapshot-metadata file "snapshot_metadata_<SNAPSHOT_ID>.xml"
   * in first node's first partition folder. */
  fs::Path    get_snapshot_metadata_file_path(SnapshotId snapshot_id) const;

  Engine* const           engine_;

  SnapshotManagerControlBlock*  control_block_;

  /**
   * The most recently snapshot-ed epoch, all logs upto this epoch is safe to delete.
   * If not snapshot has been taken, invalid epoch.
   * This is equivalent to snapshots_.back().valid_entil_epoch_ with empty check.
   */
  std::atomic< Epoch::EpochInteger >  snapshot_epoch_;

  /**
   * When a caller wants to immediately invoke snapshot, it calls (),
   * which sets this value and then wakes up snapshot_thread_.
   * snapshot_thread_ sees this value, unsets it, then immediately start snapshotting.
   */
  std::atomic<bool>               immediate_snapshot_requested_;

  /**
   * When snapshot_thread_ took snapshot last time.
   * Read and written only by snapshot_thread_.
   */
  std::chrono::system_clock::time_point   previous_snapshot_time_;

  /**
   * ID of previously completed snapshot. kNullSnapshotId if no snapshot has been taken.
   * Used to issue a next snapshot ID.
   */
  std::atomic<SnapshotId>         previous_snapshot_id_;

  /**
   * All previously taken snapshots.
   * Access to this data must be protected with mutex.
   */
  std::vector< Snapshot >         snapshots_;

  /**
   * The thread that occasionally wakes up and serves as the main managing thread for
   * snapshotting, which consists of several child threads and multiple phases.
   * Launched only in master engine.
   */
  thread::StoppableThread         snapshot_thread_;

  /** Fired (notify_all) whenever snapshotting is completed. */
  thread::ConditionVariable       snapshot_taken_;
};

static_assert(
  sizeof(SnapshotManagerControlBlock) <= soc::GlobalMemoryAnchors::kSnapshotManagerMemorySize,
  "SnapshotManagerControlBlock is too large.");

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_
