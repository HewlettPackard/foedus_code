/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_
#include <foedus/epoch.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/snapshot/fwd.hpp>
#include <foedus/snapshot/snapshot.hpp>
#include <foedus/snapshot/snapshot_id.hpp>
#include <atomic>
#include <vector>
namespace foedus {
namespace snapshot {
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
    explicit SnapshotManagerPimpl(Engine* engine) : engine_(engine),
        snapshot_epoch_(Epoch::EPOCH_INVALID), previous_snapshot_id_(NULL_SNAPSHOT_ID) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    Engine* const           engine_;

    /**
     * The most recently snapshot-ed epoch, all logs upto this epoch is safe to delete.
     * If not snapshot has been taken, invalid epoch.
     * This is equivalent to snapshots_.back().valid_entil_epoch_ with empty check.
     */
    std::atomic< Epoch::EpochInteger >  snapshot_epoch_;

    /**
     * ID of previously completed snapshot. NULL_SNAPSHOT_ID if no snapshot has been taken.
     * Used to issue a next snapshot ID.
     */
    SnapshotId                      previous_snapshot_id_;

    /**
     * All previously taken snapshots.
     * Access to this data must be protected with mutex.
     */
    std::vector< Snapshot >         snapshots_;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_
