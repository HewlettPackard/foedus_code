/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_HPP_
#include <foedus/epoch.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/snapshot/fwd.hpp>
#include <foedus/snapshot/snapshot_id.hpp>
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

    /** Returns the most recent snapshot's ID. NULL_SNAPSHOT_ID if no snapshot is taken. */
    SnapshotId get_previous_snapshot_id() const;
    /** Non-atomic version. */
    SnapshotId get_previous_snapshot_id_weak() const;

    /**
     * @brief Immediately take a snapshot
     * @param[in] wait_completion whether to block until the completion of entire snapshotting
     * @details
     * This method is used to immediately take snapshot for either recovery or memory-saving
     * purpose.
     */
    void    trigger_snapshot_immediate(bool wait_completion);

    /** Do not use this unless you know what you are doing. */
    SnapshotManagerPimpl* get_pimpl() { return pimpl_; }

 private:
    SnapshotManagerPimpl *pimpl_;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_HPP_
