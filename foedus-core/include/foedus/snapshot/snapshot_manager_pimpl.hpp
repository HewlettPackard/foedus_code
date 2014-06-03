/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_
#define FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/snapshot/fwd.hpp>
#include <foedus/epoch.hpp>
#include <mutex>
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
    explicit SnapshotManagerPimpl(Engine* engine) : engine_(engine) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    Engine* const           engine_;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_SNAPSHOT_MANAGER_PIMPL_HPP_
