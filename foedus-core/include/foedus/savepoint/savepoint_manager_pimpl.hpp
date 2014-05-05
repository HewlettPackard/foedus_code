/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_
#define FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/savepoint/fwd.hpp>
#include <foedus/savepoint/savepoint.hpp>
#include <foedus/epoch.hpp>
#include <mutex>
namespace foedus {
namespace savepoint {
/**
 * @brief Pimpl object of SavepointManager.
 * @ingroup LOG
 * @details
 * A private pimpl object for SavepointManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class SavepointManagerPimpl CXX11_FINAL : public DefaultInitializable {
 public:
    SavepointManagerPimpl() = delete;
    explicit SavepointManagerPimpl(Engine* engine) : engine_(engine) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    Savepoint           get_savepoint_safe() const;
    const Savepoint&    get_savepoint_fast() const;
    ErrorStack          take_savepoint(Epoch new_global_durable_epoch);

    Engine* const           engine_;

    /** Path of the savepoint file. */
    fs::Path                savepoint_path_;

    /**
     * The current progress of the entire engine.
     */
    Savepoint               savepoint_;

    /**
     * Protectes against read/write accesses to savepoint_.
     * So far exclusive mutex. We should't have frequent accesses to savepoint_, so should be ok.
     */
    mutable std::mutex      savepoint_mutex_;
};
}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_
