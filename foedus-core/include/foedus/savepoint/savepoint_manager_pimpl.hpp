/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_
#define FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/savepoint/fwd.hpp>
#include <foedus/savepoint/savepoint.hpp>
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

    Engine* const           engine_;

    /**
     * 
     */
    Savepoint               savepoint_;
};
}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_PIMPL_HPP_
