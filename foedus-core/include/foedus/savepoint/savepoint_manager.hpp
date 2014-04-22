/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_HPP_
#define FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/savepoint/fwd.hpp>
namespace foedus {
namespace savepoint {
/**
 * @brief Savepoint manager that atomically and durably writes out a savepoint file.
 * @ingroup SAVEPOINT
 */
class SavepointManager CXX11_FINAL : public virtual Initializable {
 public:
    explicit SavepointManager(Engine* engine);
    ~SavepointManager();

    // Disable default constructors
    SavepointManager() CXX11_FUNC_DELETE;
    SavepointManager(const SavepointManager&) CXX11_FUNC_DELETE;
    SavepointManager& operator=(const SavepointManager&) CXX11_FUNC_DELETE;

    ErrorStack  initialize() CXX11_OVERRIDE;
    bool        is_initialized() const CXX11_OVERRIDE;
    ErrorStack  uninitialize() CXX11_OVERRIDE;

 private:
    SavepointManagerPimpl *pimpl_;
};
}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_HPP_
