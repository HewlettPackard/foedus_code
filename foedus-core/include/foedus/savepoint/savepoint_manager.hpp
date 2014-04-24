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

    /**
     * @brief Returns a copy of the current progress of the entire engine.
     * @details
     * Note that this is a read-only access, which might see a stale information if it's
     * in race condition. However, we take a lock before copying the entire information.
     * Thus, this method is slow but safe. No garbage information returned.
     * @see get_savepoint_fast()
     */
    Savepoint get_savepoint_safe() const;

    /**
     * @brief Returns a reference of the current progress of the entire engine.
     * @details
     * This is a read-only and \e unsafe access in race condition.
     * This method doesn't take any lock. Thus, this method is fast but unsafe.
     * Use it only when it's appropriate.
     * @see get_savepoint_safe()
     */
    const Savepoint& get_savepoint_fast() const;

 private:
    SavepointManagerPimpl *pimpl_;
};
}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_MANAGER_HPP_
