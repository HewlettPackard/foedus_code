/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_PIMPL_HPP_
#define FOEDUS_ENGINE_PIMPL_HPP_

#include <foedus/cxx11.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/initializable.hpp>
#include <foedus/fs/fwd.hpp>
#include <foedus/memory/fwd.hpp>
namespace foedus {
/**
 * @brief Pimpl object of Engine.
 * @ingroup ENGINE
 * @details
 * A private pimpl object for Engine.
 * Do not include this header from a client program unless you know what you are doing.
 */
class EnginePimpl : public virtual Initializable {
 public:
    explicit EnginePimpl(const EngineOptions &options);
    ~EnginePimpl();

    // Disable default constructors
    EnginePimpl() CXX11_FUNC_DELETE;
    EnginePimpl(const EnginePimpl &) CXX11_FUNC_DELETE;
    EnginePimpl& operator=(const EnginePimpl &) CXX11_FUNC_DELETE;

    /** @copydoc Engine#initialize() */
    ErrorStack  initialize() CXX11_OVERRIDE;

    /** @copydoc Engine#uninitialize() */
    ErrorStack  uninitialize() CXX11_OVERRIDE;

    bool        is_initialized() const CXX11_OVERRIDE { return running_; }

    /** Options given at boot time. Immutable once constructed. */
    const EngineOptions     options_;

    /**
     * Engine-wide memory. This is NULL until the engine starts up.
     * We allocate it during start up.
     */
    memory::EngineMemory*   memory_;

    /**
     * Filesystem wrapper. This is NULL until the engine starts up.
     * We allocate it during start up.
     */
    fs::Filesystem*         filesystem_;

    /** Whether this engine is currently up and running. */
    bool                    running_;
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_PIMPL_HPP_
