/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_PIMPL_HPP_
#define FOEDUS_ENGINE_PIMPL_HPP_

#include <foedus/cxx11.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/initializable.hpp>
// This is pimpl. no need for further indirections. just include them all.
#include <foedus/debugging/debugging_supports.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/memory/engine_memory.hpp>
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
    EnginePimpl() = delete;
    EnginePimpl(const EnginePimpl &) = delete;
    EnginePimpl& operator=(const EnginePimpl &) = delete;

    INITIALIZABLE_DEFAULT;

    /** Options given at boot time. Immutable once constructed. */
    const EngineOptions         options_;

    /**
     * Engine-wide memory. Initialized/uninitialized in EnginePimpl's initialize/uninitialize.
     */
    memory::EngineMemory        memory_;

    /**
     * Debugging supports. Initialized/uninitialized in EnginePimpl's initialize/uninitialize.
     * \attention Because this module initializes/uninitializes basic debug logging support,
     * EnginePimpl#initialize_once() must initialize it at the beginning,
     * and EnginePimpl#uninitialize_once() must uninitialize it at the end.
     */
    debugging::DebuggingSupports    debug_;

    /**
     * Filesystem wrapper. Initialized/uninitialized in EnginePimpl's initialize/uninitialize.
     */
    fs::Filesystem              filesystem_;

    /** Whether this engine is currently up and running. */
    bool                        initialized_;
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_PIMPL_HPP_
