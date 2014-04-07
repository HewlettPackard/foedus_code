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
#include <atomic>  // yes, this is a pimpl header, so okay to explicitly use C++11
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

    /**
     * Initialize Google-logging only once. This is called at the beginning of initialize_once()
     * so that all other initialization can use glog.
     * @see static_glog_initialize_counter
     * @see static_glog_initialize_done
     */
    void                initialize_glog();
    /**
     * Uninitialize Google-logging only once.  This is called at the end of uninitialize_once()
     * so that all other uninitialization can use glog.
     * @see initialize_glog
     */
    void                uninitialize_glog();

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
    bool                    initialized_;

    /**
     * @brief This and static_glog_initialize_done are the \b only static variables
     * we have in the entire code base.
     * @details
     * Because google-logging requires initialization/uninitialization only once in a process,
     * we need this to coordinate it between multiple engines.
     * We atomically increment/decrement this when an Engine is initialized/uninitialized.
     * The one who observed "0" on atomic increment (as old value), will initialize glog.
     * The one who observed "1" on atomic decrement (as old value), will uninitialize glog.
     * @invariant 0 or larger. negative value is definitely a bug in synchronization code.
     */
    static std::atomic<int> static_glog_initialize_counter;

    /**
     * @brief Whether Google-logging is initialized or not.
     * @details
     * When a new engine observed non-zero on atomic increment, it must spin on this value
     * until it becomes TRUE. This is to avoid a race condition (though very unlikely) where
     * a thread that observed non-zero stats using glog before the thread that observed zero
     * finishes initializing glog.
     */
    static bool             static_glog_initialize_done;
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_PIMPL_HPP_
