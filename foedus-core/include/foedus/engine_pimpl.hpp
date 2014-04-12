/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_PIMPL_HPP_
#define FOEDUS_ENGINE_PIMPL_HPP_

#include <foedus/engine_options.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
// This is pimpl. no need for further indirections. just include them all.
#include <foedus/debugging/debugging_supports.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/log/log_manager.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/thread/thread_pool.hpp>
namespace foedus {
/**
 * @brief Pimpl object of Engine.
 * @ingroup ENGINE
 * @details
 * A private pimpl object for Engine.
 * Do not include this header from a client program unless you know what you are doing.
 */
class EnginePimpl final : public DefaultInitializable {
 public:
    EnginePimpl() = delete;
    explicit EnginePimpl(Engine* engine, const EngineOptions &options);
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    /** Options given at boot time. Immutable once constructed. */
    const EngineOptions             options_;

    /** Pointer to the enclosing object. Few places would need it, but hold it in case. */
    Engine* const                   engine_;

// Individual modules. Placed in initialize()/uninitialize() order
// (remember, modules have dependencies between them).

    /**
     * Debugging supports.
     * @attention Because this module initializes/uninitializes basic debug logging support,
     * EnginePimpl#initialize_once() must initialize it at the beginning,
     * and EnginePimpl#uninitialize_once() must uninitialize it at the end.
     */
    debugging::DebuggingSupports    debug_;
    fs::Filesystem                  filesystem_;
    memory::EngineMemory            memory_manager_;
    thread::ThreadPool              thread_pool_;
    log::LogManager                 log_manager_;
    storage::StorageManager         storage_manager_;
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_PIMPL_HPP_
