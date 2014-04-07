/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_OPTIONS_HPP_
#define FOEDUS_ENGINE_OPTIONS_HPP_

// rather than forward declarations of option classes for each module, we include them here.
// these are anyway very small header files, and demanding user code to include each of them
// won't fly. further, just holding instances, rather than pointers, makes (de)allocation simpler.
#include <foedus/cache/cache_options.hpp>
#include <foedus/debugging/debugging_options.hpp>
#include <foedus/fs/filesystem_options.hpp>
#include <foedus/log/log_options.hpp>
#include <foedus/memory/memory_options.hpp>
#include <foedus/snapshot/snapshot_options.hpp>
#include <foedus/storage/storage_options.hpp>
#include <foedus/thread/thread_options.hpp>
#include <iosfwd>
namespace foedus {
/**
 * @brief Set of option values given to the engine at start-up.
 * @ingroup ENGINE
 * @details
 * This object is a collection of engine-wide settings and settings for individual
 * modules (XxxOptions). To configure options, instantiate this class then modify
 * values in each sub-module's options.
 */
struct EngineOptions {
    /**
     * Constructs option values with default values.
     */
    EngineOptions();
    EngineOptions(const EngineOptions& other);
    EngineOptions& operator=(const EngineOptions& other);

    // options for each module
    cache::CacheOptions         cache_;
    debugging::DebuggingOptions debugging_;
    fs::FilesystemOptions       fs_;
    log::LogOptions             log_;
    memory::MemoryOptions       memory_;
    snapshot::SnapshotOptions   snapshot_;
    storage::StorageOptions     storage_;
    thread::ThreadOptions       thread_;

    friend std::ostream& operator<<(std::ostream& o, const EngineOptions& v);
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_OPTIONS_HPP_
