/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_OPTIONS_HPP_
#define FOEDUS_ENGINE_OPTIONS_HPP_

// rather than forward declarations of option classes for each module, we include them here.
// these are anyway very small header files, and demanding user code to include each of them
// won't fly. further, just holding instances, rather than pointers, makes (de)allocation simpler.
#include <foedus/cxx11.hpp>
#include <foedus/externalize/externalizable.hpp>
#include <foedus/cache/cache_options.hpp>
#include <foedus/debugging/debugging_options.hpp>
#include <foedus/log/log_options.hpp>
#include <foedus/memory/memory_options.hpp>
#include <foedus/restart/restart_options.hpp>
#include <foedus/savepoint/savepoint_options.hpp>
#include <foedus/snapshot/snapshot_options.hpp>
#include <foedus/storage/storage_options.hpp>
#include <foedus/thread/thread_options.hpp>
#include <foedus/xct/xct_options.hpp>
namespace foedus {
/**
 * @brief Set of option values given to the engine at start-up.
 * @ingroup ENGINE
 * @details
 * This object is a collection of engine-wide settings and settings for individual
 * modules (XxxOptions). To configure options, instantiate this class then modify
 * values in each sub-module's options.
 * When you start-up the database engine, you have to provide this object.
 *
 * @section INSTANTIATION Instantiating EngineOptions object
 * To instantiate an EngineOptions object, simply call the default constructor like:
 * @code{.cpp}
 * EngineOptions options;
 * @endcode
 * This sets default values to all settings. See each module's option classes for the description
 * of their default values.
 *
 * @section EXTERNALIZATION Externalization: Loading and Saving config values.
 * You can save and load this object to an XML file:
 * @code{.cpp}
 * EngineOptions options;
 * ... (change something in options)
 * if (options.save_to_file("/your/path/to/foedus_config.xml").is_error()) {
 *    // handle errors. It might be file permission issue or other file I/O issues.
 * }
 *
 * .... (after doing something else, possibly after restarting the program)
 * .... (or, maybe the user has edited the config file on text editor)
 * if (options.load_from_file("/your/path/to/foedus_config.xml").is_error()) {
 *    // handle errors. It might be file permission, corrupted XML files, etc.
 * }
 * @endcode
 */
struct EngineOptions CXX11_FINAL : public virtual externalize::Externalizable {
    /**
     * Constructs option values with default values.
     */
    EngineOptions();
    EngineOptions(const EngineOptions& other);
    EngineOptions& operator=(const EngineOptions& other);

    // options for each module
    cache::CacheOptions         cache_;
    debugging::DebuggingOptions debugging_;
    log::LogOptions             log_;
    memory::MemoryOptions       memory_;
    restart::RestartOptions     restart_;
    savepoint::SavepointOptions savepoint_;
    snapshot::SnapshotOptions   snapshot_;
    storage::StorageOptions     storage_;
    thread::ThreadOptions       thread_;
    xct::XctOptions             xct_;

    EXTERNALIZABLE(EngineOptions);
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_OPTIONS_HPP_
