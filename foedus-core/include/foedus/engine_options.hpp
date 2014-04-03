/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_OPTIONS_HPP_
#define FOEDUS_ENGINE_OPTIONS_HPP_

#include <iosfwd>
namespace foedus {

// forward declarations of option classes for each module.
namespace log {
    struct LogOptions;
}  // namespace log

namespace memory {
    struct MemoryOptions;
}  // namespace memory

namespace snapshot {
    struct SnapshotOptions;
}  // namespace snapshot

namespace storage {
    struct StorageOptions;
}  // namespace storage

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
    ~EngineOptions();

    log::LogOptions*            log_;
    memory::MemoryOptions*      memory_;
    snapshot::SnapshotOptions*  snapshot_;
    storage::StorageOptions*    storage_;
};
}  // namespace foedus
std::ostream& operator<<(std::ostream& o, const foedus::EngineOptions& v);
#endif  // FOEDUS_ENGINE_OPTIONS_HPP_
