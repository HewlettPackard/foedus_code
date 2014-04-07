/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine_options.hpp>
#include <foedus/memory/engine_memory.hpp>
namespace foedus {
namespace memory {
EngineMemory::EngineMemory(const EngineOptions &options) : options_(options), initialized_(false) {
}
EngineMemory::~EngineMemory() {
}
ErrorStack EngineMemory::initialize_once() {
    return RET_OK;
}

ErrorStack EngineMemory::uninitialize_once() {
    return RET_OK;
}

}  // namespace memory
}  // namespace foedus
