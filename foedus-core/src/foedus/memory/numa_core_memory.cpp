/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/memory/numa_core_memory.hpp>
#include <foedus/memory/numa_node_memory.hpp>
namespace foedus {
namespace memory {
ErrorStack NumaCoreMemory::initialize_once() {
    return RET_OK;
}
ErrorStack NumaCoreMemory::uninitialize_once() {
    return RET_OK;
}
}  // namespace memory
}  // namespace foedus
