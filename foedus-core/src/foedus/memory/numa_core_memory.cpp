/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/memory/numa_core_memory.hpp>
#include <foedus/memory/numa_node_memory.hpp>
namespace foedus {
namespace memory {
NumaCoreMemory::NumaCoreMemory(NumaNodeMemory *node_memory, foedus::thread::ThreadId core_id)
    : engine_memory_(node_memory->get_engine_memory()), node_memory_(node_memory),
    core_id_(core_id), core_local_ordinal_(foedus::thread::decompose_numa_local_ordinal(core_id)),
    initialized_(false) {
}
NumaCoreMemory::~NumaCoreMemory() {
}
ErrorStack NumaCoreMemory::initialize_once() {
    return RET_OK;
}
ErrorStack NumaCoreMemory::uninitialize_once() {
    return RET_OK;
}
}  // namespace memory
}  // namespace foedus
