/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <foedus/memory/numa_node_memory.hpp>
#include <foedus/thread/thread_options.hpp>
namespace foedus {
namespace memory {
NumaNodeMemory::NumaNodeMemory(Engine* engine, foedus::thread::ThreadGroupId numa_node)
    : engine_(engine), numa_node_(numa_node) {
}
ErrorStack NumaNodeMemory::initialize_once() {
    thread::ThreadLocalOrdinal cores = engine_->get_options().thread_.thread_count_per_group_;
    for (thread::ThreadLocalOrdinal ordinal = 0; ordinal < cores; ++ordinal) {
        core_memories_.push_back(new NumaCoreMemory(engine_, this, ordinal));
        CHECK_ERROR(core_memories_.back()->initialize());
    }
    return RET_OK;
}
ErrorStack NumaNodeMemory::uninitialize_once() {
    ErrorStackBatch batch;
    batch.uninitialize_and_delete_all(&core_memories_);
    return SUMMARIZE_ERROR_BATCH(batch);
}

}  // namespace memory
}  // namespace foedus
