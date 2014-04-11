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
#include <glog/logging.h>
#include <numa.h>
namespace foedus {
namespace memory {
NumaNodeMemory::NumaNodeMemory(Engine* engine, foedus::thread::ThreadGroupId numa_node)
    : engine_(engine), numa_node_(numa_node) {
}
ErrorStack NumaNodeMemory::initialize_once() {
    LOG(INFO) << "Initializing NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " BEFORE: numa_node_size=" << numa_node_size(numa_node_, nullptr);

    thread::ThreadLocalOrdinal cores = engine_->get_options().thread_.thread_count_per_group_;
    for (thread::ThreadLocalOrdinal ordinal = 0; ordinal < cores; ++ordinal) {
        core_memories_.push_back(new NumaCoreMemory(engine_, this, ordinal));
        CHECK_ERROR(core_memories_.back()->initialize());
    }

    LOG(INFO) << "Initialized NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " AFTER: numa_node_size=" << numa_node_size(numa_node_, nullptr);
    return RET_OK;
}
ErrorStack NumaNodeMemory::uninitialize_once() {
    LOG(INFO) << "Uninitializing NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " BEFORE: numa_node_size=" << numa_node_size(numa_node_, nullptr);

    ErrorStackBatch batch;
    batch.uninitialize_and_delete_all(&core_memories_);

    LOG(INFO) << "Uninitialized NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " AFTER: numa_node_size=" << numa_node_size(numa_node_, nullptr);
    return SUMMARIZE_ERROR_BATCH(batch);
}

}  // namespace memory
}  // namespace foedus
