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
#include <foedus/xct/xct_access.hpp>
#include <glog/logging.h>
#include <numa.h>
#include <cassert>
namespace foedus {
namespace memory {
NumaNodeMemory::NumaNodeMemory(Engine* engine, thread::ThreadGroupId numa_node)
    : engine_(engine), numa_node_(numa_node),
        cores_(engine_->get_options().thread_.thread_count_per_group_) {}

ErrorStack NumaNodeMemory::initialize_once() {
    LOG(INFO) << "Initializing NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " BEFORE: numa_node_size=" << ::numa_node_size(numa_node_, nullptr);

    CHECK_ERROR(initialize_read_write_set_memory());
    for (auto ordinal = 0; ordinal < cores_; ++ordinal) {
        CHECK_ERROR(initialize_core_memory(ordinal));
    }

    LOG(INFO) << "Initialized NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " AFTER: numa_node_size=" << ::numa_node_size(numa_node_, nullptr);
    return RET_OK;
}
ErrorStack NumaNodeMemory::initialize_read_write_set_memory() {
    uint32_t readsets = engine_->get_options().xct_.max_read_set_size_;
    uint32_t writesets = engine_->get_options().xct_.max_write_set_size_;
    size_t readset_size = sizeof(xct::XctAccess) * readsets;
    size_t writeset_size = sizeof(xct::XctAccess) * writesets;

    CHECK_ERROR(allocate_numa_memory(cores_ * readset_size, &read_set_memory_));
    CHECK_ERROR(allocate_numa_memory(cores_ * writeset_size, &write_set_memory_));
    for (auto ordinal = 0; ordinal < cores_; ++ordinal) {
        read_set_memory_pieces_.push_back(reinterpret_cast<xct::XctAccess*>(
            read_set_memory_.get_block()) + readsets);
        write_set_memory_pieces_.push_back(reinterpret_cast<xct::XctAccess*>(
            write_set_memory_.get_block()) + writesets);
    }

    return RET_OK;
}
ErrorStack NumaNodeMemory::initialize_core_memory(thread::ThreadLocalOrdinal ordinal) {
    auto core_id = thread::compose_thread_id(numa_node_, ordinal);
    core_memories_.push_back(new NumaCoreMemory(engine_, this, core_id, ordinal));
    CHECK_ERROR(core_memories_.back()->initialize());
    return RET_OK;
}


ErrorStack NumaNodeMemory::uninitialize_once() {
    LOG(INFO) << "Uninitializing NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " BEFORE: numa_node_size=" << ::numa_node_size(numa_node_, nullptr);

    ErrorStackBatch batch;
    batch.uninitialize_and_delete_all(&core_memories_);
    write_set_memory_pieces_.clear();
    write_set_memory_.release_block();
    read_set_memory_pieces_.clear();
    read_set_memory_.release_block();

    LOG(INFO) << "Uninitialized NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " AFTER: numa_node_size=" << ::numa_node_size(numa_node_, nullptr);
    return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack NumaNodeMemory::allocate_numa_memory(size_t size, AlignedMemory *out) {
    assert(out);
    AlignedMemory allocated(size, 1 << 12, AlignedMemory::NUMA_ALLOC_ONNODE, numa_node_);
    if (allocated.is_null()) {
        return ERROR_STACK(ERROR_CODE_OUTOFMEMORY);
    }

    *out = std::move(allocated);
    return RET_OK;
}

}  // namespace memory
}  // namespace foedus
