/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <foedus/memory/numa_node_memory.hpp>
#include <foedus/memory/page_pool.hpp>
#include <foedus/thread/thread_options.hpp>
#include <foedus/xct/xct_access.hpp>
#include <glog/logging.h>
#include <numa.h>
#include <cassert>
namespace foedus {
namespace memory {
NumaNodeMemory::NumaNodeMemory(Engine* engine, thread::ThreadGroupId numa_node)
    : engine_(engine), numa_node_(numa_node),
        cores_(engine_->get_options().thread_.thread_count_per_group_),
        loggers_(assorted::int_div_ceil(engine_->get_options().log_.log_paths_.size(),
                    engine_->get_options().thread_.group_count_)) {
}

ErrorStack NumaNodeMemory::initialize_once() {
    LOG(INFO) << "Initializing NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " BEFORE: numa_node_size=" << ::numa_node_size(numa_node_, nullptr);

    CHECK_ERROR(initialize_read_write_set_memory());
    CHECK_ERROR(initialize_page_offset_chunk_memory());
    CHECK_ERROR(initialize_log_buffers_memory());
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
ErrorStack NumaNodeMemory::initialize_page_offset_chunk_memory() {
    size_t size_per_core = sizeof(PagePoolOffsetChunk);
    size_t total_size = size_per_core * cores_ * size_per_core;
    LOG(INFO) << "Initializing page_offset_chunk_memory_. total_size=" << total_size << " bytes";
    if (total_size < HUGEPAGE_SIZE) {
        // Just one per NUMA node. Not a significant waste.
        total_size = HUGEPAGE_SIZE;
        LOG(INFO) << "Allocating extra space to utilize hugepage.";
    }
    CHECK_ERROR(allocate_numa_memory(total_size, &page_offset_chunk_memory_));
    for (auto ordinal = 0; ordinal < cores_; ++ordinal) {
        PagePoolOffsetChunk* chunk = reinterpret_cast<PagePoolOffsetChunk*>(
            page_offset_chunk_memory_.get_block()) + ordinal;
        chunk->clear();
        page_offset_chunk_memory_pieces_.push_back(chunk);
    }

    return RET_OK;
}

ErrorStack NumaNodeMemory::initialize_log_buffers_memory() {
    size_t size_per_core = engine_->get_options().log_.thread_buffer_kb_;
    uint64_t private_total = (cores_ * size_per_core) << 10;
    LOG(INFO) << "Initializing thread_buffer_memory_. total_size=" << private_total;
    CHECK_ERROR(allocate_numa_memory(private_total << 10, &thread_buffer_memory_));
    for (auto ordinal = 0; ordinal < cores_; ++ordinal) {
        char* piece = reinterpret_cast<char*>(thread_buffer_memory_.get_block())
            + size_per_core * ordinal;
        thread_buffer_memory_pieces_.push_back(piece);
    }

    uint64_t size_per_logger = (engine_->get_options().log_.logger_buffer_kb_) << 10;
    uint64_t logger_buffer = size_per_logger * loggers_;
    LOG(INFO) << "Initializing logger_buffer_memory_. size=" << logger_buffer;
    CHECK_ERROR(allocate_numa_memory(logger_buffer, &logger_buffer_memory_));
    for (auto logger = 0; logger < loggers_; ++logger) {
        char* piece = reinterpret_cast<char*>(logger_buffer_memory_.get_block())
            + size_per_logger * logger;
        logger_buffer_memory_pieces_.push_back(piece);
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
    page_offset_chunk_memory_pieces_.clear();
    page_offset_chunk_memory_.release_block();
    write_set_memory_pieces_.clear();
    write_set_memory_.release_block();
    read_set_memory_pieces_.clear();
    read_set_memory_.release_block();
    thread_buffer_memory_pieces_.clear();
    thread_buffer_memory_.release_block();
    logger_buffer_memory_pieces_.clear();
    logger_buffer_memory_.release_block();

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
