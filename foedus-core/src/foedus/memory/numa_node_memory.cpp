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
#include <foedus/assert_nd.hpp>
namespace foedus {
namespace memory {
NumaNodeMemory::NumaNodeMemory(Engine* engine, thread::ThreadGroupId numa_node)
    : engine_(engine), numa_node_(numa_node),
        cores_(engine_->get_options().thread_.thread_count_per_group_),
        loggers_(engine_->get_options().log_.loggers_per_node_), page_pool_(engine, numa_node) {
}

ErrorStack NumaNodeMemory::initialize_once() {
    LOG(INFO) << "Initializing NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " BEFORE: numa_node_size=" << ::numa_node_size(numa_node_, nullptr);

    CHECK_ERROR(page_pool_.initialize());
    CHECK_ERROR(initialize_read_write_set_memory());
    CHECK_ERROR(initialize_page_offset_chunk_memory());
    CHECK_ERROR(initialize_log_buffers_memory());
    for (auto ordinal = 0; ordinal < cores_; ++ordinal) {
        CHECK_ERROR(initialize_core_memory(ordinal));
    }
    ASSERT_ND(page_pool_.is_initialized());
    ASSERT_ND(core_memories_.size() == cores_);
    ASSERT_ND(read_set_memory_pieces_.size() == cores_);
    ASSERT_ND(write_set_memory_pieces_.size() == cores_);
    ASSERT_ND(page_offset_chunk_memory_pieces_.size() == cores_);
    ASSERT_ND(log_buffer_memory_pieces_.size() == cores_);

    LOG(INFO) << "Initialized NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " AFTER: numa_node_size=" << ::numa_node_size(numa_node_, nullptr);
    return RET_OK;
}
ErrorStack NumaNodeMemory::initialize_read_write_set_memory() {
    uint32_t readsets = engine_->get_options().xct_.max_read_set_size_;
    uint32_t writesets = engine_->get_options().xct_.max_write_set_size_;
    size_t readset_size = sizeof(xct::XctAccess) * readsets;
    size_t writeset_size = sizeof(xct::WriteXctAccess) * writesets;

    CHECK_ERROR(allocate_numa_memory(cores_ * readset_size, &read_set_memory_));
    CHECK_ERROR(allocate_numa_memory(cores_ * writeset_size, &write_set_memory_));
    for (auto ordinal = 0; ordinal < cores_; ++ordinal) {
        read_set_memory_pieces_.push_back(reinterpret_cast<xct::XctAccess*>(
            read_set_memory_.get_block()) + (readsets * ordinal));
        write_set_memory_pieces_.push_back(reinterpret_cast<xct::WriteXctAccess*>(
            write_set_memory_.get_block()) + (writesets * ordinal));
    }

    return RET_OK;
}
ErrorStack NumaNodeMemory::initialize_page_offset_chunk_memory() {
    size_t size_per_core = sizeof(PagePoolOffsetChunk);
    size_t total_size = size_per_core * cores_;
    LOG(INFO) << "Initializing page_offset_chunk_memory_. total_size=" << total_size << " bytes";
    if (total_size < kHugepageSize) {
        // Just one per NUMA node. Not a significant waste.
        total_size = kHugepageSize;
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
    uint64_t size_per_core_ = engine_->get_options().log_.log_buffer_kb_ << 10;
    uint64_t private_total = (cores_ * size_per_core_);
    LOG(INFO) << "Initializing log_buffer_memory_. total_size=" << private_total;
    CHECK_ERROR(allocate_numa_memory(private_total, &log_buffer_memory_));
    LOG(INFO) << "log_buffer_memory_ allocated. addr=" << log_buffer_memory_.get_block();
    for (auto ordinal = 0; ordinal < cores_; ++ordinal) {
        AlignedMemorySlice piece(&log_buffer_memory_, size_per_core_ * ordinal, size_per_core_);
        LOG(INFO) << "log_buffer_piece[" << ordinal << "] addr=" << piece.get_block();
        log_buffer_memory_pieces_.push_back(piece);
    }

    return RET_OK;
}


ErrorStack NumaNodeMemory::initialize_core_memory(thread::ThreadLocalOrdinal ordinal) {
    auto core_id = thread::compose_thread_id(numa_node_, ordinal);
    NumaCoreMemory* core_memory = new NumaCoreMemory(engine_, this, core_id);
    core_memories_.push_back(core_memory);
    CHECK_ERROR(core_memory->initialize());
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
    log_buffer_memory_pieces_.clear();
    log_buffer_memory_.release_block();
    batch.emprace_back(page_pool_.uninitialize());

    LOG(INFO) << "Uninitialized NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
        << " AFTER: numa_node_size=" << ::numa_node_size(numa_node_, nullptr);
    return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack NumaNodeMemory::allocate_numa_memory(size_t size, AlignedMemory *out) {
    ASSERT_ND(out);
    out->alloc(size, 1 << 12, AlignedMemory::NUMA_ALLOC_ONNODE, numa_node_);
    if (out->is_null()) {
        return ERROR_STACK(ERROR_CODE_OUTOFMEMORY);
    }
    return RET_OK;
}

}  // namespace memory
}  // namespace foedus
