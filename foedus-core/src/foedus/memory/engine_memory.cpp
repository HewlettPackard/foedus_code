/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/debugging/debugging_supports.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/memory/numa_node_memory.hpp>
#include <foedus/thread/thread_id.hpp>
#include <glog/logging.h>
namespace foedus {
namespace memory {
ErrorStack EngineMemory::initialize_once() {
    LOG(INFO) << "Initializing EngineMemory..";
    if (!engine_->get_debug().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    const thread::ThreadOptions &options = engine_->get_options().thread_;

    LOG(INFO) << "Initializing VolatilePage Pool...";
    {
        AlignedMemory::AllocType alloc_type = AlignedMemory::NUMA_ALLOC_INTERLEAVED;
        size_t size = 1ULL << 28;
        size_t alignment = 1ULL << 12;
        volatile_page_pool_ = std::move(AlignedMemory(size, alignment, alloc_type, 0));
    }
    LOG(INFO) << "Initialized VolatilePage Pool. " << volatile_page_pool_;

    for (thread::ThreadGroupId node = 0; node < options.group_count_; ++node) {
        node_memories_.push_back(new NumaNodeMemory(engine_, node));
        CHECK_ERROR(node_memories_.back()->initialize());
    }
    return RET_OK;
}

ErrorStack EngineMemory::uninitialize_once() {
    LOG(INFO) << "Uninitializing EngineMemory..";
    ErrorStackBatch batch;
    if (!engine_->get_debug().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }

    LOG(INFO) << "Releasing VolatilePage Pool...";
    volatile_page_pool_.release_block();
    LOG(INFO) << "Released VolatilePage Pool. " << volatile_page_pool_;

    batch.uninitialize_and_delete_all(&node_memories_);
    return SUMMARIZE_ERROR_BATCH(batch);
}

NumaCoreMemory* EngineMemory::get_core_memory(thread::ThreadId id) const {
    thread::ThreadGroupId node = thread::decompose_numa_node(id);
    NumaNodeMemory* node_memory = get_node_memory(node);
    assert(node_memory);
    return node_memory->get_core_memory(id);
}


}  // namespace memory
}  // namespace foedus
