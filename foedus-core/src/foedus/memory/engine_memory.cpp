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
#include <foedus/storage/storage_id.hpp>
#include <foedus/thread/thread_id.hpp>
#include <glog/logging.h>
#include <numa.h>
namespace foedus {
namespace memory {
ErrorStack EngineMemory::initialize_once() {
    LOG(INFO) << "Initializing EngineMemory..";
    if (!engine_->get_debug().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    } else if (::numa_available() < 0) {
        return ERROR_STACK(ERROR_CODE_MEMORY_NUMA_UNAVAILABLE);
    }
    ASSERT_ND(node_memories_.empty());
    const EngineOptions& options = engine_->get_options();

    // Can we at least start up?
    size_t total_threads = options.thread_.group_count_ * options.thread_.thread_count_per_group_;
    size_t minimal_page_pool = total_threads * options.memory_.private_page_pool_initial_grab_
        * storage::PAGE_SIZE;
    if ((static_cast<uint64_t>(options.memory_.page_pool_size_mb_) << 20) < minimal_page_pool) {
        return ERROR_STACK(ERROR_CODE_MEMORY_PAGE_POOL_TOO_SMALL);
    }

    CHECK_ERROR(page_pool_.initialize());

    thread::ThreadGroupId numa_nodes = options.thread_.group_count_;
    for (thread::ThreadGroupId node = 0; node < numa_nodes; ++node) {
        ScopedNumaPreferred numa_scope(node);
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

    batch.uninitialize_and_delete_all(&node_memories_);
    batch.emprace_back(page_pool_.uninitialize());
    return SUMMARIZE_ERROR_BATCH(batch);
}

NumaCoreMemory* EngineMemory::get_core_memory(thread::ThreadId id) const {
    thread::ThreadGroupId node = thread::decompose_numa_node(id);
    NumaNodeMemory* node_memory = get_node_memory(node);
    ASSERT_ND(node_memory);
    return node_memory->get_core_memory(id);
}


}  // namespace memory
}  // namespace foedus
