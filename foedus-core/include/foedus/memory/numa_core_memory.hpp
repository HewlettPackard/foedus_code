/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_NUMA_CORE_MEMORY_HPP_
#define FOEDUS_MEMORY_NUMA_CORE_MEMORY_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/thread/thread_id.hpp>
namespace foedus {
namespace memory {
/**
 * @brief Repository of memories dynamically acquired within one CPU core (thread).
 * @ingroup MEMHIERARCHY THREAD
 * @details
 * One NumaCoreMemory corresponds to one foedus::thread::Thread.
 * Each Thread exclusively access its NumaCoreMemory so that it needs no synchronization
 * nor causes cache misses/cache-line ping-pongs.
 * All memories here are allocated/freed via ::numa_alloc_interleaved(), ::numa_alloc_onnode(),
 * and ::numa_free() (except the user specifies to not use them).
 */
class NumaCoreMemory CXX11_FINAL : public DefaultInitializable {
 public:
    NumaCoreMemory() CXX11_FUNC_DELETE;
    NumaCoreMemory(Engine* engine, NumaNodeMemory *node_memory, foedus::thread::ThreadId core_id)
        : engine_(engine), node_memory_(node_memory), core_id_(core_id),
            core_local_ordinal_(foedus::thread::decompose_numa_local_ordinal(core_id)) {
    }
    ErrorStack  initialize_once() CXX11_OVERRIDE;
    ErrorStack  uninitialize_once() CXX11_OVERRIDE;

 private:
    Engine* const           engine_;

    /**
     * The parent memory repository, which holds this object.
     */
    NumaNodeMemory* const   node_memory_;

    /**
     * Global ID of the NUMA core this memory is allocated for.
     */
    const foedus::thread::ThreadId core_id_;

    /**
     * Local ordinal of the NUMA core this memory is allocated for.
     */
    const foedus::thread::ThreadLocalOrdinal core_local_ordinal_;
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_NUMA_CORE_MEMORY_HPP_
