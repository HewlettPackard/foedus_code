/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_NUMA_CORE_MEMORY_HPP_
#define FOEDUS_MEMORY_NUMA_CORE_MEMORY_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
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
 * All memories here are allocated/freed via ::numa_alloc_xxx() and ::numa_free()
 * (except the user specifies to not use them).
 */
class NumaCoreMemory : public virtual Initializable {
 public:
    /**
     * Description of constructor.
     */
    NumaCoreMemory(NumaNodeMemory *node_memory, foedus::thread::ThreadId core);
    /**
     * Description of destructor.
     */
    ~NumaCoreMemory();

    // Disable default constructors
    NumaCoreMemory() CXX11_FUNC_DELETE;
    NumaCoreMemory(const NumaCoreMemory&) CXX11_FUNC_DELETE;
    NumaCoreMemory& operator=(const NumaCoreMemory&) CXX11_FUNC_DELETE;

    INITIALIZABLE_DEFAULT;

 private:
    /**
     * The grand-parent memory repository, which holds the parent of this object.
     */
    EngineMemory* const     engine_memory_;

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

    bool                    initialized_;
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_NUMA_CORE_MEMORY_HPP_
