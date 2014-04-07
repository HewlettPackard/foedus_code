/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_NUMA_NODE_MEMORY_HPP_
#define FOEDUS_MEMORY_NUMA_NODE_MEMORY_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/initializable.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/thread/thread_id.hpp>
#include <vector>
namespace foedus {
namespace memory {
/**
 * @brief Repository of memories dynamically acquired and shared within one NUMA node (socket).
 * @ingroup MEMHIERARCHY THREAD
 * @details
 * One NumaNodeMemory corresponds to one foedus::thread::ThreadGroup.
 * All threads in the thread group belong to the NUMA node, thus sharing memories between
 * them must be efficient.
 * So, all memories here are allocated/freed via ::numa_alloc_xxx() and ::numa_free()
 * (except the user specifies to not use them).
 */
class NumaNodeMemory : public virtual Initializable {
 public:
    /**
     * Description of constructor.
     */
    NumaNodeMemory(EngineMemory *engine_memory, foedus::thread::thread_group_id numa_node);
    /**
     * Description of destructor.
     */
    ~NumaNodeMemory();

    // Disable default constructors
    NumaNodeMemory() CXX11_FUNC_DELETE;
    NumaNodeMemory(const NumaNodeMemory&) CXX11_FUNC_DELETE;
    NumaNodeMemory& operator=(const NumaNodeMemory&) CXX11_FUNC_DELETE;

    INITIALIZABLE_DEFAULT;

    EngineMemory* get_engine_memory() const { return engine_memory_; }
    foedus::thread::thread_group_id get_numa_node() const { return numa_node_; }

    // accessors for child memories
    foedus::thread::thread_local_ordinal get_core_memory_count() const {
        assert(core_memories_.size() <= foedus::thread::MAX_THREAD_LOCAL_ORDINAL);
        return static_cast<foedus::thread::thread_local_ordinal>(core_memories_.size());
    }
    std::vector<NumaCoreMemory*>& get_core_memories() { return core_memories_; }
    NumaCoreMemory* get_core_memory(foedus::thread::thread_id id) const {
        return core_memories_[foedus::thread::decompose_numa_local_ordinal(id)];
    }
    NumaCoreMemory* get_core_memory(foedus::thread::thread_local_ordinal ordinal) const {
        return core_memories_[ordinal];
    }

 private:
    /**
     * The parent memory repository, which holds this object.
     */
    EngineMemory* const                     engine_memory_;

    /**
     * The NUMA node this memory is allocated for.
     */
    const foedus::thread::thread_group_id   numa_node_;

    /**
     * List of NumaCoreMemory, one for each core in this node.
     * Index is local ordinal of the NUMA cores.
     */
    std::vector<NumaCoreMemory*>            core_memories_;

    bool                                    initialized_;
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_NUMA_NODE_MEMORY_HPP_
