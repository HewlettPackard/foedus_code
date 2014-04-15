/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_NUMA_NODE_MEMORY_HPP_
#define FOEDUS_MEMORY_NUMA_NODE_MEMORY_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/memory/aligned_memory.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/xct/fwd.hpp>
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
 * So, all memories here are allocated/freed via ::numa_alloc_interleaved(), ::numa_alloc_onnode(),
 * and ::numa_free() (except the user specifies to not use them).
 */
class NumaNodeMemory CXX11_FINAL : public DefaultInitializable {
 public:
    NumaNodeMemory() CXX11_FUNC_DELETE;
    NumaNodeMemory(Engine* engine, foedus::thread::ThreadGroupId numa_node);
    ErrorStack  initialize_once() CXX11_OVERRIDE;
    ErrorStack  uninitialize_once() CXX11_OVERRIDE;

    foedus::thread::ThreadGroupId get_numa_node() const { return numa_node_; }

    // accessors for child memories
    foedus::thread::ThreadLocalOrdinal get_core_memory_count() const {
        assert(core_memories_.size() <= foedus::thread::MAX_THREAD_LOCAL_ORDINAL);
        return static_cast<foedus::thread::ThreadLocalOrdinal>(core_memories_.size());
    }
    std::vector<NumaCoreMemory*>& get_core_memories() { return core_memories_; }
    NumaCoreMemory* get_core_memory(foedus::thread::ThreadId id) const {
        return core_memories_[foedus::thread::decompose_numa_local_ordinal(id)];
    }
    NumaCoreMemory* get_core_memory(foedus::thread::ThreadLocalOrdinal ordinal) const {
        return core_memories_[ordinal];
    }

    /**
     * Allocate a memory of the given size on this NUMA node.
     * @param[in] size byte size of the memory to acquire
     * @param[out] out allocated memory is moved to object
     * @return Expect OUTOFMEMORY error.
     */
    ErrorStack      allocate_numa_memory(size_t size, AlignedMemory *out);

    xct::XctAccess* get_read_set_memory_piece(foedus::thread::ThreadLocalOrdinal core_ordinal) {
        return read_set_memory_pieces_[core_ordinal];
    }
    xct::XctAccess* get_write_set_memory_piece(foedus::thread::ThreadLocalOrdinal core_ordinal) {
        return write_set_memory_pieces_[core_ordinal];
    }

 private:
    /** initialize read-set and write-set memory. */
    ErrorStack      initialize_read_write_set_memory();
    /** initialize child memories per core */
    ErrorStack      initialize_core_memory(thread::ThreadLocalOrdinal ordinal);

    Engine* const                           engine_;

    /**
     * The NUMA node this memory is allocated for.
     */
    const foedus::thread::ThreadGroupId     numa_node_;

    /** Number of cores in this node. */
    const thread::ThreadLocalOrdinal        cores_;

    /**
     * List of NumaCoreMemory, one for each core in this node.
     * Index is local ordinal of the NUMA cores.
     */
    std::vector<NumaCoreMemory*>            core_memories_;

    /**
     * Memory to keep track of read-set during transactions.
     * To better utilize HugePages, we allocate this in node level for all cores rather than in
     * individual core level. NumaCoreMemory merely gets a piece of this memory.
     */
    AlignedMemory                           read_set_memory_;
    std::vector<xct::XctAccess*>            read_set_memory_pieces_;

    /**
     * Memory to keep track of write-set during transactions. Same above.
     */
    AlignedMemory                           write_set_memory_;
    std::vector<xct::XctAccess*>            write_set_memory_pieces_;
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_NUMA_NODE_MEMORY_HPP_
