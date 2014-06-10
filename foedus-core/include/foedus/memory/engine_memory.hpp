/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_ENGINE_MEMORY_HPP_
#define FOEDUS_MEMORY_ENGINE_MEMORY_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/memory/aligned_memory.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/memory/page_pool.hpp>
#include <foedus/memory/page_resolver.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/assert_nd.hpp>
#include <vector>
namespace foedus {
namespace memory {
/**
 * @brief Repository of all memories dynamically acquired and shared within one database engine.
 * @ingroup MEMHIERARCHY ENGINE
 * @details
 * @par Overview
 * This is the top-level memory repository in our engine.
 * All other memory types are contained in this object.
 *
 * @par List of engine-wide memories
 *  \li List of NumaNodeMemory, one for each NUMA socket in the machine.
 *  \li Page pool for volatile read/write store (VolatilePage) and
 * the read-only bufferpool (SnapshotPage).
 */
class EngineMemory CXX11_FINAL : public DefaultInitializable {
 public:
    EngineMemory() CXX11_FUNC_DELETE;
    explicit EngineMemory(Engine* engine) : engine_(engine) {}
    ErrorStack  initialize_once() CXX11_OVERRIDE;
    ErrorStack  uninitialize_once() CXX11_OVERRIDE;

    // accessors for child memories
    foedus::thread::ThreadGroupId get_node_memory_count() const {
        ASSERT_ND(node_memories_.size() <= foedus::thread::MAX_THREAD_GROUP_ID);
        return static_cast<foedus::thread::ThreadGroupId>(node_memories_.size());
    }
    NumaNodeMemory* get_node_memory(foedus::thread::ThreadGroupId group) const {
        return node_memories_[group];
    }
    NumaCoreMemory* get_core_memory(foedus::thread::ThreadId id) const;

    /**
     * Returns the page resolver to convert page ID to page pointer.
     * Any code can get the global page resolver from engine memory, but the most efficient
     * way is to use the global page page resolver per core because
     * it never requires remote memory access.
     * @see thread::Thread::get_global_page_resolver()
     */
    const GlobalPageResolver& get_global_page_resolver() const { return global_page_resolver_; }

 private:
    Engine* const                   engine_;

    /**
     * List of NumaNodeMemory, one for each NUMA socket in the machine.
     * Index is NUMA node ID.
     */
    std::vector<NumaNodeMemory*>    node_memories_;

    /**
     * Converts page ID to page pointer.
     */
    GlobalPageResolver              global_page_resolver_;
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_ENGINE_MEMORY_HPP_
