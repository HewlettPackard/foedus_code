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
#include <foedus/memory/fwd.hpp>
#include <foedus/thread/thread_id.hpp>
#include <cassert>
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
 *  \li Page pool for the read-only bufferpool.
 *  \li Page pool for volatile read/write store.
 * So far we allocate separate memory for the second and third.
 * But, there is no fundamental reason to do so. It's for simplicity, and we might revisit it.
 */
class EngineMemory : public DefaultInitializable {
 public:
    EngineMemory() CXX11_FUNC_DELETE;
    explicit EngineMemory(const EngineOptions &options);
    ErrorStack  initialize_once() CXX11_OVERRIDE;
    ErrorStack  uninitialize_once() CXX11_OVERRIDE;

    const EngineOptions& get_options() const { return options_; }

    // accessors for child memories
    foedus::thread::ThreadGroupId get_node_memory_count() const {
        assert(node_memories_.size() <= foedus::thread::MAX_THREAD_GROUP_ID);
        return static_cast<foedus::thread::ThreadGroupId>(node_memories_.size());
    }
    std::vector<NumaNodeMemory*>& get_node_memories() { return node_memories_; }
    NumaNodeMemory* get_node_memory(foedus::thread::ThreadGroupId group) const {
        return node_memories_[group];
    }

 private:
    const EngineOptions&            options_;

    /**
     * List of NumaNodeMemory, one for each NUMA socket in the machine.
     * Index is NUMA node ID.
     */
    std::vector<NumaNodeMemory*>    node_memories_;
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_ENGINE_MEMORY_HPP_
