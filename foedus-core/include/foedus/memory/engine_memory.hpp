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
class EngineMemory : public virtual Initializable {
 public:
    explicit EngineMemory(const EngineOptions &options);
    ~EngineMemory();

    // Disable default constructors
    EngineMemory() CXX11_FUNC_DELETE;
    EngineMemory(const EngineMemory&) CXX11_FUNC_DELETE;
    EngineMemory& operator=(const EngineMemory&) CXX11_FUNC_DELETE;

    ErrorStack  initialize() CXX11_OVERRIDE;
    bool        is_initialized() const CXX11_OVERRIDE { return initialized_; }
    ErrorStack  uninitialize() CXX11_OVERRIDE;

 private:
    const EngineOptions&            options_;
    bool                            initialized_;

    /**
     * List of NumaNodeMemory, one for each NUMA socket in the machine.
     * Index is NUMA node ID.
     */
    std::vector<NumaNodeMemory*>    node_memories_;
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_ENGINE_MEMORY_HPP_
