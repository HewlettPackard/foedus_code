/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_ENGINE_MEMORY_HPP_
#define FOEDUS_MEMORY_ENGINE_MEMORY_HPP_

#include <string>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/thread/thread_id.hpp"

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
  explicit EngineMemory(Engine* engine) : engine_(engine), local_memory_(CXX11_NULLPTR) {}
  ErrorStack  initialize_once() CXX11_OVERRIDE;
  ErrorStack  uninitialize_once() CXX11_OVERRIDE;

  // accessors for child memories
  NumaNodeMemory* get_local_memory() const {
    return local_memory_;
  }
  NumaNodeMemoryRef* get_node_memory(foedus::thread::ThreadGroupId group) const {
    return node_memories_[group];
  }

  /** Report rough statistics of free memory */
  std::string     dump_free_memory_stat() const;

  /**
   * Returns the page resolver to convert volatile page ID to page pointer.
   * Any code can get the global page resolver from engine memory.
   * Note that this is only for volatile pages. As snapshot cache is per-node, there is no
   * global snapshot page resolver (just the node-local one should be enough).
   * @see thread::Thread::get_global_volatile_page_resolver()
   */
  const GlobalVolatilePageResolver& get_global_volatile_page_resolver() const {
    return global_volatile_page_resolver_;
  }

 private:
  Engine* const                   engine_;

  /**
   * NumaNodeMemory of this SOC engine.
   * Null if master engine.
   */
  NumaNodeMemory*                 local_memory_;

  /**
   * View of all node memories. We have ref objects for all SOCs even if this is a master engine.
   */
  std::vector<NumaNodeMemoryRef*> node_memories_;

  /**
   * Converts volatile page ID to page pointer.
   */
  GlobalVolatilePageResolver      global_volatile_page_resolver_;
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_ENGINE_MEMORY_HPP_
