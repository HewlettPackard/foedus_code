/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SOC_SHARED_MEMORY_REPO_HPP_
#define FOEDUS_SOC_SHARED_MEMORY_REPO_HPP_

#include <stdint.h>

#include "foedus/cxx11.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/memory/shared_memory.hpp"

namespace foedus {
namespace soc {

const uint16_t kMaxSocs = 256;

/**
 * @brief Repository of all shared memory in one FOEDUS instance.
 * @ingroup SOC
 * @details
 * These are the memories shared across SOCs, which are allocated at initialization.
 * Be super careful on objects placed in these shared memories. You can't put objects with
 * heap-allocated contents, such as std::string.
 */
struct SharedMemoryRepo {
  SharedMemoryRepo() {}
  ~SharedMemoryRepo() { deallocate_shared_memories(); }

  // Disable copy constructor
  SharedMemoryRepo(const SharedMemoryRepo &other) CXX11_FUNC_DELETE;
  SharedMemoryRepo& operator=(const SharedMemoryRepo &other) CXX11_FUNC_DELETE;

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  /**
   * Move constructor that steals the memory block from other.
   */
  SharedMemoryRepo(SharedMemoryRepo &&other);
  /**
   * Move assignment operator that steals the memory block from other.
   */
  SharedMemoryRepo& operator=(SharedMemoryRepo &&other);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

  /** Master process creates shared memories by calling this method*/
  ErrorStack  allocate_shared_memories(const EngineOptions& options);

  /**
   * Child processes (emulated or not) set a reference to shared memory and receive
   * the EngnieOption value by calling this method.
   * @param[in] master_upid Universal (or Unique) ID of the master process. This is the only
   * parameter that has to be passed from the master process to child processes.
   * @param[out] options One of the shared memory contains the EngineOption values.
   * This method also retrieves them from the shared memory.
   */
  ErrorStack  attach_shared_memories(uint64_t master_upid, EngineOptions* options);

  /**
   * @brief Marks shared memories as being removed so that it will be reclaimed when all processes
   * detach it.
   * @details
   * This is part of deallocation of shared memory in master process.
   * After calling this method, no process can attach this shared memory. So, do not call this
   * too early. However, on the other hand, do not call this too late.
   * If the master process dies for an unexpected reason, the shared memory remains until
   * next reboot. Call it as soon as child processes ack-ed that they have attached the memory
   * or that there are some issues the master process should exit.
   * This method is idempotent, meaning you can safely call this many times.
   */
  void        mark_for_release();

  /** Detaches and releases the shared memories. In child processes, this just detaches. */
  void        deallocate_shared_memories();

  /**
   * Each module needs a small shared memory. This packs all of them into one shared memory.
   * This memory is either tied to NUMA node-0 or intereleaved, so no locality.
   * Per-node memories are separated to their own shared memories below.
   */
  memory::SharedMemory global_memory_;

  /**
   * Per-node memories that have to be accessible to other SOCs.
   *  \li Channel memory between root and individual SOCs.
   *  \li All inputs/output of impersonated sessions are stored in this memory.
   *  \li Memory for MCS-locking.
   *  \li etc
   * Each memory is tied to an SOC.
   */
  memory::SharedMemory shared_node_memories_[kMaxSocs];

  /**
   * Memory for volatile page pool in each SOC.
   * This is by far the biggest shared memory.
   * These are tied to each SOC, but still shared.
   */
  memory::SharedMemory volatile_pools_[kMaxSocs];
};

}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SHARED_MEMORY_REPO_HPP_
