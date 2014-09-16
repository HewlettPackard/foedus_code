/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/memory/engine_memory.hpp"

#include <numa.h>
#include <glog/logging.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace memory {
ErrorStack EngineMemory::initialize_once() {
  LOG(INFO) << "Initializing EngineMemory..";
  if (!engine_->get_debug().is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  } else if (::numa_available() < 0) {
    return ERROR_STACK(kErrorCodeMemoryNumaUnavailable);
  }
  ASSERT_ND(node_memories_.empty());
  const EngineOptions& options = engine_->get_options();
  thread::ThreadGroupId numa_nodes = options.thread_.group_count_;
  GlobalVolatilePageResolver::Base bases[256];
  uint64_t pool_begin = 0, pool_end = 0;
  for (thread::ThreadGroupId node = 0; node < numa_nodes; ++node) {
    NumaNodeMemoryRef* ref = new NumaNodeMemoryRef(engine_, node);
    node_memories_.push_back(ref);
    bases[node] = ref->get_volatile_pool().get_base();
    pool_begin = ref->get_volatile_pool().get_resolver().begin_;
    pool_end = ref->get_volatile_pool().get_resolver().end_;
  }
  global_volatile_page_resolver_ = GlobalVolatilePageResolver(
    bases,
    numa_nodes,
    pool_begin,
    pool_end);

  // Initialize local memory.
  if (!engine_->is_master()) {
    soc::SocId node = engine_->get_soc_id();
    local_memory_ = new NumaNodeMemory(engine_, node);
    CHECK_ERROR(local_memory_->initialize());
    LOG(INFO) << "Node memory-" << node << " was initialized!";
  }
  return kRetOk;
}

ErrorStack EngineMemory::uninitialize_once() {
  LOG(INFO) << "Uninitializing EngineMemory..";
  ErrorStackBatch batch;
  if (!engine_->get_debug().is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  node_memories_.clear();

  // Uninitialize local memory.
  if (!engine_->is_master() && local_memory_) {
    soc::SocId node = engine_->get_soc_id();
    batch.emprace_back(local_memory_->uninitialize());
    delete local_memory_;
    local_memory_ = nullptr;
    LOG(INFO) << "Node memory-" << node << " was uninitialized!";
  }
  return SUMMARIZE_ERROR_BATCH(batch);
}

std::string EngineMemory::dump_free_memory_stat() const {
  std::stringstream ret;
  ret << "  == Free memory stat ==" << std::endl;
  thread::ThreadGroupId numa_nodes = engine_->get_options().thread_.group_count_;
  for (thread::ThreadGroupId node = 0; node < numa_nodes; ++node) {
    NumaNodeMemoryRef* memory = node_memories_[node];
    ret << " - Node_" << static_cast<int>(node) << " -" << std::endl;
    ret << memory->dump_free_memory_stat();
    if (node + 1U < numa_nodes) {
      ret << std::endl;
    }
  }
  return ret.str();
}

}  // namespace memory
}  // namespace foedus
