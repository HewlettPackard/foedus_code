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
  check_transparent_hugepage_setting();
  ASSERT_ND(node_memories_.empty());
  const EngineOptions& options = engine_->get_options();

  // Can we at least start up?
  uint64_t total_threads = options.thread_.group_count_ * options.thread_.thread_count_per_group_;
  uint64_t minimal_page_pool = total_threads * options.memory_.private_page_pool_initial_grab_
    * storage::kPageSize;
  if ((static_cast<uint64_t>(options.memory_.page_pool_size_mb_per_node_)
      * options.thread_.group_count_ << 20) < minimal_page_pool) {
    return ERROR_STACK(kErrorCodeMemoryPagePoolTooSmall);
  }

  thread::ThreadGroupId numa_nodes = options.thread_.group_count_;
  GlobalVolatilePageResolver::Base bases[256];
  for (thread::ThreadGroupId node = 0; node < numa_nodes; ++node) {
    node_memories_.push_back(nullptr);
  }

  // in a big NUMA machine (DragonHawk) the following takes too long to do in one thread.
  // let's launch a thread for each of them.
  std::vector<std::thread> init_threads;
  for (thread::ThreadGroupId node = 0; node < numa_nodes; ++node) {
    init_threads.push_back(std::thread([this, node, &bases]() {
      ScopedNumaPreferred numa_scope(node);
      NumaNodeMemory* node_memory = new NumaNodeMemory(engine_, node);
      node_memories_[node] = node_memory;
      COERCE_ERROR(node_memory->initialize());  // TODO(Hideaki) collect errors
      PagePool& pool = node_memory->get_volatile_pool();
      bases[node] = pool.get_resolver().base_;
    }));
  }
  LOG(INFO) << "Launched threads to initialize node memory. waiting..";
  for (auto& init_thread : init_threads) {
    init_thread.join();
  }
  LOG(INFO) << "All node memories were initialized!";

  global_volatile_page_resolver_ = GlobalVolatilePageResolver(
    bases,
    numa_nodes,
    node_memories_[0]->get_volatile_pool().get_resolver().begin_,
    node_memories_[0]->get_volatile_pool().get_resolver().end_);
  return kRetOk;
}

ErrorStack EngineMemory::uninitialize_once() {
  LOG(INFO) << "Uninitializing EngineMemory..";
  ErrorStackBatch batch;
  if (!engine_->get_debug().is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }

  // even uninitialize takes long time. parallelize
  std::vector<std::thread> uninit_threads;
  for (uint16_t node = 0; node < node_memories_.size(); ++node) {
    NumaNodeMemory* node_memory = node_memories_[node];
    uninit_threads.push_back(std::thread([node_memory]() {
      COERCE_ERROR(node_memory->uninitialize());  // TODO(Hideaki) collect errors
    }));
  }
  LOG(INFO) << "Launched threads to uninitialize node memory. waiting..";
  for (auto& uninit_thread : uninit_threads) {
    uninit_thread.join();
  }
  LOG(INFO) << "All node memories were uninitialized!";
  for (uint16_t node = 0; node < node_memories_.size(); ++node) {
    delete node_memories_[node];
  }
  node_memories_.clear();
  return SUMMARIZE_ERROR_BATCH(batch);
}

NumaCoreMemory* EngineMemory::get_core_memory(thread::ThreadId id) const {
  thread::ThreadGroupId node = thread::decompose_numa_node(id);
  NumaNodeMemory* node_memory = get_node_memory(node);
  ASSERT_ND(node_memory);
  return node_memory->get_core_memory(id);
}

void EngineMemory::check_transparent_hugepage_setting() const {
  std::ifstream conf("/sys/kernel/mm/transparent_hugepage/enabled");
  if (conf.is_open()) {
    std::string line;
    std::getline(conf, line);
    conf.close();
    if (line == "[always] madvise never") {
      LOG(INFO) << "Great, THP is in always mode";
    } else {
      LOG(WARNING) << "THP is not in always mode ('" << line << "')."
        << " Not enabling THP reduces our performance up to 30%. Run the following to enable it:"
        << std::endl << "  sudo su"
        << std::endl << "  echo always > /sys/kernel/mm/transparent_hugepage/enabled";
    }
    return;
  }

  LOG(WARNING) << "Could not read /sys/kernel/mm/transparent_hugepage/enabled to check"
    << " if THP is enabled. This implies that THP is not available in this system."
    << " Using an old linux without THP reduces our performance up to 30%";
}

std::string EngineMemory::dump_free_memory_stat() const {
  std::stringstream ret;
  ret << "  == Free memory stat ==" << std::endl;
  thread::ThreadGroupId numa_nodes = engine_->get_options().thread_.group_count_;
  for (thread::ThreadGroupId node = 0; node < numa_nodes; ++node) {
    NumaNodeMemory* memory = node_memories_[node];
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
