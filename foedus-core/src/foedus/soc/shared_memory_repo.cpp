/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/shared_memory_repo.hpp"

#include <unistd.h>

#include <cstring>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "foedus/assert_nd.hpp"

namespace foedus {
namespace soc {

std::string get_self_path() {
  pid_t pid = ::getpid();
  std::string pid_str = std::to_string(pid);
  return std::string("/tmp/libfoedus_shm_") + pid_str;
}
std::string get_master_path(uint64_t master_upid) {
  std::string pid_str = std::to_string(master_upid);
  return std::string("/tmp/libfoedus_shm_") + pid_str;
}

void allocate_one_node(
  uint16_t node,
  uint64_t shared_node_memory_size,
  uint64_t volatile_pool_size,
  SharedMemoryRepo* repo) {
  std::string node_memory_path = get_self_path() + std::string("_node_") + std::to_string(node);
  repo->shared_node_memories_[node].alloc(node_memory_path, shared_node_memory_size, node);
  std::string volatile_pool_path = get_self_path() + std::string("_vpool_") + std::to_string(node);
  repo->volatile_pools_[node].alloc(volatile_pool_path, volatile_pool_size, node);
}

ErrorStack SharedMemoryRepo::allocate_shared_memories(const EngineOptions& options) {
  // We place a serialized EngineOptions in the beginning of shared memory.
  std::stringstream options_stream;
  options.save_to_stream(&options_stream);
  std::string xml(options_stream.str());

  // construct unique meta files using PID.
  std::string global_memory_path = get_self_path() + std::string("_global");
  global_memory_.alloc(global_memory_path, 1ULL << 21, 0);  // TODO(Hideaki) figure out the size
  if (global_memory_.is_null()) {
    deallocate_shared_memories();
    return ERROR_STACK(kErrorCodeSocShmAllocFailed);
  }

  // copy the EngineOptions string into the beginning of the global memory
  uint64_t xml_size = xml.size();
  std::memcpy(global_memory_.get_block(), &xml_size, sizeof(xml_size));
  std::memcpy(global_memory_.get_block() + sizeof(xml_size), xml.data(), xml_size);

  // the following is parallelized
  std::vector< std::thread > alloc_threads;
  for (uint16_t node = 0; node < options.thread_.group_count_; ++node) {
    uint64_t shared_node_memory_size = 1ULL << 25;  // TODO(Hideaki) figure out the size
    uint64_t volatile_pool_size
      = static_cast<uint64_t>(options.memory_.page_pool_size_mb_per_node_) << 20;
    alloc_threads.emplace_back(std::thread(
      allocate_one_node,
      node,
      shared_node_memory_size,
      volatile_pool_size,
      this));
  }

  bool failed = false;
  for (uint16_t node = 0; node < options.thread_.group_count_; ++node) {
    alloc_threads[node].join();
    if (shared_node_memories_[node].is_null() || volatile_pools_[node].is_null()) {
      failed = true;
    }
  }

  if (failed) {
    deallocate_shared_memories();
    return ERROR_STACK(kErrorCodeSocShmAllocFailed);
  }

  return kRetOk;
}

ErrorStack SharedMemoryRepo::attach_shared_memories(uint64_t master_upid, EngineOptions* options) {
  std::string base = get_master_path(master_upid);
  std::string global_memory_path = base + std::string("_global");
  global_memory_.attach(global_memory_path);
  if (global_memory_.is_null()) {
    deallocate_shared_memories();
    return ERROR_STACK(kErrorCodeSocShmAttachFailed);
  }

  // read the options from global_memory
  uint64_t xml_size = 0;
  std::memcpy(&xml_size, global_memory_.get_block(), sizeof(xml_size));
  ASSERT_ND(xml_size > 0);
  std::string xml(global_memory_.get_block() + sizeof(xml_size), xml_size);
  CHECK_ERROR(options->load_from_string(xml));

  bool failed = false;
  for (uint16_t node = 0; node < options->thread_.group_count_; ++node) {
    shared_node_memories_[node].attach(base + std::string("_node_") + std::to_string(node));
    volatile_pools_[node].attach(base + std::string("_vpool_") + std::to_string(node));
    if (shared_node_memories_[node].is_null() || volatile_pools_[node].is_null()) {
      failed = true;
      break;
    }
  }

  if (failed) {
    deallocate_shared_memories();
    return ERROR_STACK(kErrorCodeSocShmAttachFailed);
  }
  return kRetOk;
}

void SharedMemoryRepo::mark_for_release() {
  // mark_for_release() is idempotent, so just do it on all of them
  global_memory_.mark_for_release();
  for (uint16_t i = 0; i < kMaxSocs; ++i) {
    shared_node_memories_[i].mark_for_release();
    volatile_pools_[i].mark_for_release();
  }
}
void SharedMemoryRepo::deallocate_shared_memories() {
  mark_for_release();
  // release_block() is idempotent, so just do it on all of them
  global_memory_.release_block();
  for (uint16_t i = 0; i < kMaxSocs; ++i) {
    shared_node_memories_[i].release_block();
    volatile_pools_[i].release_block();
  }
}

SharedMemoryRepo::SharedMemoryRepo(SharedMemoryRepo&& other) {
  *this = std::move(other);
}

SharedMemoryRepo& SharedMemoryRepo::operator=(SharedMemoryRepo&& other) {
  global_memory_ = std::move(other.global_memory_);
  for (uint16_t i = 0; i < kMaxSocs; ++i) {
    shared_node_memories_[i] = std::move(other.shared_node_memories_[i]);
    volatile_pools_[i] = std::move(other.volatile_pools_[i]);
  }
  return *this;
}

}  // namespace soc
}  // namespace foedus
