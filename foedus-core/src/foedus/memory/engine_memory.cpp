/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace memory {
ErrorStack EngineMemory::initialize_once() {
  LOG(INFO) << "Initializing EngineMemory..";
  if (!engine_->get_debug()->is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  } else if (::numa_available() < 0) {
    LOG(WARNING) << "WARNING, this machine is not a NUMA machine. FOEDUS still works fine,"
      << " but it is mainly designed for large servers with many sockets and cores";
    // Even if the kernel is built without NUMA (eg ARMv8), we keep running.
    // return ERROR_STACK(kErrorCodeMemoryNumaUnavailable);
  }
  ASSERT_ND(node_memories_.empty());
  const EngineOptions& options = engine_->get_options();
  thread::ThreadGroupId numa_nodes = options.thread_.group_count_;
  GlobalVolatilePageResolver::Base bases[256];
  uint64_t pool_begin = 0, pool_end = 0;
  for (thread::ThreadGroupId node = 0; node < numa_nodes; ++node) {
    NumaNodeMemoryRef* ref = new NumaNodeMemoryRef(engine_, node);
    node_memories_.push_back(ref);
    bases[node] = ref->get_volatile_pool()->get_base();
    pool_begin = ref->get_volatile_pool()->get_resolver().begin_;
    pool_end = ref->get_volatile_pool()->get_resolver().end_;
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
  if (!engine_->get_debug()->is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  for (auto* ref : node_memories_) {
    delete ref;
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

ErrorStack EngineMemory::grab_one_volatile_page(
  thread::ThreadGroupId node,
  storage::VolatilePagePointer* pointer,
  storage::Page** page) {
  PagePool* pool = get_node_memory(node)->get_volatile_pool();
  PagePoolOffset offset;
  WRAP_ERROR_CODE(pool->grab_one(&offset));
  *page = pool->get_resolver().resolve_offset_newpage(offset);
  pointer->components.numa_node = node;
  pointer->components.flags = 0;
  pointer->components.mod_count = 0;
  pointer->components.offset = offset;
  return kRetOk;
}

ErrorStack EngineMemory::load_one_volatile_page(
  cache::SnapshotFileSet* fileset,
  storage::SnapshotPagePointer snapshot_pointer,
  storage::VolatilePagePointer* pointer,
  storage::Page** page) {
  ASSERT_ND(snapshot_pointer != 0);
  thread::ThreadGroupId node = storage::extract_numa_node_from_snapshot_pointer(snapshot_pointer);
  CHECK_ERROR(grab_one_volatile_page(node, pointer, page));
  WRAP_ERROR_CODE(fileset->read_page(snapshot_pointer, *page));
  (*page)->get_header().snapshot_ = false;
  (*page)->get_header().page_id_ = pointer->word;
  return kRetOk;
}

}  // namespace memory
}  // namespace foedus
