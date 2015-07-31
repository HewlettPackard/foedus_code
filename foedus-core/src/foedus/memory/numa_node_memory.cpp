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
#include "foedus/memory/numa_node_memory.hpp"

#include <numa.h>
#include <glog/logging.h>

#include <iostream>
#include <sstream>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/cache/cache_hashtable.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/thread/thread_options.hpp"

namespace foedus {
namespace memory {
NumaNodeMemory::NumaNodeMemory(Engine* engine, thread::ThreadGroupId numa_node)
  : engine_(engine),
    numa_node_(numa_node),
    cores_(engine_->get_options().thread_.thread_count_per_group_),
    loggers_(engine_->get_options().log_.loggers_per_node_),
    snapshot_cache_table_(nullptr) {
}

int64_t get_numa_node_size(int node) {
  if (::numa_available() < 0) {
    return 0;
  } else {
    return ::numa_node_size(node, nullptr);
  }
}

ErrorStack NumaNodeMemory::initialize_once() {
  LOG(INFO) << "Initializing NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
    << " BEFORE: numa_node_size=" << get_numa_node_size(numa_node_);


  // volatile pool is placed on the shared memory
  soc::SharedMemoryRepo* memory_repo = engine_->get_soc_manager()->get_shared_memory_repo();
  uint64_t volatile_size =
    static_cast<uint64_t>(engine_->get_options().memory_.page_pool_size_mb_per_node_) << 20;
  volatile_pool_.attach(
    memory_repo->get_node_memory_anchors(numa_node_)->volatile_pool_status_,
    memory_repo->get_volatile_pool(numa_node_),
    volatile_size,
    true,
    engine_->get_options().memory_.rigorous_page_boundary_check_);
  volatile_pool_.set_debug_pool_name(
    std::string("VolatilePool-")
    + std::to_string(static_cast<int>(numa_node_)));

  // snapshot pool is SOC-local
  uint64_t snapshot_pool_bytes
    = static_cast<uint64_t>(engine_->get_options().cache_.snapshot_cache_size_mb_per_node_) << 20;
  if (engine_->get_options().memory_.rigorous_page_boundary_check_) {
    // mprotect raises EINVAL if the underlying pages are hugepages.
    LOG(INFO) << "rigorous_page_boundary_check_ is specified, so disabled hugepages.";
    allocate_numa_memory(snapshot_pool_bytes, &snapshot_pool_memory_);
  } else {
    allocate_huge_numa_memory(snapshot_pool_bytes, &snapshot_pool_memory_);
  }
  snapshot_pool_control_block_.alloc(1 << 12, 1 << 12, AlignedMemory::kNumaAllocOnnode, numa_node_);
  snapshot_pool_.attach(
    reinterpret_cast<PagePoolControlBlock*>(snapshot_pool_control_block_.get_block()),
    snapshot_pool_memory_.get_block(),
    snapshot_pool_memory_.get_size(),
    true,
    engine_->get_options().memory_.rigorous_page_boundary_check_);
  snapshot_pool_.set_debug_pool_name(
    std::string("SnapshotPool-")
    + std::to_string(static_cast<int>(numa_node_)));

  CHECK_ERROR(volatile_pool_.initialize());
  CHECK_ERROR(snapshot_pool_.initialize());

  // snapshot_pool_ consumes #pages * 4kb bytes of memory.
  // CacheBucket is 16 bytes, so even with 32-fold (3% full hashtable), we spend only
  // #pages * 0.5kb for hash buckets. This is a neligible overhead.
  uint64_t cache_hashtable_buckets = (snapshot_pool_.get_memory_size() / storage::kPageSize) * 32;
  snapshot_cache_table_ = new cache::CacheHashtable(cache_hashtable_buckets, numa_node_);
  CHECK_ERROR(initialize_page_offset_chunk_memory());
  CHECK_ERROR(initialize_log_buffers_memory());
  for (auto ordinal = 0; ordinal < cores_; ++ordinal) {
    CHECK_ERROR(initialize_core_memory(ordinal));
  }
  ASSERT_ND(volatile_pool_.is_initialized());
  ASSERT_ND(snapshot_pool_.is_initialized());
  ASSERT_ND(core_memories_.size() == cores_);
  ASSERT_ND(volatile_offset_chunk_memory_pieces_.size() == cores_);
  ASSERT_ND(snapshot_offset_chunk_memory_pieces_.size() == cores_);
  ASSERT_ND(log_buffer_memory_pieces_.size() == cores_);

  LOG(INFO) << "Initialized NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
    << " AFTER: numa_node_size=" << get_numa_node_size(numa_node_);
  return kRetOk;
}
ErrorStack NumaNodeMemory::initialize_page_offset_chunk_memory() {
  size_t size_per_core = sizeof(PagePoolOffsetChunk) * 2;
  size_t total_size = size_per_core * cores_;
  LOG(INFO) << "Initializing page_offset_chunk_memory_. total_size=" << total_size << " bytes";
  if (total_size < kHugepageSize) {
    // Just one per NUMA node. Not a significant waste.
    total_size = kHugepageSize;
    LOG(INFO) << "Allocating extra space to utilize hugepage.";
  }
  CHECK_ERROR(allocate_huge_numa_memory(total_size, &volatile_offset_chunk_memory_));
  CHECK_ERROR(allocate_huge_numa_memory(total_size, &snapshot_offset_chunk_memory_));
  for (auto ordinal = 0; ordinal < cores_; ++ordinal) {
    {
      PagePoolOffsetChunk* chunk = reinterpret_cast<PagePoolOffsetChunk*>(
        volatile_offset_chunk_memory_.get_block()) + ordinal;
      chunk->clear();
      volatile_offset_chunk_memory_pieces_.push_back(chunk);
    }
    {
      PagePoolOffsetChunk* chunk = reinterpret_cast<PagePoolOffsetChunk*>(
        snapshot_offset_chunk_memory_.get_block()) + ordinal;
      chunk->clear();
      snapshot_offset_chunk_memory_pieces_.push_back(chunk);
    }
  }

  return kRetOk;
}

ErrorStack NumaNodeMemory::initialize_log_buffers_memory() {
  uint64_t size_per_core_ = static_cast<uint64_t>(engine_->get_options().log_.log_buffer_kb_) << 10;
  uint64_t private_total = (cores_ * size_per_core_);
  LOG(INFO) << "Initializing log_buffer_memory_. total_size=" << private_total;
  CHECK_ERROR(allocate_huge_numa_memory(private_total, &log_buffer_memory_));
  LOG(INFO) << "log_buffer_memory_ allocated. addr=" << log_buffer_memory_.get_block();
  for (auto ordinal = 0; ordinal < cores_; ++ordinal) {
    AlignedMemorySlice piece(&log_buffer_memory_, size_per_core_ * ordinal, size_per_core_);
    LOG(INFO) << "log_buffer_piece[" << ordinal << "] addr=" << piece.get_block();
    log_buffer_memory_pieces_.push_back(piece);
  }

  return kRetOk;
}


ErrorStack NumaNodeMemory::initialize_core_memory(thread::ThreadLocalOrdinal ordinal) {
  auto core_id = thread::compose_thread_id(numa_node_, ordinal);
  NumaCoreMemory* core_memory = new NumaCoreMemory(engine_, this, core_id);
  core_memories_.push_back(core_memory);
  CHECK_ERROR(core_memory->initialize());
  return kRetOk;
}


ErrorStack NumaNodeMemory::uninitialize_once() {
  LOG(INFO) << "Uninitializing NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
    << " BEFORE: numa_node_size=" << get_numa_node_size(numa_node_);

  ErrorStackBatch batch;
  batch.uninitialize_and_delete_all(&core_memories_);
  volatile_offset_chunk_memory_pieces_.clear();
  volatile_offset_chunk_memory_.release_block();
  snapshot_offset_chunk_memory_pieces_.clear();
  snapshot_offset_chunk_memory_.release_block();
  log_buffer_memory_pieces_.clear();
  log_buffer_memory_.release_block();
  if (snapshot_cache_table_) {
    delete snapshot_cache_table_;
    snapshot_cache_table_ = nullptr;
  }
  batch.emprace_back(volatile_pool_.uninitialize());
  batch.emprace_back(snapshot_pool_.uninitialize());
  snapshot_pool_memory_.release_block();
  snapshot_pool_control_block_.release_block();

  LOG(INFO) << "Uninitialized NumaNodeMemory for node " << static_cast<int>(numa_node_) << "."
    << " AFTER: numa_node_size=" << get_numa_node_size(numa_node_);
  return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack NumaNodeMemory::allocate_numa_memory_general(
  uint64_t size,
  uint64_t alignment,
  AlignedMemory *out) const {
  ASSERT_ND(out);
  if (engine_->get_options().memory_.use_mmap_hugepages_ &&
    alignment >= kHugepageSize
    && size >= (1ULL << 30) * 8 / 10) {
    LOG(INFO) << "This is a big memory allocation. Let's use the mmap hugepage (1GB pages)";
    out->alloc(size, 1ULL << 30, AlignedMemory::kNumaMmapOneGbPages, numa_node_);
  } else {
    out->alloc(size, alignment, AlignedMemory::kNumaAllocOnnode, numa_node_);
  }
  if (out->is_null()) {
    return ERROR_STACK(kErrorCodeOutofmemory);
  }
  return kRetOk;
}

std::string NumaNodeMemory::dump_free_memory_stat() const {
  std::stringstream ret;
  PagePool::Stat volatile_stat = volatile_pool_.get_stat();
  ret << "    Volatile-Pool: " << volatile_stat.allocated_pages_ << " allocated pages, "
    << volatile_stat.total_pages_ << " total pages, "
    << (volatile_stat.total_pages_ - volatile_stat.allocated_pages_) << " free pages"
    << std::endl;
  PagePool::Stat snapshot_stat = snapshot_pool_.get_stat();
  ret << "    Snapshot-Pool: " << snapshot_stat.allocated_pages_ << " allocated pages, "
    << snapshot_stat.total_pages_ << " total pages, "
    << (snapshot_stat.total_pages_ - snapshot_stat.allocated_pages_) << " free pages"
    << std::endl;
  return ret.str();
}

NumaNodeMemoryRef::NumaNodeMemoryRef(Engine* engine, thread::ThreadGroupId numa_node)
  : engine_(engine), numa_node_(numa_node) {
  soc::SharedMemoryRepo* memory_repo = engine->get_soc_manager()->get_shared_memory_repo();
  volatile_pool_.attach(
    memory_repo->get_node_memory_anchors(numa_node)->volatile_pool_status_,
    memory_repo->get_volatile_pool(numa_node),
    static_cast<uint64_t>(engine->get_options().memory_.page_pool_size_mb_per_node_) << 20,
    false,
    false);
}

std::string NumaNodeMemoryRef::dump_free_memory_stat() const {
  std::stringstream ret;
  PagePool::Stat volatile_stat = volatile_pool_.get_stat();
  ret << "    Volatile-Pool: " << volatile_stat.allocated_pages_ << " allocated pages, "
    << volatile_stat.total_pages_ << " total pages, "
    << (volatile_stat.total_pages_ - volatile_stat.allocated_pages_) << " free pages"
    << std::endl;
  return ret.str();
}

}  // namespace memory
}  // namespace foedus
