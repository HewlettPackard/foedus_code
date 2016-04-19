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
#include "foedus/memory/numa_core_memory.hpp"

#include <glog/logging.h>

#include <algorithm>

#include "foedus/compiler.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/thread/thread_pimpl.hpp"
#include "foedus/xct/retrospective_lock_list.hpp"
#include "foedus/xct/sysxct_impl.hpp"
#include "foedus/xct/xct_access.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/xct/xct_options.hpp"

namespace foedus {
namespace memory {
NumaCoreMemory::NumaCoreMemory(
  Engine* engine,
  NumaNodeMemory *node_memory,
  thread::ThreadId core_id)
  : engine_(engine),
    node_memory_(node_memory),
    core_id_(core_id),
    numa_node_(thread::decompose_numa_node(core_id)),
    core_local_ordinal_(thread::decompose_numa_local_ordinal(core_id)),
    free_volatile_pool_chunk_(nullptr),
    free_snapshot_pool_chunk_(nullptr),
    retired_volatile_pool_chunks_(nullptr),
    current_lock_list_memory_(nullptr),
    current_lock_list_capacity_(0),
    retrospective_lock_list_memory_(nullptr),
    retrospective_lock_list_capacity_(0),
    volatile_pool_(nullptr),
    snapshot_pool_(nullptr) {
  ASSERT_ND(numa_node_ == node_memory->get_numa_node());
  ASSERT_ND(core_id_ == thread::compose_thread_id(node_memory->get_numa_node(),
                          core_local_ordinal_));
}

uint64_t NumaCoreMemory::calculate_local_small_memory_size(const EngineOptions& options) {
  uint64_t memory_size = 0;
  // for the "shift" part, we calculate conservatively then skip it at the end.
  // it's a wasted memory, but negligible.
  memory_size += static_cast<uint64_t>(options.thread_.thread_count_per_group_) << 12;
  memory_size += sizeof(xct::SysxctWorkspace);
  memory_size += sizeof(xct::PageVersionAccess) * xct::Xct::kMaxPageVersionSets;
  memory_size += sizeof(xct::PointerAccess) * xct::Xct::kMaxPointerSets;
  const xct::XctOptions& xct_opt = options.xct_;
  const uint16_t nodes = options.thread_.group_count_;
  memory_size += sizeof(xct::ReadXctAccess) * xct_opt.max_read_set_size_;
  memory_size += sizeof(xct::WriteXctAccess) * xct_opt.max_write_set_size_;
  memory_size += sizeof(xct::LockFreeReadXctAccess)
    * xct_opt.max_lock_free_read_set_size_;
  memory_size += sizeof(xct::LockFreeWriteXctAccess)
    * xct_opt.max_lock_free_write_set_size_;
  memory_size += sizeof(memory::PagePoolOffsetAndEpochChunk) * nodes;

  // In reality almost no chance we take as many locks as all read/write-sets,
  // but let's simplify that. Not much memory anyways.
  const uint64_t total_access_sets = xct_opt.max_read_set_size_ + xct_opt.max_write_set_size_;
  memory_size += sizeof(xct::LockEntry) * total_access_sets;
  memory_size += sizeof(xct::LockEntry) * total_access_sets;
  return memory_size;
}

ErrorStack NumaCoreMemory::initialize_once() {
  LOG(INFO) << "Initializing NumaCoreMemory for core " << core_id_;
  free_volatile_pool_chunk_ = node_memory_->get_volatile_offset_chunk_memory_piece(
    core_local_ordinal_);
  free_snapshot_pool_chunk_ = node_memory_->get_snapshot_offset_chunk_memory_piece(
    core_local_ordinal_);
  volatile_pool_ = node_memory_->get_volatile_pool();
  snapshot_pool_ = node_memory_->get_snapshot_pool();
  log_buffer_memory_ = node_memory_->get_log_buffer_memory_piece(core_local_ordinal_);

  // allocate small_thread_local_memory_. it's a collection of small memories
  uint64_t memory_size = calculate_local_small_memory_size(engine_->get_options());
  if (memory_size > (1U << 21)) {
    VLOG(1) << "mm, small_local_memory_size is more than 2MB(" << memory_size << ")."
      " not a big issue, but consumes one more TLB entry...";
  }
  CHECK_ERROR(node_memory_->allocate_numa_memory(memory_size, &small_thread_local_memory_));

  const xct::XctOptions& xct_opt = engine_->get_options().xct_;
  const uint16_t nodes = engine_->get_options().thread_.group_count_;
  const uint16_t thread_per_group = engine_->get_options().thread_.thread_count_per_group_;
  char* memory = reinterpret_cast<char*>(small_thread_local_memory_.get_block());
  // "shift" 4kb for each thread on this node so that memory banks are evenly used.
  // in many architecture, 13th- or 14th- bits are memory banks (see [JEONG11])
  memory += static_cast<uint64_t>(core_local_ordinal_) << 12;
  small_thread_local_memory_pieces_.sysxct_workspace_memory_ = memory;
  memory += sizeof(xct::SysxctWorkspace);
  small_thread_local_memory_pieces_.xct_page_version_memory_ = memory;
  memory += sizeof(xct::PageVersionAccess) * xct::Xct::kMaxPageVersionSets;
  small_thread_local_memory_pieces_.xct_pointer_access_memory_ = memory;
  memory += sizeof(xct::PointerAccess) * xct::Xct::kMaxPointerSets;
  small_thread_local_memory_pieces_.xct_read_access_memory_ = memory;
  memory += sizeof(xct::ReadXctAccess) * xct_opt.max_read_set_size_;
  small_thread_local_memory_pieces_.xct_write_access_memory_ = memory;
  memory += sizeof(xct::WriteXctAccess) * xct_opt.max_write_set_size_;
  small_thread_local_memory_pieces_.xct_lock_free_read_access_memory_ = memory;
  memory += sizeof(xct::LockFreeReadXctAccess) * xct_opt.max_lock_free_read_set_size_;
  small_thread_local_memory_pieces_.xct_lock_free_write_access_memory_ = memory;
  memory += sizeof(xct::LockFreeWriteXctAccess) * xct_opt.max_lock_free_write_set_size_;
  retired_volatile_pool_chunks_ = reinterpret_cast<PagePoolOffsetAndEpochChunk*>(memory);
  memory += sizeof(memory::PagePoolOffsetAndEpochChunk) * nodes;

  const uint64_t total_access_sets = xct_opt.max_read_set_size_ + xct_opt.max_write_set_size_;
  current_lock_list_memory_ = reinterpret_cast<xct::LockEntry*>(memory);
  current_lock_list_capacity_ = total_access_sets;
  memory += sizeof(xct::LockEntry) * total_access_sets;
  retrospective_lock_list_memory_ = reinterpret_cast<xct::LockEntry*>(memory);
  retrospective_lock_list_capacity_ = total_access_sets;
  memory += sizeof(xct::LockEntry) * total_access_sets;

  memory += static_cast<uint64_t>(thread_per_group - core_local_ordinal_) << 12;
  ASSERT_ND(reinterpret_cast<char*>(small_thread_local_memory_.get_block())
    + memory_size == memory);

  for (uint16_t node = 0; node < nodes; ++node) {
    retired_volatile_pool_chunks_[node].clear();
  }

  CHECK_ERROR(node_memory_->allocate_numa_memory(
    xct_opt.local_work_memory_size_mb_ * (1ULL << 20),
    &local_work_memory_));

  // Each core starts from 50%-full free pool chunk (configurable)
  uint32_t initial_pages = engine_->get_options().memory_.private_page_pool_initial_grab_;
  {
    uint32_t grab_count = std::min<uint32_t>(
      volatile_pool_->get_recommended_pages_per_grab(),
      std::min<uint32_t>(
        initial_pages,
        volatile_pool_->get_free_pool_capacity() / (2U * thread_per_group)));
    WRAP_ERROR_CODE(volatile_pool_->grab(grab_count, free_volatile_pool_chunk_));
  }
  {
    uint32_t grab_count = std::min<uint32_t>(
      snapshot_pool_->get_recommended_pages_per_grab(),
      std::min<uint32_t>(
        initial_pages,
        snapshot_pool_->get_free_pool_capacity() / (2U * thread_per_group)));
    WRAP_ERROR_CODE(snapshot_pool_->grab(grab_count, free_snapshot_pool_chunk_));
  }
  return kRetOk;
}
ErrorStack NumaCoreMemory::uninitialize_once() {
  LOG(INFO) << "Releasing NumaCoreMemory for core " << core_id_;
  ErrorStackBatch batch;
  // return all free pages
  if (retired_volatile_pool_chunks_) {
    // this should be already released in ThreadPimpl's uninitialize.
    // we can't do it here because uninitialization of node/core memories are parallelized
    for (uint16_t node = 0; node < engine_->get_soc_count(); ++node) {
      PagePoolOffsetAndEpochChunk* chunk = retired_volatile_pool_chunks_ + node;
      ASSERT_ND(chunk->empty());  // just sanity check
    }
    retired_volatile_pool_chunks_ = nullptr;
  }
  if (free_volatile_pool_chunk_) {
    volatile_pool_->release(free_volatile_pool_chunk_->size(), free_volatile_pool_chunk_);
    free_volatile_pool_chunk_ = nullptr;
    volatile_pool_ = nullptr;
  }
  if (free_snapshot_pool_chunk_) {
    snapshot_pool_->release(free_snapshot_pool_chunk_->size(), free_snapshot_pool_chunk_);
    free_snapshot_pool_chunk_ = nullptr;
    snapshot_pool_ = nullptr;
  }
  log_buffer_memory_.clear();
  local_work_memory_.release_block();
  small_thread_local_memory_.release_block();
  return SUMMARIZE_ERROR_BATCH(batch);
}

PagePoolOffset NumaCoreMemory::grab_free_volatile_page() {
  if (UNLIKELY(free_volatile_pool_chunk_->empty())) {
    if (grab_free_pages_from_node(free_volatile_pool_chunk_, volatile_pool_) != kErrorCodeOk) {
      return 0;
    }
  }
  ASSERT_ND(!free_volatile_pool_chunk_->empty());
  return free_volatile_pool_chunk_->pop_back();
}
storage::VolatilePagePointer NumaCoreMemory::grab_free_volatile_page_pointer() {
  storage::VolatilePagePointer ret;
  ret.set(numa_node_, grab_free_volatile_page());
  return ret;
}
void NumaCoreMemory::release_free_volatile_page(PagePoolOffset offset) {
  if (UNLIKELY(free_volatile_pool_chunk_->full())) {
    release_free_pages_to_node(free_volatile_pool_chunk_, volatile_pool_);
  }
  ASSERT_ND(!free_volatile_pool_chunk_->full());
  free_volatile_pool_chunk_->push_back(offset);
}

PagePoolOffset NumaCoreMemory::grab_free_snapshot_page() {
  if (UNLIKELY(free_snapshot_pool_chunk_->empty())) {
    if (grab_free_pages_from_node(free_snapshot_pool_chunk_, snapshot_pool_) != kErrorCodeOk) {
      return 0;
    }
  }
  ASSERT_ND(!free_snapshot_pool_chunk_->empty());
  return free_snapshot_pool_chunk_->pop_back();
}
void NumaCoreMemory::release_free_snapshot_page(PagePoolOffset offset) {
  if (UNLIKELY(free_snapshot_pool_chunk_->full())) {
    release_free_pages_to_node(free_snapshot_pool_chunk_, snapshot_pool_);
  }
  ASSERT_ND(!free_snapshot_pool_chunk_->full());
  free_snapshot_pool_chunk_->push_back(offset);
}

ErrorCode NumaCoreMemory::grab_free_pages_from_node(
  PagePoolOffsetChunk* free_chunk,
  memory::PagePool* pool) {
  uint32_t desired = (free_chunk->capacity() - free_chunk->size()) / 2;
  desired = std::min<uint32_t>(desired, pool->get_recommended_pages_per_grab());
  return pool->grab(desired, free_chunk);
}

void NumaCoreMemory::release_free_pages_to_node(
  PagePoolOffsetChunk* free_chunk,
  memory::PagePool *pool) {
  uint32_t desired = free_chunk->size() / 2;
  pool->release(desired, free_chunk);
}

PagePoolOffsetAndEpochChunk* NumaCoreMemory::get_retired_volatile_pool_chunk(uint16_t node) {
  return retired_volatile_pool_chunks_ + node;
}

}  // namespace memory
}  // namespace foedus
