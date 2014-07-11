/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/memory/numa_core_memory.hpp"

#include <glog/logging.h>

#include "foedus/compiler.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
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
    core_local_ordinal_(thread::decompose_numa_local_ordinal(core_id)),
    read_set_memory_(nullptr),
    write_set_memory_(nullptr),
    lock_free_write_set_memory_(nullptr),
    free_volatile_pool_chunk_(nullptr),
    free_snapshot_pool_chunk_(nullptr),
    volatile_pool_(nullptr),
    snapshot_pool_(nullptr){
  ASSERT_ND(thread::decompose_numa_node(core_id_) == node_memory->get_numa_node());
  ASSERT_ND(core_id_ == thread::compose_thread_id(node_memory->get_numa_node(),
                          core_local_ordinal_));
}

ErrorStack NumaCoreMemory::initialize_once() {
  LOG(INFO) << "Initializing NumaCoreMemory for core " << core_id_;
  read_set_memory_ = node_memory_->get_read_set_memory_piece(core_local_ordinal_);
  read_set_size_ = engine_->get_options().xct_.max_read_set_size_;
  write_set_memory_ = node_memory_->get_write_set_memory_piece(core_local_ordinal_);
  write_set_size_ = engine_->get_options().xct_.max_write_set_size_;
  lock_free_write_set_memory_ = node_memory_->get_lock_free_write_set_memory_piece(
    core_local_ordinal_);
  lock_free_write_set_size_ = engine_->get_options().xct_.max_lock_free_write_set_size_;
  free_volatile_pool_chunk_ = node_memory_->get_volatile_offset_chunk_memory_piece(
    core_local_ordinal_);
  free_snapshot_pool_chunk_ = node_memory_->get_snapshot_offset_chunk_memory_piece(
    core_local_ordinal_);
  volatile_pool_ = &node_memory_->get_volatile_pool();
  snapshot_pool_ = &node_memory_->get_snapshot_pool();
  log_buffer_memory_ = node_memory_->get_log_buffer_memory_piece(core_local_ordinal_);

  // Each core starts from 50%-full free pool chunk (configurable)
  uint32_t initial_pages = engine_->get_options().memory_.private_page_pool_initial_grab_;
  WRAP_ERROR_CODE(volatile_pool_->grab(initial_pages, free_volatile_pool_chunk_));
  WRAP_ERROR_CODE(snapshot_pool_->grab(initial_pages, free_snapshot_pool_chunk_));
  return kRetOk;
}
ErrorStack NumaCoreMemory::uninitialize_once() {
  LOG(INFO) << "Releasing NumaCoreMemory for core " << core_id_;
  ErrorStackBatch batch;
  read_set_memory_ = nullptr;
  write_set_memory_ = nullptr;
  lock_free_write_set_memory_ = nullptr;
  // return all free pages
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
  memory::PagePool *pool) {
  uint32_t desired = (free_chunk->capacity() - free_chunk->size()) / 2;
  return pool->grab(desired, free_chunk);
}

void NumaCoreMemory::release_free_pages_to_node(
  PagePoolOffsetChunk* free_chunk,
  memory::PagePool *pool) {
  uint32_t desired = free_chunk->size() / 2;
  pool->release(desired, free_chunk);
}

}  // namespace memory
}  // namespace foedus
