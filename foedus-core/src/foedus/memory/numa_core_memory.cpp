/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/compiler.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <foedus/memory/numa_node_memory.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/xct/xct_id.hpp>
#include <foedus/xct/xct_access.hpp>
#include <foedus/xct/xct_options.hpp>
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <glog/logging.h>
namespace foedus {
namespace memory {
NumaCoreMemory::NumaCoreMemory(Engine* engine, NumaNodeMemory *node_memory,
      thread::ThreadId core_id) : engine_(engine), node_memory_(node_memory),
    core_id_(core_id), core_local_ordinal_(thread::decompose_numa_local_ordinal(core_id)),
    read_set_memory_(nullptr), write_set_memory_(nullptr) {
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
  free_pool_chunk_ = node_memory_->get_page_offset_chunk_memory_piece(core_local_ordinal_);
  log_buffer_memory_ = node_memory_->get_log_buffer_memory_piece(core_local_ordinal_);

  // Each core starts from 50%-full free pool chunk (configurable)
  uint32_t initial_pages = engine_->get_options().memory_.private_page_pool_initial_grab_;
  WRAP_ERROR_CODE(node_memory_->get_page_pool().grab(initial_pages, free_pool_chunk_));
  return kRetOk;
}
ErrorStack NumaCoreMemory::uninitialize_once() {
  LOG(INFO) << "Releasing NumaCoreMemory for core " << core_id_;
  ErrorStackBatch batch;
  read_set_memory_ = nullptr;
  write_set_memory_ = nullptr;
  if (free_pool_chunk_) {
    // return all free pages
    node_memory_->get_page_pool().release(free_pool_chunk_->size(), free_pool_chunk_);
    free_pool_chunk_ = nullptr;
  }
  log_buffer_memory_.clear();
  return SUMMARIZE_ERROR_BATCH(batch);
}

PagePoolOffset NumaCoreMemory::grab_free_page() {
  if (UNLIKELY(free_pool_chunk_->empty())) {
    if (grab_free_pages_from_node() != kErrorCodeOk) {
      return 0;
    }
  }
  ASSERT_ND(!free_pool_chunk_->empty());
  return free_pool_chunk_->pop_back();
}
void NumaCoreMemory::release_free_page(PagePoolOffset offset) {
  if (UNLIKELY(free_pool_chunk_->full())) {
    release_free_pages_to_node();
  }
  ASSERT_ND(!free_pool_chunk_->full());
  free_pool_chunk_->push_back(offset);
}

ErrorCode NumaCoreMemory::grab_free_pages_from_node() {
  uint32_t desired = (free_pool_chunk_->capacity() - free_pool_chunk_->size()) / 2;
  return node_memory_->get_page_pool().grab(desired, free_pool_chunk_);
}

void NumaCoreMemory::release_free_pages_to_node() {
  uint32_t desired = free_pool_chunk_->size() / 2;
  node_memory_->get_page_pool().release(desired, free_pool_chunk_);
}

}  // namespace memory
}  // namespace foedus
