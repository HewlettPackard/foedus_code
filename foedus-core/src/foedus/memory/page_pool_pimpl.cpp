/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/memory/page_pool_pimpl.hpp"

#include <glog/logging.h>

#include <algorithm>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/memory/memory_options.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"

namespace foedus {
namespace memory {
PagePoolPimpl::PagePoolPimpl(
  uint64_t memory_byte_size,
  uint64_t memory_alignment,
  thread::ThreadGroupId numa_node)
    : memory_byte_size_(memory_byte_size / memory_alignment * memory_alignment),
    memory_alignment_(memory_alignment),
    numa_node_(numa_node) {}

ErrorStack PagePoolPimpl::initialize_once() {
  pool_base_ = nullptr;
  pool_size_= 0;
  free_pool_ = nullptr;
  free_pool_capacity_ = 0;
  free_pool_head_ = 0;
  free_pool_count_ = 0;

  LOG(INFO) << "Acquiring memory for Page Pool (" << memory_byte_size_ << " bytes) on NUMA node "
    << static_cast<int>(numa_node_)<< "...";
  ASSERT_ND(memory_byte_size_ % memory_alignment_ == 0);
  memory_.alloc(memory_byte_size_, memory_alignment_, AlignedMemory::kNumaAllocOnnode, numa_node_);
  pool_base_ = reinterpret_cast<storage::Page*>(memory_.get_block());
  pool_size_ = memory_.get_size() / storage::kPageSize;
  LOG(INFO) << "Acquired memory Page Pool. " << memory_ << ". pages=" << pool_size_;

  ASSERT_ND(memory_.get_size() % storage::kPageSize == 0);
  uint64_t total_pages = memory_.get_size() / storage::kPageSize;
  uint64_t pointers_total_size = total_pages * sizeof(PagePoolOffset);
  pages_for_free_pool_ = assorted::int_div_ceil(pointers_total_size, storage::kPageSize);
  LOG(INFO) << "total_pages=" << total_pages
    << ", pointers_total_size=" << pointers_total_size
    << ", pages_for_free_pool_=" << pages_for_free_pool_;

  LOG(INFO) << "Constructing circular free pool...";
  free_pool_ = reinterpret_cast<PagePoolOffset*>(memory_.get_block());
  free_pool_capacity_ = total_pages - pages_for_free_pool_;
  // all pages after pages_for_free_pool_-th page is in the free pool at first
  for (uint64_t i = 0; i < free_pool_capacity_; ++i) {
    free_pool_[i] = pages_for_free_pool_ + i;
  }
  free_pool_head_ = 0;
  free_pool_count_ = free_pool_capacity_;
  resolver_ = LocalPageResolver(pool_base_, pages_for_free_pool_, pool_size_);
  LOG(INFO) << "Constructed circular free pool.";

  return kRetOk;
}

ErrorStack PagePoolPimpl::uninitialize_once() {
  LOG(INFO) << "Releasing memory of Page Pool.";
  if (free_pool_count_ != free_pool_capacity_) {
    // This is not a memory leak as we anyway releases everything, but it's a smell of bug.
    LOG(WARNING) << "Page Pool has not received back all free pages by its uninitialization!!"
      << " count=" << free_pool_count_ << ", capacity=" << free_pool_capacity_;
  } else {
    LOG(INFO) << "Page Pool has received back all free pages. No suspicious behavior.";
  }
  memory_.release_block();
  free_pool_ = nullptr;
  pool_base_ = nullptr;
  LOG(INFO) << "Released memory. ";
  return kRetOk;
}

ErrorCode PagePoolPimpl::grab(uint32_t desired_grab_count, PagePoolOffsetChunk* chunk) {
  ASSERT_ND(is_initialized());
  ASSERT_ND(chunk->size() + desired_grab_count <= chunk->capacity());
  VLOG(0) << "Grabbing " << desired_grab_count << " pages."
    << " free_pool_count_=" << free_pool_count_;
  std::lock_guard<std::mutex> guard(lock_);
  if (free_pool_count_ == 0) {
    LOG(WARNING) << "No more free pages left in the pool";
    return kErrorCodeMemoryNoFreePages;
  }

  // grab from the head
  uint64_t grab_count = std::min<uint64_t>(desired_grab_count, free_pool_count_);
  PagePoolOffset* head = free_pool_ + free_pool_head_;
  if (free_pool_head_ + grab_count > free_pool_capacity_) {
    // wrap around
    uint64_t wrap_count = free_pool_capacity_ - free_pool_head_;
    chunk->push_back(head, head + wrap_count);
    free_pool_head_ = 0;
    free_pool_count_ -= wrap_count;
    grab_count -= wrap_count;
    head = free_pool_;
  }

  // no wrap around (or no more wrap around)
  ASSERT_ND(free_pool_head_ + grab_count <= free_pool_capacity_);
  chunk->push_back(head, head + grab_count);
  free_pool_head_ += grab_count;
  free_pool_count_ -= grab_count;
  return kErrorCodeOk;
}

ErrorCode PagePoolPimpl::grab_one(PagePoolOffset *offset) {
  ASSERT_ND(is_initialized());
  VLOG(0) << "Grabbing just one page. free_pool_count_=" << free_pool_count_;
  *offset = 0;
  std::lock_guard<std::mutex> guard(lock_);
  if (free_pool_count_ == 0) {
    LOG(WARNING) << "No more free pages left in the pool";
    return kErrorCodeMemoryNoFreePages;
  }

  // grab from the head
  PagePoolOffset* head = free_pool_ + free_pool_head_;
  if (free_pool_head_ == free_pool_capacity_) {
    // wrap around
    free_pool_head_ = 0;
    head = free_pool_;
  }

  // no wrap around (or no more wrap around)
  ASSERT_ND(free_pool_head_ + 1 <= free_pool_capacity_);
  *offset = *head;
  ++free_pool_head_;
  --free_pool_count_;
  return kErrorCodeOk;
}

void PagePoolPimpl::release(uint32_t desired_release_count, PagePoolOffsetChunk *chunk) {
  ASSERT_ND(is_initialized());
  ASSERT_ND(chunk->size() >= desired_release_count);
  VLOG(0) << "Releasing " << desired_release_count << " pages."
    << " free_pool_count_=" << free_pool_count_;
  std::lock_guard<std::mutex> guard(lock_);
  if (free_pool_count_ + desired_release_count > free_pool_capacity_) {
    // this can't happen unless something is wrong! This is a critical issue from which
    // we can't recover because page pool is inconsistent!
    LOG(ERROR) << "PagePoolPimpl::release() More than full free-pool. inconsistent state!";
    // TODO(Hideaki) Minor: Do a duplicate-check here to identify the problemetic pages.
    COERCE_ERROR(ERROR_STACK(kErrorCodeMemoryDuplicatePage));
  }

  // append to the tail
  uint64_t release_count = std::min<uint64_t>(desired_release_count, chunk->size());
  uint64_t tail = free_pool_head_ + free_pool_count_;
  if (tail >= free_pool_capacity_) {
    tail -= free_pool_capacity_;
  }
  if (tail + release_count > free_pool_capacity_) {
    // wrap around
    uint32_t wrap_count = free_pool_capacity_ - tail;
    chunk->move_to(free_pool_ + tail, wrap_count);
    free_pool_count_ += wrap_count;
    release_count -= wrap_count;
    tail = 0;
  }

  // no wrap around (or no more wrap around)
  ASSERT_ND(tail + release_count <= free_pool_capacity_);
  chunk->move_to(free_pool_ + tail, release_count);
  free_pool_count_ += release_count;
}

void PagePoolPimpl::release_one(PagePoolOffset offset) {
  ASSERT_ND(is_initialized());
  VLOG(0) << "Releasing just one page. free_pool_count_=" << free_pool_count_;
  std::lock_guard<std::mutex> guard(lock_);
  if (free_pool_count_ >= free_pool_capacity_) {
    // this can't happen unless something is wrong! This is a critical issue from which
    // we can't recover because page pool is inconsistent!
    LOG(ERROR) << "PagePoolPimpl::release_one() More than full free-pool. inconsistent state!";
    COERCE_ERROR(ERROR_STACK(kErrorCodeMemoryDuplicatePage));
  }

  // append to the tail
  uint64_t tail = free_pool_head_ + free_pool_count_;
  if (tail >= free_pool_capacity_) {
    tail -= free_pool_capacity_;
  }
  if (tail == free_pool_capacity_) {
    // wrap around
    tail = 0;
  }

  // no wrap around (or no more wrap around)
  ASSERT_ND(tail + 1 <= free_pool_capacity_);
  free_pool_[tail] = offset;
  ++free_pool_count_;
}


std::ostream& operator<<(std::ostream& o, const PagePoolPimpl& v) {
  o << "<PagePool>"
    << "<memory_>" << v.memory_ << "</memory_>"
    << "<numa_node_>" << v.numa_node_ << "</numa_node_>"
    << "<pages_for_free_pool_>" << v.pages_for_free_pool_ << "</pages_for_free_pool_>"
    << "<free_pool_capacity_>" << v.free_pool_capacity_ << "</free_pool_capacity_>"
    << "<free_pool_head_>" << v.free_pool_head_ << "</free_pool_head_>"
    << "<free_pool_count_>" << v.free_pool_count_ << "</free_pool_count_>"
    << "</PagePool>";
  return o;
}

}  // namespace memory
}  // namespace foedus
