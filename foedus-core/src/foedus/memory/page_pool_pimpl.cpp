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
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/memory/memory_options.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"

namespace foedus {
namespace memory {
PagePoolPimpl::PagePoolPimpl()
  : control_block_(nullptr), memory_(nullptr), memory_size_(0), owns_(false) {}

void PagePoolPimpl::attach(
  PagePoolControlBlock* control_block,
  void* memory,
  uint64_t memory_size,
  bool owns) {
  control_block_ = control_block;
  memory_ = memory;
  memory_size_ = memory_size;
  owns_ = owns;
  pool_base_ = reinterpret_cast<storage::Page*>(memory_);
  pool_size_ = memory_size_ / storage::kPageSize;

  uint64_t pointers_total_size = pool_size_ * sizeof(PagePoolOffset);
  pages_for_free_pool_ = assorted::int_div_ceil(pointers_total_size, storage::kPageSize);

  free_pool_ = reinterpret_cast<PagePoolOffset*>(memory_);
  free_pool_capacity_ = pool_size_ - pages_for_free_pool_;
  resolver_ = LocalPageResolver(pool_base_, pages_for_free_pool_, pool_size_);
}

ErrorStack PagePoolPimpl::initialize_once() {
  if (owns_) {
    LOG(INFO) << "total_pages=" << pool_size_ << ", pages_for_free_pool_=" << pages_for_free_pool_;
    control_block_->initialize();
    LOG(INFO) << "Constructing circular free pool...";
    // all pages after pages_for_free_pool_-th page is in the free pool at first
    for (uint64_t i = 0; i < free_pool_capacity_; ++i) {
      free_pool_[i] = pages_for_free_pool_ + i;
    }

    // [experimental] randomize the free pool pointers so that we can evenly utilize all
    // memory banks
    if (false) {  // disabled for now
      // this should be an option...
      LOG(INFO) << "Randomizing free pool...";
      struct Randomizer {
        PagePoolOffset  offset_;
        uint32_t        rank_;
        static bool compare(const Randomizer& left, const Randomizer& right) {
          return left.rank_ < right.rank_;
        }
      };
      Randomizer* randomizers = new Randomizer[free_pool_capacity_];
      assorted::UniformRandom rnd(123456L);
      for (uint64_t i = 0; i < free_pool_capacity_; ++i) {
        randomizers[i].offset_ = pages_for_free_pool_ + i;
        randomizers[i].rank_ = rnd.next_uint32();
      }
      std::sort(randomizers, randomizers + free_pool_capacity_, Randomizer::compare);
      for (uint64_t i = 0; i < free_pool_capacity_; ++i) {
        free_pool_[i] = randomizers[i].offset_;
      }
      delete[] randomizers;
      LOG(INFO) << "Randomized free pool.";
    }

    control_block_->free_pool_head_ = 0;
    control_block_->free_pool_count_ = free_pool_capacity_;
    LOG(INFO) << "Constructed circular free pool.";
  }

  return kRetOk;
}

ErrorStack PagePoolPimpl::uninitialize_once() {
  if (owns_) {
    if (free_pool_count() != free_pool_capacity_) {
      // This is not a memory leak as we anyway releases everything, but it's a smell of bug.
      LOG(WARNING) << "Page Pool has not received back all free pages by its uninitialization!!"
        << " count=" << free_pool_count() << ", capacity=" << free_pool_capacity_;
    } else {
      LOG(INFO) << "Page Pool has received back all free pages. No suspicious behavior.";
    }
    control_block_->uninitialize();
  }
  return kRetOk;
}

ErrorCode PagePoolPimpl::grab(uint32_t desired_grab_count, PagePoolOffsetChunk* chunk) {
  ASSERT_ND(chunk->size() + desired_grab_count <= chunk->capacity());
  VLOG(0) << "Grabbing " << desired_grab_count << " pages."
    << " free_pool_count_=" << free_pool_count();
  soc::SharedMutexScope guard(&control_block_->lock_);
  if (UNLIKELY(free_pool_count() == 0)) {
    LOG(WARNING) << "No more free pages left in the pool";
    return kErrorCodeMemoryNoFreePages;
  }

  // grab from the head
  uint64_t grab_count = std::min<uint64_t>(desired_grab_count, free_pool_count());
  PagePoolOffset* head = free_pool_ + free_pool_head();
  if (free_pool_head() + grab_count > free_pool_capacity_) {
    // wrap around
    uint64_t wrap_count = free_pool_capacity_ - free_pool_head();
    chunk->push_back(head, head + wrap_count);
    free_pool_head() = 0;
    free_pool_count() -= wrap_count;
    grab_count -= wrap_count;
    head = free_pool_;
  }

  // no wrap around (or no more wrap around)
  ASSERT_ND(free_pool_head() + grab_count <= free_pool_capacity_);
  chunk->push_back(head, head + grab_count);
  free_pool_head() += grab_count;
  free_pool_count() -= grab_count;
  return kErrorCodeOk;
}

ErrorCode PagePoolPimpl::grab_one(PagePoolOffset *offset) {
  VLOG(0) << "Grabbing just one page. free_pool_count_=" << free_pool_count();
  *offset = 0;
  soc::SharedMutexScope guard(&control_block_->lock_);
  if (UNLIKELY(free_pool_count() == 0)) {
    LOG(WARNING) << "No more free pages left in the pool";
    return kErrorCodeMemoryNoFreePages;
  }

  // grab from the head
  PagePoolOffset* head = free_pool_ + free_pool_head();
  if (free_pool_head() == free_pool_capacity_) {
    // wrap around
    free_pool_head() = 0;
    head = free_pool_;
  }

  // no wrap around (or no more wrap around)
  ASSERT_ND(free_pool_head() + 1 <= free_pool_capacity_);
  *offset = *head;
  ++free_pool_head();
  --free_pool_count();
  return kErrorCodeOk;
}

void PagePoolPimpl::release(uint32_t desired_release_count, PagePoolOffsetChunk *chunk) {
  ASSERT_ND(chunk->size() >= desired_release_count);
  VLOG(0) << "Releasing " << desired_release_count << " pages."
    << " free_pool_count_=" << free_pool_count();
  soc::SharedMutexScope guard(&control_block_->lock_);
  if (free_pool_count() + desired_release_count > free_pool_capacity_) {
    // this can't happen unless something is wrong! This is a critical issue from which
    // we can't recover because page pool is inconsistent!
    LOG(ERROR) << "PagePoolPimpl::release() More than full free-pool. inconsistent state!";
    // TODO(Hideaki) Minor: Do a duplicate-check here to identify the problemetic pages.
    COERCE_ERROR(ERROR_STACK(kErrorCodeMemoryDuplicatePage));
  }

  // append to the tail
  uint64_t release_count = std::min<uint64_t>(desired_release_count, chunk->size());
  uint64_t tail = free_pool_head() + free_pool_count();
  if (tail >= free_pool_capacity_) {
    tail -= free_pool_capacity_;
  }
  if (tail + release_count > free_pool_capacity_) {
    // wrap around
    uint32_t wrap_count = free_pool_capacity_ - tail;
    chunk->move_to(free_pool_ + tail, wrap_count);
    free_pool_count() += wrap_count;
    release_count -= wrap_count;
    tail = 0;
  }

  // no wrap around (or no more wrap around)
  ASSERT_ND(tail + release_count <= free_pool_capacity_);
  chunk->move_to(free_pool_ + tail, release_count);
  free_pool_count() += release_count;
}

void PagePoolPimpl::release_one(PagePoolOffset offset) {
  ASSERT_ND(is_initialized());
  VLOG(0) << "Releasing just one page. free_pool_count_=" << free_pool_count();
  soc::SharedMutexScope guard(&control_block_->lock_);
  if (free_pool_count() >= free_pool_capacity_) {
    // this can't happen unless something is wrong! This is a critical issue from which
    // we can't recover because page pool is inconsistent!
    LOG(ERROR) << "PagePoolPimpl::release_one() More than full free-pool. inconsistent state!";
    COERCE_ERROR(ERROR_STACK(kErrorCodeMemoryDuplicatePage));
  }

  // append to the tail
  uint64_t tail = free_pool_head() + free_pool_count();
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
  ++free_pool_count();
}


std::ostream& operator<<(std::ostream& o, const PagePoolPimpl& v) {
  o << "<PagePool>"
    << "<memory_>" << v.memory_ << "</memory_>"
    << "<memory_size>" << v.memory_size_ << "</memory_size>"
    << "<owns_>" << v.owns_ << "</owns_>"
    << "<pages_for_free_pool_>" << v.pages_for_free_pool_ << "</pages_for_free_pool_>"
    << "<free_pool_capacity_>" << v.free_pool_capacity_ << "</free_pool_capacity_>"
    << "<free_pool_head_>" << v.free_pool_head() << "</free_pool_head_>"
    << "<free_pool_count_>" << v.free_pool_count() << "</free_pool_count_>"
    << "</PagePool>";
  return o;
}

PagePool::Stat PagePoolPimpl::get_stat() const {
  PagePool::Stat ret;
  ret.total_pages_ = pool_size_ - pages_for_free_pool_;
  ret.allocated_pages_ = ret.total_pages_ - free_pool_count();
  return ret;
}

}  // namespace memory
}  // namespace foedus
