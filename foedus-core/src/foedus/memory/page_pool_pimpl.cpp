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
  if (owns) {
    control_block_->debug_pool_name_.clear();
  }
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
    LOG(INFO) << get_debug_pool_name()
      << " - total_pages=" << pool_size_ << ", pages_for_free_pool_=" << pages_for_free_pool_;
    control_block_->initialize();
    LOG(INFO) << get_debug_pool_name() << " - Constructing circular free pool...";
    // all pages after pages_for_free_pool_-th page is in the free pool at first
    for (uint64_t i = 0; i < free_pool_capacity_; ++i) {
      free_pool_[i] = pages_for_free_pool_ + i;
    }

    // [experimental] randomize the free pool pointers so that we can evenly utilize all
    // memory banks
    if (false) {  // disabled for now
      // this should be an option...
      LOG(INFO) << get_debug_pool_name() << " - Randomizing free pool...";
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
      LOG(INFO) << get_debug_pool_name() << " - Randomized free pool.";
    }

    control_block_->free_pool_head_ = 0;
    control_block_->free_pool_count_ = free_pool_capacity_;
    LOG(INFO) << get_debug_pool_name() << " - Constructed circular free pool.";
  }

  return kRetOk;
}

ErrorStack PagePoolPimpl::uninitialize_once() {
  if (owns_) {
    uint64_t free_count = get_free_pool_count();
    if (free_count != free_pool_capacity_) {
      // This is not a memory leak as we anyway releases everything, but it's a smell of bug.
      LOG(WARNING) << get_debug_pool_name()
        << " - Page Pool has not received back all free pages by its uninitialization!!"
        << " count=" << free_count << ", capacity=" << free_pool_capacity_;
    } else {
      LOG(INFO) << get_debug_pool_name()
        << " - Page Pool has received back all free pages. No suspicious behavior.";
    }
    control_block_->uninitialize();
  }
  return kRetOk;
}

ErrorCode PagePoolPimpl::grab(uint32_t desired_grab_count, PagePoolOffsetChunk* chunk) {
  ASSERT_ND(chunk->size() + desired_grab_count <= chunk->capacity());
  VLOG(0) << get_debug_pool_name() << " - Grabbing " << desired_grab_count << " pages."
    << " free_pool_count_=" << get_free_pool_count()
    << "->" << (get_free_pool_count() - desired_grab_count);
  soc::SharedMutexScope guard(&control_block_->lock_);
  uint64_t free_count = get_free_pool_count();
  if (UNLIKELY(free_count == 0)) {
    LOG(WARNING) << get_debug_pool_name() << " - No more free pages left in the pool";
    return kErrorCodeMemoryNoFreePages;
  }

  // grab from the head
  uint64_t grab_count = std::min<uint64_t>(desired_grab_count, free_count);
  PagePoolOffset* head = free_pool_ + free_pool_head();
  if (free_pool_head() + grab_count > free_pool_capacity_) {
    // wrap around
    uint64_t wrap_count = free_pool_capacity_ - free_pool_head();
    chunk->push_back(head, head + wrap_count);
    free_pool_head() = 0;
    decrease_free_pool_count(wrap_count);
    grab_count -= wrap_count;
    head = free_pool_;
  }

  // no wrap around (or no more wrap around)
  ASSERT_ND(free_pool_head() + grab_count <= free_pool_capacity_);
  chunk->push_back(head, head + grab_count);
  free_pool_head() += grab_count;
  decrease_free_pool_count(grab_count);
  return kErrorCodeOk;
}

ErrorCode PagePoolPimpl::grab_one(PagePoolOffset *offset) {
  VLOG(1) << get_debug_pool_name()
    << " - Grabbing just one page. free_pool_count_=" << get_free_pool_count() << "->"
    << (get_free_pool_count() - 1);
  *offset = 0;
  soc::SharedMutexScope guard(&control_block_->lock_);
  uint64_t free_count = get_free_pool_count();
  if (UNLIKELY(free_count == 0)) {
    LOG(WARNING) << get_debug_pool_name() << " - No more free pages left in the pool";
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
  decrease_free_pool_count(1);
  return kErrorCodeOk;
}

template <typename CHUNK>
void PagePoolPimpl::release_impl(uint32_t desired_release_count, CHUNK* chunk) {
  ASSERT_ND(chunk->size() >= desired_release_count);
  VLOG(0) << get_debug_pool_name() << " - Releasing " << desired_release_count << " pages."
    << " free_pool_count_=" << get_free_pool_count() << "->"
    << (get_free_pool_count() + desired_release_count);
  soc::SharedMutexScope guard(&control_block_->lock_);
  uint64_t free_count = get_free_pool_count();
  if (free_count + desired_release_count > free_pool_capacity_) {
    // this can't happen unless something is wrong! This is a critical issue from which
    // we can't recover because page pool is inconsistent!
    LOG(ERROR) << get_debug_pool_name()
      << " - PagePoolPimpl::release() More than full free-pool. inconsistent state!"
        << " free_count/capacity/release_count=" << free_count << "/" << free_pool_capacity_
          << "/" << desired_release_count;
    // TASK(Hideaki) Do a duplicate-check here to identify the problemetic pages.
    // crash here only in debug mode. otherwise just log the error
    // ASSERT_ND(free_count + desired_release_count <= free_pool_capacity_);
    // TASK(Hideaki) need to figure out why we hit this.
    return;
  }

  // append to the tail
  uint64_t release_count = std::min<uint64_t>(desired_release_count, chunk->size());
  uint64_t tail = free_pool_head() + free_count;
  if (tail >= free_pool_capacity_) {
    tail -= free_pool_capacity_;
  }
  if (tail + release_count > free_pool_capacity_) {
    // wrap around
    uint32_t wrap_count = free_pool_capacity_ - tail;
    chunk->move_to(free_pool_ + tail, wrap_count);
    increase_free_pool_count(wrap_count);
    release_count -= wrap_count;
    tail = 0;
  }

  // no wrap around (or no more wrap around)
  ASSERT_ND(tail + release_count <= free_pool_capacity_);
  chunk->move_to(free_pool_ + tail, release_count);
  increase_free_pool_count(release_count);
}
void PagePoolPimpl::release(uint32_t desired_release_count, PagePoolOffsetChunk* chunk) {
  release_impl<PagePoolOffsetChunk>(desired_release_count, chunk);
}
void PagePoolPimpl::release(uint32_t desired_release_count, PagePoolOffsetDynamicChunk* chunk) {
  release_impl<PagePoolOffsetDynamicChunk>(desired_release_count, chunk);
}
void PagePoolPimpl::release(uint32_t desired_release_count, PagePoolOffsetAndEpochChunk* chunk) {
  release_impl<PagePoolOffsetAndEpochChunk>(desired_release_count, chunk);
}

void PagePoolPimpl::release_one(PagePoolOffset offset) {
  ASSERT_ND(is_initialized() || !owns_);
  VLOG(1) << get_debug_pool_name() << " - Releasing just one page. free_pool_count_="
    << get_free_pool_count() << "->" << (get_free_pool_count() + 1);
  soc::SharedMutexScope guard(&control_block_->lock_);
  uint64_t free_count = get_free_pool_count();
  if (free_count >= free_pool_capacity_) {
    // this can't happen unless something is wrong! This is a critical issue from which
    // we can't recover because page pool is inconsistent!
    LOG(ERROR) << get_debug_pool_name()
      << " - PagePoolPimpl::release_one() More than full free-pool. inconsistent state!";
    COERCE_ERROR(ERROR_STACK(kErrorCodeMemoryDuplicatePage));
  }

  // append to the tail
  uint64_t tail = free_pool_head() + free_count;
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
  increase_free_pool_count(1);
}


std::ostream& operator<<(std::ostream& o, const PagePoolPimpl& v) {
  o << "<PagePool>"
    << "<name_>" << v.get_debug_pool_name() << "</name_>"
    << "<memory_>" << v.memory_ << "</memory_>"
    << "<memory_size>" << v.memory_size_ << "</memory_size>"
    << "<owns_>" << v.owns_ << "</owns_>"
    << "<pages_for_free_pool_>" << v.pages_for_free_pool_ << "</pages_for_free_pool_>"
    << "<free_pool_capacity_>" << v.free_pool_capacity_ << "</free_pool_capacity_>"
    << "<free_pool_head_>" << v.free_pool_head() << "</free_pool_head_>"
    << "<free_pool_count_>" << v.get_free_pool_count() << "</free_pool_count_>"
    << "</PagePool>";
  return o;
}

PagePool::Stat PagePoolPimpl::get_stat() const {
  PagePool::Stat ret;
  ret.total_pages_ = pool_size_ - pages_for_free_pool_;
  ret.allocated_pages_ = ret.total_pages_ - get_free_pool_count();
  return ret;
}

}  // namespace memory
}  // namespace foedus
