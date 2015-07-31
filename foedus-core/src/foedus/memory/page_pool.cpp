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
#include "foedus/memory/page_pool.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/memory/page_pool_pimpl.hpp"

namespace foedus {
namespace memory {
void PagePoolOffsetChunk::push_back(const PagePoolOffset* begin, const PagePoolOffset* end) {
  uint32_t count = end - begin;
  ASSERT_ND(size_ + count <= kMaxSize);
  std::memcpy(chunk_ + size_, begin, count * sizeof(PagePoolOffset));
  size_ += count;
}
void PagePoolOffsetChunk::move_to(PagePoolOffset* destination, uint32_t count) {
  ASSERT_ND(size_ >= count);
  std::memcpy(destination, chunk_ + (size_ - count), count * sizeof(PagePoolOffset));
  // we move from the tail of this chunk. so, just decrementing the count is enough.
  // no need to move the remainings back to the beginning
  size_ -= count;
}

void PagePoolOffsetDynamicChunk::move_to(PagePoolOffset* destination, uint32_t count) {
  ASSERT_ND(size_ >= count);
  std::memcpy(destination, chunk_, count * sizeof(PagePoolOffset));

  // Skip the consumed entries
  chunk_ += count;
  size_ -= count;
}

uint32_t PagePoolOffsetAndEpochChunk::get_safe_offset_count(const Epoch& threshold) const {
  ASSERT_ND(is_sorted());
  OffsetAndEpoch dummy;
  dummy.safe_epoch_ = threshold.value();
  struct CompareEpoch {
    bool operator() (const OffsetAndEpoch& left, const OffsetAndEpoch& right) {
      return Epoch(left.safe_epoch_) < Epoch(right.safe_epoch_);
    }
  };
  const OffsetAndEpoch* result = std::lower_bound(chunk_, chunk_ + size_, dummy, CompareEpoch());
  ASSERT_ND(result);
  ASSERT_ND(result - chunk_ <= size_);
  return result - chunk_;
}

void PagePoolOffsetAndEpochChunk::move_to(PagePoolOffset* destination, uint32_t count) {
  ASSERT_ND(size_ >= count);
  // we can't do memcpy. Just copy one by one
  for (uint32_t i = 0; i < count; ++i) {
    destination[i] = chunk_[i].offset_;
  }
  // Also, unlike PagePoolOffsetChunk, we copied from the head (we have to because epoch matters).
  // So, we also have to move remainings to the beginning
  if (size_ > count) {
    std::memmove(chunk_, chunk_ + count, (size_ - count) * sizeof(OffsetAndEpoch));
  }
  size_ -= count;
  ASSERT_ND(is_sorted());
}

bool PagePoolOffsetAndEpochChunk::is_sorted() const {
  for (uint32_t i = 1; i < size_; ++i) {
    ASSERT_ND(chunk_[i].offset_);
    if (Epoch(chunk_[i - 1U].safe_epoch_) > Epoch(chunk_[i].safe_epoch_)) {
      return false;
    }
  }
  return true;
}


PagePool::PagePool() {
  pimpl_ = new PagePoolPimpl();
}
void PagePool::attach(
  PagePoolControlBlock* control_block,
  void* memory,
  uint64_t memory_size,
  bool owns,
  bool rigorous_page_boundary_check) {
  pimpl_->attach(control_block, memory, memory_size, owns, rigorous_page_boundary_check);
}
PagePool::~PagePool() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  PagePool::initialize() { return pimpl_->initialize(); }
bool        PagePool::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  PagePool::uninitialize() { return pimpl_->uninitialize(); }
uint64_t    PagePool::get_memory_size() const { return pimpl_->memory_size_; }
PagePool::Stat PagePool::get_stat() const { return pimpl_->get_stat(); }
storage::Page* PagePool::get_base() const { return pimpl_->pool_base_; }
uint64_t    PagePool::get_free_pool_capacity() const { return pimpl_->get_free_pool_capacity(); }
std::string PagePool::get_debug_pool_name() const { return pimpl_->get_debug_pool_name(); }
void PagePool::set_debug_pool_name(const std::string& name) { pimpl_->set_debug_pool_name(name); }

uint32_t PagePool::get_recommended_pages_per_grab() const {
  return std::min<uint32_t>(1U << 12, pimpl_->pool_size_ / 8U);
}


ErrorCode   PagePool::grab(uint32_t desired_grab_count, PagePoolOffsetChunk* chunk) {
  return pimpl_->grab(desired_grab_count, chunk);
}
ErrorCode   PagePool::grab_one(PagePoolOffset* offset) { return pimpl_->grab_one(offset); }

void        PagePool::release(uint32_t desired_release_count, PagePoolOffsetChunk *chunk) {
  pimpl_->release(desired_release_count, chunk);
}
void        PagePool::release(uint32_t desired_release_count, PagePoolOffsetDynamicChunk* chunk) {
  pimpl_->release(desired_release_count, chunk);
}
void        PagePool::release(uint32_t desired_release_count, PagePoolOffsetAndEpochChunk* chunk) {
  pimpl_->release(desired_release_count, chunk);
}

void PagePool::release_one(PagePoolOffset offset) { pimpl_->release_one(offset); }

const LocalPageResolver& PagePool::get_resolver() const { return pimpl_->get_resolver(); }

std::ostream& operator<<(std::ostream& o, const PagePool& v) {
  o << v.pimpl_;
  return o;
}

PageReleaseBatch::PageReleaseBatch(Engine* engine)
  : engine_(engine), numa_node_count_(engine->get_options().thread_.group_count_) {
  std::memset(chunks_, 0, sizeof(ChunkPtr) * 256);
  for (thread::ThreadGroupId i = 0; i < numa_node_count_; ++i) {
    chunks_[i] = new PagePoolOffsetChunk;
  }
}

PageReleaseBatch::~PageReleaseBatch() {
  release_all();
  for (thread::ThreadGroupId i = 0; i < numa_node_count_; ++i) {
    delete chunks_[i];
    chunks_[i] = nullptr;
  }
}

void PageReleaseBatch::release(thread::ThreadGroupId numa_node, PagePoolOffset offset) {
  ASSERT_ND(numa_node < numa_node_count_);
  if (chunks_[numa_node]->full()) {
    release_chunk(numa_node);
  }
  ASSERT_ND(!chunks_[numa_node]->full());
  chunks_[numa_node]->push_back(offset);
}

void PageReleaseBatch::release_chunk(thread::ThreadGroupId numa_node) {
  ASSERT_ND(numa_node < numa_node_count_);
  if (chunks_[numa_node]->empty()) {
    return;
  }

  engine_->get_memory_manager()->get_node_memory(numa_node)->get_volatile_pool()->release(
    chunks_[numa_node]->size(), chunks_[numa_node]);
  ASSERT_ND(chunks_[numa_node]->empty());
}

void PageReleaseBatch::release_all() {
  for (thread::ThreadGroupId i = 0; i < numa_node_count_; ++i) {
    release_chunk(i);
  }
}
RoundRobinPageGrabBatch::RoundRobinPageGrabBatch(Engine* engine)
: engine_(engine), numa_node_count_(engine->get_options().thread_.group_count_), current_node_(0) {
}

RoundRobinPageGrabBatch::~RoundRobinPageGrabBatch() {
  release_all();
}

storage::VolatilePagePointer RoundRobinPageGrabBatch::grab() {
  if (chunk_.empty()) {
    const thread::ThreadGroupId old = current_node_;
    while (true) {
      ++current_node_;
      if (current_node_ >= numa_node_count_) {
        current_node_ = 0;
      }
      PagePool* pool = engine_->get_memory_manager()->get_node_memory(current_node_)->
        get_volatile_pool();
      uint32_t grab_count = std::min<uint32_t>(
        chunk_.capacity(),
        pool->get_recommended_pages_per_grab());
      ErrorCode code = pool->grab(grab_count, &chunk_);
      if (code == kErrorCodeOk) {
        break;
      }

      if (code == kErrorCodeMemoryNoFreePages) {
        LOG(WARNING) << "NUMA node-" << current_node_ << " has no more free pages."
          << " trying another node..";
        if (current_node_ == old) {
          print_backtrace();
          LOG(FATAL) << "No NUMA node has any free pages. This situation is so far "
            " not handled. Aborting";
        }
      } else {
        LOG(FATAL) << "Unexpected error code.. wtf error="
          << code << "(" << get_error_name(code) << ")";
      }
    }
    ASSERT_ND(!chunk_.empty());
  }

  storage::VolatilePagePointer ret;
  ret.components.numa_node = current_node_;
  ret.components.flags = 0;
  ret.components.mod_count = 0;
  ret.components.offset = chunk_.pop_back();
  return ret;
}

void RoundRobinPageGrabBatch::release_all() {
  if (chunk_.empty()) {
    return;
  }

  engine_->get_memory_manager()->get_node_memory(current_node_)->get_volatile_pool()->release(
    chunk_.size(), &chunk_);
  ASSERT_ND(chunk_.empty());
}


DivvyupPageGrabBatch::DivvyupPageGrabBatch(Engine* engine)
: engine_(engine), node_count_(engine->get_options().thread_.group_count_) {
  chunks_ = new PagePoolOffsetChunk[node_count_];
  for (uint16_t node = 0; node < node_count_; ++node) {
    ASSERT_ND(chunks_[node].empty());
  }
}

DivvyupPageGrabBatch::~DivvyupPageGrabBatch() {
  release_all();
  delete[] chunks_;
  chunks_ = nullptr;
}


storage::VolatilePagePointer DivvyupPageGrabBatch::grab(thread::ThreadGroupId node) {
  ASSERT_ND(node < node_count_);
  if (chunks_[node].empty()) {
    PagePool* pool = engine_->get_memory_manager()->get_node_memory(node)->get_volatile_pool();
    uint32_t grab_count = std::min<uint32_t>(
      chunks_[node].capacity(),
      pool->get_recommended_pages_per_grab());
    ErrorCode code = pool->grab(grab_count, chunks_ + node);
    if (code == kErrorCodeMemoryNoFreePages) {
      LOG(FATAL) << "NUMA node " << static_cast<int>(node) << " has no free pages. This situation "
        " is so far not handled in DivvyupPageGrabBatch. Aborting";
    } else if (code != kErrorCodeOk) {
      LOG(FATAL) << "Unexpected error code.. wtf error="
        << code << "(" << get_error_name(code) << ")";
    }
  }
  storage::VolatilePagePointer ret;
  ret.components.numa_node = node;
  ret.components.flags = 0;
  ret.components.mod_count = 0;
  ret.components.offset = chunks_[node].pop_back();
  return ret;
}

storage::VolatilePagePointer DivvyupPageGrabBatch::grab_evenly(uint64_t cur, uint64_t total) {
  thread::ThreadGroupId node;
  if (node_count_ == 1U) {
    node = 0;
  } else {
    node = cur * node_count_ / total;
  }
  return grab(node);
}

void DivvyupPageGrabBatch::release_all() {
  for (uint16_t node = 0; node < node_count_; ++node) {
    if (chunks_[node].empty()) {
      continue;
    }
    engine_->get_memory_manager()->get_node_memory(node)->get_volatile_pool()->release(
      chunks_[node].size(), chunks_ + node);
    ASSERT_ND(chunks_[node].empty());
  }
}
}  // namespace memory
}  // namespace foedus
