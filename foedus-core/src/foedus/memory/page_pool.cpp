/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/memory/page_pool.hpp"

#include <glog/logging.h>

#include <cstring>

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
  size_ -= count;
}

PagePool::PagePool() {
  pimpl_ = new PagePoolPimpl();
}
void PagePool::attach(
  PagePoolControlBlock* control_block,
  void* memory,
  uint64_t memory_size,
  bool owns) {
  pimpl_->attach(control_block, memory, memory_size, owns);
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


ErrorCode   PagePool::grab(uint32_t desired_grab_count, PagePoolOffsetChunk* chunk) {
  return pimpl_->grab(desired_grab_count, chunk);
}
ErrorCode   PagePool::grab_one(PagePoolOffset* offset) { return pimpl_->grab_one(offset); }

void        PagePool::release(uint32_t desired_release_count, PagePoolOffsetChunk *chunk) {
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
      ErrorCode code = engine_->get_memory_manager()->get_node_memory(current_node_)->
        get_volatile_pool()->grab(chunk_.capacity(), &chunk_);
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
    ErrorCode code = engine_->get_memory_manager()->get_node_memory(node)->
      get_volatile_pool()->grab(chunks_[node].capacity(), chunks_ + node);
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
