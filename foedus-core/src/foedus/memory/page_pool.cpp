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

PagePool::PagePool(Engine* engine, thread::ThreadGroupId numa_node) : pimpl_(nullptr) {
  pimpl_ = new PagePoolPimpl(engine, numa_node);
}
PagePool::~PagePool() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  PagePool::initialize() { return pimpl_->initialize(); }
bool        PagePool::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  PagePool::uninitialize() { return pimpl_->uninitialize(); }
thread::ThreadGroupId PagePool::get_numa_node() const { return pimpl_->numa_node_; }

ErrorCode   PagePool::grab(uint32_t desired_grab_count, PagePoolOffsetChunk* chunk) {
  return pimpl_->grab(desired_grab_count, chunk);
}
void        PagePool::release(uint32_t desired_release_count, PagePoolOffsetChunk *chunk) {
  pimpl_->release(desired_release_count, chunk);
}
LocalPageResolver& PagePool::get_resolver() { return pimpl_->get_resolver(); }

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

  engine_->get_memory_manager().get_node_memory(numa_node)->get_page_pool().release(
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
      ErrorCode code = engine_->get_memory_manager().get_node_memory(current_node_)->
        get_page_pool().grab(chunk_.capacity(), &chunk_);
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

  engine_->get_memory_manager().get_node_memory(current_node_)->get_page_pool().release(
    chunk_.size(), &chunk_);
  ASSERT_ND(chunk_.empty());
}

}  // namespace memory
}  // namespace foedus
