/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/memory/aligned_memory.hpp"

#include <numa.h>
#include <glog/logging.h>

#include <cstdlib>
#include <cstring>
#include <ostream>

#include "foedus/assert_nd.hpp"
#include "foedus/debugging/stop_watch.hpp"

namespace foedus {
namespace memory {
AlignedMemory::AlignedMemory(uint64_t size, uint64_t alignment,
               AllocType alloc_type, int numa_node) noexcept
  : size_(0), alignment_(0), alloc_type_(kPosixMemalign), numa_node_(0), block_(nullptr) {
  alloc(size, alignment, alloc_type, numa_node);
}

void AlignedMemory::alloc(
  uint64_t size,
  uint64_t alignment,
  AllocType alloc_type,
  int numa_node) noexcept {
  release_block();
  ASSERT_ND(block_ == nullptr);
  size_ = size;
  alignment_ = alignment;
  alloc_type_ = alloc_type;
  numa_node_ = numa_node;
  ASSERT_ND((alignment & (alignment - 1)) == 0);  // alignment is power of two
  if (size_ % alignment != 0) {
    size_ = ((size_ / alignment) + 1) * alignment;
  }
  debugging::StopWatch watch;
  switch (alloc_type_) {
    case kPosixMemalign:
      ::posix_memalign(&block_, alignment, size_);
      break;
    case kNumaAllocInterleaved:
      block_ = ::numa_alloc_interleaved(size_);
      break;
    case kNumaAllocOnnode:
      block_ = ::numa_alloc_onnode(size_, numa_node);
      break;
    default:
      ASSERT_ND(false);
  }
  watch.stop();

  debugging::StopWatch watch2;
  std::memset(block_, 0, size_);  // see class comment for why we do this immediately
  watch2.stop();
  LOG(INFO) << "Allocated memory in " << watch.elapsed_ns() << "+"
    << watch2.elapsed_ns() << " ns (alloc+memset)." << *this;
}


AlignedMemory::AlignedMemory(AlignedMemory &&other) noexcept : block_(nullptr) {
  *this = std::move(other);
}
AlignedMemory& AlignedMemory::operator=(AlignedMemory &&other) noexcept {
  release_block();
  size_ = other.size_;
  alignment_ = other.alignment_;
  alloc_type_ = other.alloc_type_;
  block_ = other.block_;
  other.block_ = nullptr;
  return *this;
}

void AlignedMemory::release_block() {
  if (block_ != nullptr) {
    switch (alloc_type_) {
      case kPosixMemalign:
        ::free(block_);
        break;
      case kNumaAllocInterleaved:
      case kNumaAllocOnnode:
        ::numa_free(block_, size_);
        break;
      default:
        ASSERT_ND(false);
    }
    block_ = nullptr;
  }
}

std::ostream& operator<<(std::ostream& o, const AlignedMemory& v) {
  o << "<AlignedMemory>";
  o << "<is_null>" << v.is_null() << "</is_null>";
  o << "<size>" << v.get_size() << "</size>";
  o << "<alignment>" << v.get_alignment() << "</alignment>";
  o << "<alloc_type>" << v.get_alloc_type() << "</alloc_type>";
  o << "<numa_node>" << static_cast<int>(v.get_numa_node()) << "</numa_node>";
  o << "<address>" << v.get_block() << "</address>";
  o << "</AlignedMemory>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const AlignedMemorySlice& v) {
  o << "<AlignedMemorySlice>";
  o << "<offset>" << v.offset_ << "</offset>";
  o << "<count>" << v.count_ << "</count>";
  if (v.memory_) {
    o << *v.memory_;
  }
  o << "</AlignedMemorySlice>";
  return o;
}
}  // namespace memory
}  // namespace foedus

