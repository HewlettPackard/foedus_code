/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/memory/aligned_memory.hpp"

#include <numa.h>
#include <numaif.h>
#include <valgrind.h>
#include <glog/logging.h>
#include <sys/mman.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/stop_watch.hpp"


// this is a quite new flag, so not exists in many environment. define it here.
#ifndef MAP_HUGE_SHIFT
#define MAP_HUGE_SHIFT  26
#endif  // MAP_HUGE_SHIFT
#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#endif  // MAP_HUGE_2MB
#ifndef MAP_HUGE_1GB
#define MAP_HUGE_1GB (30 << MAP_HUGE_SHIFT)
#endif  // MAP_HUGE_1GB

namespace foedus {
namespace memory {
AlignedMemory::AlignedMemory(uint64_t size, uint64_t alignment,
               AllocType alloc_type, int numa_node) noexcept
  : size_(0), alignment_(0), alloc_type_(kPosixMemalign), numa_node_(0),
  block_(nullptr) {
  alloc(size, alignment, alloc_type, numa_node);
}

// std::mutex mmap_allocate_mutex;
// No, this doesn't matter. Rather, turns out that the issue is in linux kernel:
// https://git.kernel.org/cgit/linux/kernel/git/torvalds/linux.git/commit/?id=8382d914ebf72092aa15cdc2a5dcedb2daa0209d
// In linux 3.15 and later, this problem gets resolved and highly parallelizable.

char* alloc_mmap(uint64_t size, uint64_t alignment) {
  // std::lock_guard<std::mutex> guard(mmap_allocate_mutex);
  // we don't use MAP_POPULATE because it will block here and also serialize hugepage allocation!
  // even if we run mmap in parallel, linux serializes the looooong population in all numa nodes.
  // lame. we will memset right after this.
  int pagesize;
  if (alignment >= (1ULL << 30)) {
    if (is_1gb_hugepage_enabled()) {
      pagesize = MAP_HUGE_1GB | MAP_HUGETLB;
    } else {
      pagesize = MAP_HUGE_2MB | MAP_HUGETLB;
    }
  } else if (alignment >= (1ULL << 21)) {
    pagesize = MAP_HUGE_2MB | MAP_HUGETLB;
  } else {
    pagesize = 0;
  }
  bool running_on_valgrind = RUNNING_ON_VALGRIND;
  if (running_on_valgrind) {
    // if this is running under valgrind, we have to avoid using hugepages due to a bug in valgrind.
    // When we are running on valgrind, we don't care performance anyway. So shouldn't matter.
    pagesize = 0;
  }
  char* ret = reinterpret_cast<char*>(::mmap(
    nullptr,
    size,
    PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS | MAP_PRIVATE | MAP_NORESERVE | pagesize,
    -1,
    0));
  // when mmap() fails, it returns -1 (MAP_FAILED)
  if (ret == nullptr || ret == MAP_FAILED) {
    LOG(FATAL) << "mmap() failed. size=" << size << ", error=" << assorted::os_error()
      << ". This error usually means you don't have enough hugepages allocated."
      << " eg) sudo sh -c 'echo 196608 > /proc/sys/vm/nr_hugepages'";
  }
  return ret;
}

void* alloc_mmap_1gb_pages(uint64_t size) {
  ASSERT_ND(size % (1ULL << 30) == 0);
  return alloc_mmap(size, 1ULL << 30);
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
  if (alloc_type_ == kNumaMmapOneGbPages) {
    alignment = 1ULL << 30;
  }
  if (size_ == 0 || size_ % alignment != 0) {
    size_ = ((size_ / alignment) + 1) * alignment;
  }

  // Use libnuma's numa_set_preferred to initialize the NUMA node of the memory.
  // We can later do the equivalent with mbind IF the memory is not shared.
  // mbind does nothing for shared memory. So, this is the only way
  int original_node = ::numa_preferred();
  ::numa_set_preferred(numa_node);

  debugging::StopWatch watch;
  switch (alloc_type_) {
    case kPosixMemalign:
      ::posix_memalign(&block_, alignment, size_);
      break;
    case kNumaAllocInterleaved:  // actually we no longer support this.. no reason to use this.
    case kNumaAllocOnnode:
      block_ = alloc_mmap(size_, alignment);
      break;
    case kNumaMmapOneGbPages:
      block_ = alloc_mmap_1gb_pages(size_);
      break;
    default:
      ASSERT_ND(false);
  }
  watch.stop();

  debugging::StopWatch watch2;
  std::memset(block_, 0, size_);  // see class comment for why we do this immediately
  watch2.stop();
  ::numa_set_preferred(original_node);
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
      case kNumaMmapOneGbPages:
        ::munmap(block_, size_);
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
  o << "<alloc_type>" << v.get_alloc_type() << " (";
  switch (v.get_alloc_type()) {
    case AlignedMemory::kPosixMemalign:
      o << "kPosixMemalign";
      break;
    case AlignedMemory::kNumaAllocInterleaved:
      o << "kNumaAllocInterleaved";
      break;
    case AlignedMemory::kNumaAllocOnnode:
      o << "kNumaAllocOnnode";
      break;
    case AlignedMemory::kNumaMmapOneGbPages:
      o << "kNumaMmapOneGbPages";
      break;
    default:
      o << "Unknown";
  }
  o << ")</alloc_type>";
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

bool is_1gb_hugepage_enabled() {
  // /proc/meminfo should have "Hugepagesize:    1048576 kB"
  // Unfortunately, sysinfo() doesn't provide this information. So, just read the whole file.
  // Alternatively, we can use gethugepagesizes(3) in libhugetlbs, but I don't want to add
  // a dependency just for that...
  std::ifstream file("/proc/meminfo");
  if (!file.is_open()) {
    return false;
  }

  std::string line;
  while (std::getline(file, line)) {
    if (line.find("Hugepagesize:") != std::string::npos) {
      break;
    }
  }
  file.close();
  if (line.find("1048576 kB") != std::string::npos) {
    return true;
  }
  return false;
}
}  // namespace memory
}  // namespace foedus

