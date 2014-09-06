/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/memory/shared_memory.hpp"

#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>

#include <cstring>
#include <iostream>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/memory/aligned_memory.hpp"


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
void SharedMemory::alloc(uint64_t size, int numa_node) {
  release_block();
  ASSERT_ND(block_ == nullptr);
  size_ = size;
  numa_node_ = numa_node;
  owned_ = true;

  int pagesize;
  if (size >= (768ULL << 20) && is_1gb_hugepage_enabled()) {
    pagesize = MAP_HUGE_1GB | MAP_HUGETLB;
    if (size_ % (1ULL << 30) != 0) {
      size_ = ((size_ >> 30) + 1ULL) << 30;
    }
  } else {
    pagesize = MAP_HUGE_2MB | MAP_HUGETLB;
    if (size_ % (1ULL << 21) != 0) {
      size_ = ((size_ >> 21) + 1ULL) << 21;
    }
  }
  // Use libnuma's numa_set_preferred to initialize the NUMA node of the memory.
  // This is the only way to control numa allocation for shared memory.
  // mbind does nothing for shared memory.
  int original_node = ::numa_preferred();
  ::numa_set_preferred(numa_node);

  // we don't use MAP_POPULATE because it will block here and also serialize hugepage allocation!
  // even if we run mmap in parallel, linux serializes the looooong population in all numa nodes.
  // lame. we will memset right after this.
  block_ = ::mmap(
    nullptr,
    size_,
    PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS | MAP_SHARED | MAP_NORESERVE | pagesize,
    -1,
    0);
  // when mmap() fails, it returns -1 (MAP_FAILED)
  if (block_ == MAP_FAILED) {
    // Do not use glog in this module. It might be before initializing debug module
    std::cerr << "mmap() failed. size=" << size_ << "(" << size << ")"
      << ", error=" << assorted::os_error()
      << ". This error usually means you don't have enough hugepages allocated."
      << " eg) sudo sh -c 'echo 196608 > /proc/sys/vm/nr_hugepages'" << std::endl;
    block_ = nullptr;
  }

  std::memset(block_, 0, size_);  // see class comment for why we do this immediately
  // This memset takes a very long time due to the issue in linux kernel:
  // https://git.kernel.org/cgit/linux/kernel/git/torvalds/linux.git/commit/?id=8382d914ebf72092aa15cdc2a5dcedb2daa0209d
  // In linux 3.15 and later, this problem gets resolved and highly parallelizable.
  ::numa_set_preferred(original_node);
}

void SharedMemory::release_block() {
  if (block_ != nullptr) {
    if (owned_) {
      ::munmap(block_, size_);
    }
    block_ = nullptr;
  }
}

std::ostream& operator<<(std::ostream& o, const SharedMemory& v) {
  o << "<SharedMemory>";
  o << "<size>" << v.get_size() << "</size>";
  o << "<owned>" << v.is_owned() << "</owned>";
  o << "<numa_node>" << static_cast<int>(v.get_numa_node()) << "</numa_node>";
  o << "<address>" << v.get_block() << "</address>";
  o << "</SharedMemory>";
  return o;
}

}  // namespace memory
}  // namespace foedus

