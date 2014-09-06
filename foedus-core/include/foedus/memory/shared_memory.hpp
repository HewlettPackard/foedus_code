/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_SHARED_MEMORY_HPP_
#define FOEDUS_MEMORY_SHARED_MEMORY_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"

namespace foedus {
namespace memory {
/**
 * @brief Represents memory shared between processes.
 * @ingroup MEMORY
 * @details
 * This class was initially a part of AlignedMemory, but we separated it out to reflect the
 * special semantics of this type of memory.
 *
 * @par Alloc/dealloc type
 * As this is a shared memory, there is only one choice unlike AlignedMemory.
 * We always use mmap() and munmap(). We always use MAP_HUGETLB.
 * We also automatically use MAP_HUGE_1GB if it's available and if it's appropriate for the
 * memory size.
 *
 * @par Ownership
 * Every shared memory is allocated and owned by the parent process.
 * Forked child processes, whether they are 'emulated' processes or not, do not have ownership
 * thus should not release them at exit.
 *
 * @par Moveable/Copiable
 * This object is \e NOT moveable or copiable.
 */
class SharedMemory CXX11_FINAL {
 public:
  /** Empty constructor which allocates nothing. */
  SharedMemory() CXX11_NOEXCEPT : size_(0), numa_node_(0), owned_(false), block_(CXX11_NULLPTR) {}

  // Disable default constructors
  SharedMemory(const SharedMemory &other) CXX11_FUNC_DELETE;
  SharedMemory& operator=(const SharedMemory &other) CXX11_FUNC_DELETE;

  /** Automatically releases the memory. */
  ~SharedMemory() { release_block(); }

  /**
   * Allocate a shared memory of given size on given NUMA node.
   * @param[in] size Byte size of the memory block. Actual allocation is at least of this size.
   * @param[in] numa_node Where the physical memory is allocated. Use for binding via libnuma.
   * @attention When memory allocation fails for some reason (eg Out-Of-Memory), this constructor
   * does NOT fail nor throws an exception. Instead, it sets the block_ NULL.
   * So, the caller is responsible for checking it after construction.
   */
  void        alloc(uint64_t size, int numa_node);
  /** Returns the memory block. */
  void*       get_block() const { return block_; }
  /** Returns if this object doesn't hold a valid memory block. */
  bool        is_null() const { return block_ == CXX11_NULLPTR; }
  /** Returns if this process owns this memory and is responsible to delete it. */
  bool        is_owned() const { return owned_; }
  /** Child processes (NOT emulated ones) first calls this to avoid double free */
  void        throw_away_ownership() {
    ASSERT_ND(owned_);
    owned_ = false;
  }
  /** Child processes (emulated or not) set a reference to shared memory by calling this method */
  void        steal_shared(uint64_t size, int numa_node, void* block) {
    release_block();
    block_ = block;
    size_ = size;
    numa_node_ = numa_node;
    owned_ = false;  // just referring to the shared memory. we don't have ownership
  }
  /** Returns the byte size of the memory block. */
  uint64_t    get_size() const { return size_; }
  /** Where the physical memory is allocated. */
  int         get_numa_node() const { return numa_node_; }

  /** Releases the memory block \b IF this process has an ownership. */
  void        release_block();

  friend std::ostream&    operator<<(std::ostream& o, const SharedMemory& v);

 private:
  /** Byte size of the memory block. */
  uint64_t    size_;
  /** Where the physical memory is allocated. */
  int         numa_node_;
  /**
   * Whether this process owns this memory and is responsible to delete it.
   * The first thing child processes do right after fork() is to turn this off for all
   * shared memory blocks to avoid double free.
   */
  bool        owned_;
  /** Allocated memory block. */
  void*       block_;
};

}  // namespace memory
}  // namespace foedus

#endif  // FOEDUS_MEMORY_SHARED_MEMORY_HPP_
