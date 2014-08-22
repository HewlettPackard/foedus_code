/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_NUMA_CORE_MEMORY_HPP_
#define FOEDUS_MEMORY_NUMA_CORE_MEMORY_HPP_
#include <stdint.h>

#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace memory {
/**
 * @brief Repository of memories dynamically acquired within one CPU core (thread).
 * @ingroup MEMHIERARCHY THREAD
 * @details
 * One NumaCoreMemory corresponds to one foedus::thread::Thread.
 * Each Thread exclusively access its NumaCoreMemory so that it needs no synchronization
 * nor causes cache misses/cache-line ping-pongs.
 * All memories here are allocated/freed via ::numa_alloc_interleaved(), ::numa_alloc_onnode(),
 * and ::numa_free() (except the user specifies to not use them).
 */
class NumaCoreMemory CXX11_FINAL : public DefaultInitializable {
 public:
  /** Packs pointers to pieces of small_thread_local_memory_*/
  struct SmallThreadLocalMemoryPieces {
    char* thread_mcs_block_memory_;
    char* xct_pointer_access_memory_;
    char* xct_page_version_memory_;
    char* xct_read_access_memory_;
    char* xct_write_access_memory_;
    char* xct_lock_free_write_access_memory_;
  };

  NumaCoreMemory() CXX11_FUNC_DELETE;
  NumaCoreMemory(Engine* engine, NumaNodeMemory *node_memory, thread::ThreadId core_id);
  ErrorStack  initialize_once() CXX11_OVERRIDE;
  ErrorStack  uninitialize_once() CXX11_OVERRIDE;

  AlignedMemorySlice get_log_buffer_memory() const { return log_buffer_memory_; }

  /** Returns the parent memory repository. */
  NumaNodeMemory* get_node_memory()       const { return node_memory_; }

  /**
   * @brief Acquires one free volatile page from \b local page pool.
   * @return acquired page, or 0 if no free page is available (OUTOFMEMORY).
   * @details
   * This method does not return error code to be simple and fast.
   * Instead, The caller MUST check if the returned page is zero or not.
   */
  PagePoolOffset  grab_free_volatile_page();
  /** Same, except it's for snapshot page */
  PagePoolOffset  grab_free_snapshot_page();
  /** Returns one free volatile page to \b local page pool. */
  void            release_free_volatile_page(PagePoolOffset offset);
  /** Same, except it's for snapshot page */
  void            release_free_snapshot_page(PagePoolOffset offset);

  memory::PagePool* get_volatile_pool() { return volatile_pool_; }
  memory::PagePool* get_snapshot_pool() { return snapshot_pool_; }

  const SmallThreadLocalMemoryPieces& get_small_thread_local_memory_pieces() const {
    return small_thread_local_memory_pieces_;
  }

 private:
  /** Called when there no local free pages. */
  static ErrorCode  grab_free_pages_from_node(
    PagePoolOffsetChunk* free_chunk,
    memory::PagePool *pool);
  /** Called when there are too many local free pages. */
  static void       release_free_pages_to_node(
    PagePoolOffsetChunk* free_chunk,
    memory::PagePool *pool);

  Engine* const           engine_;

  /**
   * The parent memory repository, which holds this object.
   */
  NumaNodeMemory* const   node_memory_;

  /**
   * Global ID of the NUMA core this memory is allocated for.
   */
  const foedus::thread::ThreadId core_id_;

  /**
   * Local ordinal of the NUMA core this memory is allocated for.
   */
  const thread::ThreadLocalOrdinal        core_local_ordinal_;

  /**
   * A tiny (still 2MB) thread local memory used for various things.
   * To reduce # of TLB entries, we pack several small things to this 2MB.
   * \li (used in ThreadPimpl) McsBlock(8b) * 64k : 512kb
   * \li (used in Xct) PointerAccess(16b) * 1k : 16kb
   * \li (used in Xct) PageVersionAccess(16b) * 1k : 16kb
   * \li (used in Xct) XctAccess(24b) * 32k :768kb
   * \li (used in Xct) WriteXctAccess(40b) * 8k : 320kb
   * \li (used in Xct) LockFreeWriteXctAccess(16b) * 4k : 64kb
   * In total within 2MB.
   * Depending on options (esp, xct_.max_read_set_size and max_write_set_size), this might
   * become two 2MB pages, which is not ideal. Hopefully the numbers above are sufficient.
   */
  memory::AlignedMemory   small_thread_local_memory_;
  SmallThreadLocalMemoryPieces small_thread_local_memory_pieces_;

  /**
   * @brief Holds a \b local set of pointers to free volatile pages.
   * @details
   * All page allocation/deallocation are local operations without synchronization
   * except when this chunk goes below 10% or above 90% full.
   * When it happens, we grab/release a bunch of free pages from node memory's page pool.
   * @see PagePool
   */
  PagePoolOffsetChunk*                    free_volatile_pool_chunk_;
  /** Same above, but for snapshot cache. */
  PagePoolOffsetChunk*                    free_snapshot_pool_chunk_;

  /** Pointer to this NUMA node's volatile page pool */
  PagePool*                               volatile_pool_;
  /** Pointer to this NUMA node's snapshot page pool */
  PagePool*                               snapshot_pool_;

  /** Private memory to hold log entries. */
  AlignedMemorySlice                      log_buffer_memory_;
};

/**
 * @brief Automatically invokes a page offset acquired for volatile page.
 * @ingroup MEMORY
 * @details
 * This is used in places that acquire a new volatile page offset but might have to release
 * it when there is some other error. You must NOT forget to call set_released() to avoid
 * releasing the page when no error happens.
 * Well, so maybe it's better to not use this class...
 */
struct AutoVolatilePageReleaseScope {
  AutoVolatilePageReleaseScope(NumaCoreMemory* memory, PagePoolOffset offset)
    : memory_(memory), offset_(offset), released_(false) {}
  ~AutoVolatilePageReleaseScope() {
    if (!released_) {
      memory_->release_free_volatile_page(offset_);
      released_ = true;
    }
  }
  void set_released() {
    released_ = true;
  }
  NumaCoreMemory* const memory_;
  const PagePoolOffset  offset_;
  bool                  released_;
};

}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_NUMA_CORE_MEMORY_HPP_
