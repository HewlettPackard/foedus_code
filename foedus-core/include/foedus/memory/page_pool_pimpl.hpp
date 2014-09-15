/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_PAGE_POOL_PIMPL_HPP_
#define FOEDUS_MEMORY_PAGE_POOL_PIMPL_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace memory {
/** Shared data in PagePoolPimpl. */
struct PagePoolControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  PagePoolControlBlock() = delete;
  ~PagePoolControlBlock() = delete;

  void initialize() {
    lock_.initialize();
  }
  void uninitialize() {
    lock_.uninitialize();
  }

  /** Inclusive head of the circular queue. Be aware of wrapping around. */
  uint64_t                        free_pool_head_;
  /** Number of free pages. */
  uint64_t                        free_pool_count_;

  /**
   * grab()/release() are protected with this lock.
   * This lock is not contentious at all because we pack many pointers in a chunk.
   */
  soc::SharedMutex                lock_;
};

/**
 * @brief Pimpl object of PagePool.
 * @ingroup MEMORY
 * @details
 * A private pimpl object for PagePool.
 * Do not include this header from a client program unless you know what you are doing.
 */
class PagePoolPimpl final : public DefaultInitializable {
 public:
  PagePoolPimpl();
  void attach(PagePoolControlBlock* control_block, void* memory, uint64_t memory_size, bool owns);
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ErrorCode           grab(uint32_t desired_grab_count, PagePoolOffsetChunk *chunk);
  void                release(uint32_t desired_release_count, PagePoolOffsetChunk *chunk);
  ErrorCode           grab_one(PagePoolOffset *offset);
  void                release_one(PagePoolOffset offset);
  LocalPageResolver&  get_resolver() { return resolver_; }
  PagePool::Stat      get_stat() const;
  uint64_t&           free_pool_head() { return control_block_->free_pool_head_;}
  uint64_t            free_pool_head() const { return control_block_->free_pool_head_;}
  uint64_t&           free_pool_count() { return control_block_->free_pool_count_;}
  uint64_t            free_pool_count() const { return control_block_->free_pool_count_;}

  friend std::ostream& operator<<(std::ostream& o, const PagePoolPimpl& v);

  PagePoolControlBlock*           control_block_;

  /** The whole memory region of the pool. */
  void*                           memory_;

  /** Byte size of this page pool. */
  uint64_t                        memory_size_;

  /** Whether this engine owns this page pool. If not, initialize just attaches */
  bool                            owns_;

  /**
   * An object to resolve an offset in \e this page pool (thus \e local) to an actual
   * pointer and vice versa.
   */
  LocalPageResolver               resolver_;

  /** Just an auxiliary variable to the beginning of the pool. Same as memory_.get_block(). */
  storage::Page*                  pool_base_;

  /** Just an auxiliary variable of the size of pool. Same as memory_byte_size/kPageSize. */
  uint64_t                        pool_size_;

  /**
   * This many first pages are used for free page maintainance.
   * This also means that "Page-0" never appears in our engine, thus we can use offset=0 as null.
   * In other words, all offsets grabbed/released should be equal or larger than this value.
   * Immutable once initialized.
   */
  uint64_t                        pages_for_free_pool_;

  /**
   * We maintain free pages as a simple circular queue.
   * We append new/released pages to tail while we eat from head.
   */
  PagePoolOffset*                 free_pool_;
  /** Size of free_pool_. Immutable once initialized. */
  uint64_t                        free_pool_capacity_;
};
static_assert(
  sizeof(PagePoolControlBlock) <= soc::NodeMemoryAnchors::kPagePoolMemorySize,
  "PagePoolControlBlock is too large.");
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_PAGE_POOL_PIMPL_HPP_
