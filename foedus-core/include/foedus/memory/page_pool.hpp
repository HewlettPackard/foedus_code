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
#ifndef FOEDUS_MEMORY_PAGE_POOL_HPP_
#define FOEDUS_MEMORY_PAGE_POOL_HPP_
#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace memory {
/**
 * @brief To reduce the overhead of grabbing/releasing pages from pool, we pack
 * this many pointers for each grab/release.
 * @ingroup MEMORY
 * @details
 * The pointers themselves might not be contiguous. This is just a package of pointers.
 */
class PagePoolOffsetChunk {
 public:
  enum Constants {
    /**
     * Max number of pointers to pack.
     * -1 for size_ (make the size of this class power of two).
     */
    kMaxSize = (1 << 12) - 1,
  };
  PagePoolOffsetChunk() : size_(0) {}

  uint32_t                capacity() const { return kMaxSize; }
  uint32_t                size()  const   { return size_; }
  bool                    empty() const   { return size_ == 0; }
  bool                    full()  const   { return size_ == kMaxSize; }
  void                    clear()         { size_ = 0; }

  PagePoolOffset          pop_back() {
    ASSERT_ND(!empty());
    return chunk_[--size_];
  }
  void                    push_back(PagePoolOffset pointer) {
    ASSERT_ND(!full());
    chunk_[size_++] = pointer;
  }
  void                    push_back(const PagePoolOffset* begin, const PagePoolOffset* end);
  void                    move_to(PagePoolOffset* destination, uint32_t count);

 private:
  uint32_t        size_;
  PagePoolOffset  chunk_[kMaxSize];
};

/** Used to point to an already existing array. Everything must be const in this case. */
class PagePoolOffsetDynamicChunk {
 public:
  PagePoolOffsetDynamicChunk(uint32_t size, PagePoolOffset* chunk)
    : size_(size), padding_(0), chunk_(chunk) {}

  uint32_t                capacity() const { return size_; }
  uint32_t                size()  const   { return size_; }
  bool                    empty() const   { return size_ == 0; }
  bool                    full()  const   { return true; }

  void                    move_to(PagePoolOffset* destination, uint32_t count) const;

 private:
  const uint32_t        size_;
  const uint32_t        padding_;
  PagePoolOffset* const chunk_;  // of arbitrary size
};

/**
 * @brief Used to store an epoch value with each entry in PagePoolOffsetChunk.
 * @ingroup MEMORY
 * @details
 * This is used where the page offset can be passed around after some epoch, for example
 * "retired" pages that must be kept intact until next-next epoch.
 */
class PagePoolOffsetAndEpochChunk {
 public:
  enum Constants {
    /**
     * Max number of pointers to pack.
     * We use this object to pool retired pages, and we observed lots of waits due to
     * full pool that causes the thread to wait for a new epoch.
     * To avoid that, we now use a much larger kMaxSize than PagePoolOffsetChunk.
     * Yes, it means much larger memory consumption in NumaCoreMemory, but shouldn't be
     * a big issue.
     * 8 * 2^16 * nodes * threads. On 16-node/12 threads-per-core (DH), 96MB per node.
     * On 4-node/12 (DL580), 24 MB per node. I'd say negligible.
     */
    kMaxSize = (1 << 16) - 1,
  };
  struct OffsetAndEpoch {
    PagePoolOffset      offset_;
    Epoch::EpochInteger safe_epoch_;
  };
  PagePoolOffsetAndEpochChunk() : size_(0) {}

  uint32_t                capacity() const { return kMaxSize; }
  uint32_t                size()  const   { return size_; }
  bool                    empty() const   { return size_ == 0; }
  bool                    full()  const   { return size_ == kMaxSize; }
  void                    clear()         { size_ = 0; }
  bool                    is_sorted() const;  // only for assertion

  /**
   * @pre empty() || Epoch(chunk_[size_ - 1U].safe_epoch_) <= safe_epoch, meaning
   * you cannot specify safe_epoch below what you have already specified.
   */
  void                    push_back(PagePoolOffset offset, const Epoch& safe_epoch) {
    ASSERT_ND(!full());
    ASSERT_ND(empty() || Epoch(chunk_[size_ - 1U].safe_epoch_) <= safe_epoch);
    chunk_[size_].offset_ = offset;
    chunk_[size_].safe_epoch_ = safe_epoch.value();
    ++size_;
  }
  /** Note that the destination is PagePoolOffset* because that's the only usecase. */
  void                    move_to(PagePoolOffset* destination, uint32_t count);
  /**
   * Returns the number of offsets (always from index-0) whose safe_epoch_ is \e strictly-before
   * the given epoch. This method does binary search assuming that chunk_ is sorted by safe_epoch_.
   * @param[in] threshold epoch that is deemed as unsafe to return.
   * @return number of offsets whose safe_epoch_ < threshold
   */
  uint32_t                get_safe_offset_count(const Epoch& threshold) const;

 private:
  uint32_t        size_;
  uint32_t        dummy_;
  OffsetAndEpoch  chunk_[kMaxSize];
};

/**
 * @brief Page pool for volatile read/write store (VolatilePage) and
 * the read-only bufferpool (SnapshotPage).
 * @ingroup MEMORY
 * @details
 * The engine maintains a few instances of this class; one for each NUMA node.
 * Each page pool provides in-memory pages for both volatile pages and snapshot pages.
 */
class PagePool CXX11_FINAL : public virtual Initializable {
 public:
  struct Stat {
    uint64_t total_pages_;
    uint64_t allocated_pages_;
  };

  PagePool();
  ~PagePool();
  void attach(PagePoolControlBlock* control_block, void* memory, uint64_t memory_size, bool owns);

  // Disable default constructors
  PagePool(const PagePool&) CXX11_FUNC_DELETE;
  PagePool& operator=(const PagePool&) CXX11_FUNC_DELETE;

  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  storage::Page*        get_base() const;
  uint64_t              get_memory_size() const;
  Stat                  get_stat() const;
  std::string           get_debug_pool_name() const;
  /** Call this anytime after attach() */
  void                  set_debug_pool_name(const std::string& name);

  /**
   * @brief Adds the specified number of free pages to the chunk.
   * @param[in] desired_grab_count we grab this number of free pages at most
   * @param[in,out] chunk we \e append the grabbed free pages to this chunk
   * @pre chunk->size() + desired_grab_count <= chunk->capacity()
   * @return only OUTOFMEMORY is possible
   * @details
   * Callers usually maintain one PagePoolOffsetChunk for its private use and
   * calls this method when the size() goes below some threshold (eg 10%)
   * so as to get size() about 50%.
   */
  ErrorCode   grab(uint32_t desired_grab_count, PagePoolOffsetChunk *chunk);
  /**
   * Grab only one page. More expensive, but handy in some situation.
   */
  ErrorCode   grab_one(PagePoolOffset *offset);

  /**
   * @brief Returns the specified number of free pages from the chunk.
   * @param[in] desired_release_count we release this number of free pages
   * @param[in,out] chunk we release free pages from the tail of this chunk
   * @pre chunk->size() - desired_release_count >= 0
   * @details
   * Callers usually maintain one PagePoolOffsetChunk for its private use and
   * calls this method when the size() goes above some threshold (eg 90%)
   * so as to get size() about 50%.
   */
  void        release(uint32_t desired_release_count, PagePoolOffsetChunk* chunk);
  void        release(uint32_t desired_release_count, PagePoolOffsetDynamicChunk* chunk);

  /** Overload for PagePoolOffsetAndEpochChunk. */
  void        release(uint32_t desired_release_count, PagePoolOffsetAndEpochChunk* chunk);

  /**
   * Returns only one page. More expensive, but handy in some situation.
   */
  void        release_one(PagePoolOffset offset);

  /**
   * Gives an object to resolve an offset in \e this page pool (thus \e local) to an actual
   * pointer and vice versa.
   */
  const LocalPageResolver& get_resolver() const;

  friend std::ostream& operator<<(std::ostream& o, const PagePool& v);

 private:
  PagePoolPimpl *pimpl_;
};

/**
 * @brief A helper class to return a bunch of pages to individual nodes.
 * @ingroup MEMORY
 * @details
 * This returns a large number of pages much more efficiently than returning one-by-one,
 * especially if the in-memory pages might come from different NUMA nodes.
 * So far this is used when we shutdown the engine or drop a storage.
 */
class PageReleaseBatch CXX11_FINAL {
 public:
  typedef PagePoolOffsetChunk* ChunkPtr;
  explicit PageReleaseBatch(Engine* engine);
  ~PageReleaseBatch();

  // Disable default constructors
  PageReleaseBatch() CXX11_FUNC_DELETE;
  PageReleaseBatch(const PageReleaseBatch&) CXX11_FUNC_DELETE;
  PageReleaseBatch& operator=(const PageReleaseBatch&) CXX11_FUNC_DELETE;

  /**
   * @brief Returns the given in-memory volatile page to appropriate NUMA node.
   * @details
   * This internally batches the pages to return. At the end, call release_all() to flush it.
   */
  void        release(storage::VolatilePagePointer page_id) {
    release(page_id.components.numa_node, page_id.components.offset);
  }

  /**
   * @brief Returns the given in-memory page to the specified NUMA node.
   * @details
   * This internally batches the pages to return. At the end, call release_all() to flush it.
   */
  void        release(thread::ThreadGroupId numa_node, PagePoolOffset offset);

  /**
   * Return all batched pages in the given chunk to its pool.
   */
  void        release_chunk(thread::ThreadGroupId numa_node);

  /**
   * Called at the end to return all batched pages to their pools.
   */
  void        release_all();

 private:
  Engine* const   engine_;
  const uint16_t  numa_node_count_;
  /** index is thread::ThreadGroupId numa_node. */
  ChunkPtr        chunks_[256];
};

/**
 * @brief A helper class to grab a bunch of pages from multiple nodes in round-robin fashion
 * per chunk.
 * @ingroup MEMORY
 * @details
 * This grabs a large number of pages much more efficiently than grabbing one-by-one from
 * individual node. Instead, this is not a true round-robin per page because we switch node
 * only per chunk, which is actually an advantage as it achieves contiguous memory that is
 * friendly for CPU cache.
 * So far this is used when create a new array storage, which requires to grab a large number of
 * pages in round-robin fashion to balance memory usage.
 */
class RoundRobinPageGrabBatch CXX11_FINAL {
 public:
  explicit RoundRobinPageGrabBatch(Engine* engine);
  ~RoundRobinPageGrabBatch();

  // Disable default constructors
  RoundRobinPageGrabBatch() CXX11_FUNC_DELETE;
  RoundRobinPageGrabBatch(const RoundRobinPageGrabBatch&) CXX11_FUNC_DELETE;
  RoundRobinPageGrabBatch& operator=(const RoundRobinPageGrabBatch&) CXX11_FUNC_DELETE;

  /**
   * @brief Grabs an in-memory page in some NUMA node.
   * @details
   * This internally batches the pages to return. At the end, call release_all() to release
   * unused pages.
   * Although the type of returned value is VolatilePagePointer, it can be used for
   * snapshot page, too. I just want to return both NUMA node and offset.
   * Maybe we should have another typedef for it.
   * @todo this so far doesn't handle out-of-memory case gracefully. what to do..
   */
  storage::VolatilePagePointer grab();

  /**
   * Called at the end to return all \e remaining pages to their pools.
   * The grabbed pages are not returned, of course.
   */
  void        release_all();

 private:
  Engine* const           engine_;
  const uint16_t          numa_node_count_;
  thread::ThreadGroupId   current_node_;
  PagePoolOffsetChunk     chunk_;
};

/**
 * @brief A helper class to grab a bunch of pages from multiple nodes in arbitrary fashion.
 * @ingroup MEMORY
 * @details
 * Similar to RoundRobinPageGrabBatch.
 * This class doesn't specify how nodes divvy up pages. The caller has the control.
 * Or, this class provides a method for each policy.
 */
class DivvyupPageGrabBatch CXX11_FINAL {
 public:
  explicit DivvyupPageGrabBatch(Engine* engine);
  ~DivvyupPageGrabBatch();

  // Disable default constructors
  DivvyupPageGrabBatch() CXX11_FUNC_DELETE;
  DivvyupPageGrabBatch(const DivvyupPageGrabBatch&) CXX11_FUNC_DELETE;
  DivvyupPageGrabBatch& operator=(const DivvyupPageGrabBatch&) CXX11_FUNC_DELETE;

  /**
   * @brief Grabs an in-memory page in specified NUMA node.
   * @param[in] node specify NUMA node
   * @pre node < node_count_
   */
  storage::VolatilePagePointer grab(thread::ThreadGroupId node);

  /**
   * @brief Grabs an in-memory page evenly and contiguously from each NUMA node.
   * @param[in] cur page index out of total.
   * @param[in] total total pages to be allocated.
   * @details
   * For example, if total=6 and there are 2 numa nodes, cur=0,1,2 get pages from
   * NUMA node 0, cur=3,4,5 get pages from NUMA node 1.
   * This is the most simple policy.
   */
  storage::VolatilePagePointer grab_evenly(uint64_t cur, uint64_t total);

  /**
   * Called at the end to return all \e remaining pages to their pools.
   * The grabbed pages are not returned, of course.
   */
  void        release_all();

 private:
  Engine* const           engine_;
  const uint16_t          node_count_;
  /** Chunk for each node. index is node ID. */
  PagePoolOffsetChunk*    chunks_;
};


STATIC_SIZE_CHECK(sizeof(PagePoolOffset), 4)
// Size of PagePoolOffsetChunk should be power of two (<=> x & (x-1) == 0)
STATIC_SIZE_CHECK(sizeof(PagePoolOffsetChunk) & (sizeof(PagePoolOffsetChunk) - 1), 0)
STATIC_SIZE_CHECK(sizeof(PagePoolOffsetChunk) * 32U, sizeof(PagePoolOffsetAndEpochChunk))

}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_PAGE_POOL_HPP_
