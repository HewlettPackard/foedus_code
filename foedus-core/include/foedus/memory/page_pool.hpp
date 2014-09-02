/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_PAGE_POOL_HPP_
#define FOEDUS_MEMORY_PAGE_POOL_HPP_
#include <stdint.h>

#include <iosfwd>

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

  PagePoolOffset&         operator[](uint32_t index) {
    ASSERT_ND(index < size_);
    return chunk_[index];
  }
  PagePoolOffset          operator[](uint32_t index) const {
    ASSERT_ND(index < size_);
    return chunk_[index];
  }
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
STATIC_SIZE_CHECK(sizeof(PagePoolOffset), 4)
// Size of PagePoolOffsetChunk should be power of two (<=> x & (x-1) == 0)
STATIC_SIZE_CHECK(sizeof(PagePoolOffsetChunk) & (sizeof(PagePoolOffsetChunk) - 1), 0)

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

  PagePool(uint64_t memory_byte_size, bool shared);
  ~PagePool();

  // Disable default constructors
  PagePool() CXX11_FUNC_DELETE;
  PagePool(const PagePool&) CXX11_FUNC_DELETE;
  PagePool& operator=(const PagePool&) CXX11_FUNC_DELETE;

  /** separated to avoid passing un-consructed object's pointer. */
  void        initialize_parent(NumaNodeMemory* parent);
  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  uint64_t              get_memory_byte_size() const;
  thread::ThreadGroupId get_numa_node() const;
  Stat                  get_stat() const;

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
  void        release(uint32_t desired_release_count, PagePoolOffsetChunk *chunk);
  /**
   * Returns only one page. More expensive, but handy in some situation.
   */
  void        release_one(PagePoolOffset offset);

  /**
   * Gives an object to resolve an offset in \e this page pool (thus \e local) to an actual
   * pointer and vice versa.
   */
  LocalPageResolver&  get_resolver();

  /** Returns the underlying memory of the pool. */
  const AlignedMemory& get_memory() const;

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

}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_PAGE_POOL_HPP_
