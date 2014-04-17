/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_PAGE_POOL_HPP_
#define FOEDUS_MEMORY_PAGE_POOL_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/memory/memory_id.hpp>
#include <foedus/memory/page_resolver.hpp>
#include <foedus/storage/page.hpp>
#include <stdint.h>
#include <cassert>
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
        MAX_SIZE = (1 << 12) - 1,
    };
    PagePoolOffsetChunk() : size_(0) {}

    uint32_t                capacity() const { return MAX_SIZE; }
    uint32_t                size()  const   { return size_; }
    bool                    empty() const   { return size_ == 0; }
    bool                    full()  const   { return size_ == MAX_SIZE; }
    void                    clear()         { size_ = 0; }

    PagePoolOffset&         operator[](uint32_t index) {
        assert(index < size_);
        return chunk_[index];
    }
    PagePoolOffset          operator[](uint32_t index) const {
        assert(index < size_);
        return chunk_[index];
    }
    PagePoolOffset          pop_back() {
        assert(!empty());
        return chunk_[--size_];
    }
    void                    push_back(PagePoolOffset pointer) {
        assert(!full());
        chunk_[size_++] = pointer;
    }
    void                    push_back(const PagePoolOffset* begin, const PagePoolOffset* end);
    void                    move_to(PagePoolOffset* destination, uint32_t count);

 private:
    uint32_t        size_;
    PagePoolOffset  chunk_[MAX_SIZE];
};
const int _dummy1 = assorted::static_size_check<sizeof(PagePoolOffset), 4>();
// Size of PagePoolOffsetChunk should be power of two (<=> x & (x-1) == 0)
const int _dummy2 = assorted::static_size_check<
    sizeof(PagePoolOffsetChunk) & (sizeof(PagePoolOffsetChunk) - 1), 0>();

/**
 * @brief Page pool for volatile read/write store (VolatilePage) and
 * the read-only bufferpool (SnapshotPage).
 * @ingroup MEMORY
 * @details
 */
class PagePool CXX11_FINAL : public virtual Initializable {
 public:
    explicit PagePool(Engine* engine);
    ~PagePool();

    // Disable default constructors
    PagePool() CXX11_FUNC_DELETE;
    PagePool(const PagePool&) CXX11_FUNC_DELETE;
    PagePool& operator=(const PagePool&) CXX11_FUNC_DELETE;

    ErrorStack  initialize() CXX11_OVERRIDE;
    bool        is_initialized() const CXX11_OVERRIDE;
    ErrorStack  uninitialize() CXX11_OVERRIDE;

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
     * Gives an object to resolve an offset in page pool to an actual pointer and vice versa.
     */
    PageResolver    get_resolver() const;

 private:
    PagePoolPimpl *pimpl_;
};
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_PAGE_POOL_HPP_
