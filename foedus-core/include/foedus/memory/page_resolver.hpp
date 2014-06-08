/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_PAGE_RESOLVER_HPP_
#define FOEDUS_MEMORY_PAGE_RESOLVER_HPP_
#include <foedus/compiler.hpp>
#include <foedus/cxx11.hpp>
#include <foedus/storage/page.hpp>
#include <foedus/storage/storage_id.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/assert_nd.hpp>
#include <cstring>
namespace foedus {
namespace memory {
/**
 * @brief Resolves an offset in \e local (same NUMA node) page pool to a pointer and vice versa.
 * @ingroup MEMORY
 * @details
 * This class abstracts how we convert 4-byte page-pool offset to/from 8-byte pointer for cases
 * where we are handling a page that resides in a local page pool.
 * Especially, snapshot pages and snapshotting can always use this page resolver.
 *
 * This object is copiable and the copying is very efficient.
 * The size of this object is only 16 bytes.
 */
struct LocalPageResolver CXX11_FINAL {
    LocalPageResolver() : begin_(0), end_(0), base_(CXX11_NULLPTR) {}
    LocalPageResolver(storage::Page* base, PagePoolOffset begin, PagePoolOffset end)
        : begin_(begin), end_(end), base_(base) {}

    // default assignment/copy-constructor is enough

    /** Resolves storage::Page* to offset in this pool. */
    PagePoolOffset resolve_page(storage::Page *page) const ALWAYS_INLINE {
        PagePoolOffset offset = page - base_;
        ASSERT_ND(offset >= begin_);
        ASSERT_ND(offset < end_);
        return offset;
    }
    /** Resolves offset in this pool to storage::Page*. */
    storage::Page* resolve_offset(PagePoolOffset offset) const ALWAYS_INLINE {
        ASSERT_ND(offset >= begin_);
        ASSERT_ND(offset < end_);
        return base_ + offset;
    }

    /** where a valid page entry starts. only for sanity checking. */
    PagePoolOffset      begin_;
    /** where a valid page entry ends. only for sanity checking. */
    PagePoolOffset      end_;
    /** base address to calculate from/to offset. */
    storage::Page*      base_;
};

/**
 * @brief Resolves an offset in a page pool to an actual pointer and vice versa.
 * @ingroup MEMORY
 * @details
 * This class abstracts how we convert 4-byte page-pool offset plus NUMA node id to/from
 * 8-byte pointer. This method must be \b VERY efficient, thus everything is inlined.
 * This also abstracts how we access in-memory pages in other NUMA node for volatile pages.
 *
 * This object is copiable and the copying is moderately efficient.
 * @todo global page resolve can't cheaply provide resolve_page(). Do we need it?
 */
struct GlobalPageResolver CXX11_FINAL {
    typedef storage::Page* Base;
    enum Constants {
        MAX_NUMA_NODE = 256,
    };
    GlobalPageResolver() : numa_node_count_(0), begin_(0), end_(0) {}
    GlobalPageResolver(const Base *bases, uint16_t numa_node_count,
                       PagePoolOffset begin, PagePoolOffset end)
        : numa_node_count_(numa_node_count), begin_(begin), end_(end) {
        ASSERT_ND(numa_node_count <= MAX_NUMA_NODE);
        std::memcpy(bases_, bases, sizeof(Base) * numa_node_count_);
    }
    // default assignment/copy constructors are a bit wasteful when #numa node is small, thus:
    GlobalPageResolver(const GlobalPageResolver &other) {
        operator=(other);
    }
    GlobalPageResolver& operator=(const GlobalPageResolver &other) {
        numa_node_count_ = other.numa_node_count_;
        begin_ = other.begin_;
        end_ = other.end_;
        std::memcpy(bases_, other.bases_, sizeof(Base) * numa_node_count_);
        return *this;
    }

    /** Resolves offset plus NUMA node ID to storage::Page*. */
    storage::Page* resolve_offset(uint8_t numa_node, PagePoolOffset offset) const ALWAYS_INLINE {
        ASSERT_ND(numa_node < numa_node_count_);
        ASSERT_ND(offset >= begin_);
        ASSERT_ND(offset < end_);
        return bases_[numa_node] + offset;
    }
    /** Resolves volatile page ID to storage::Page*. */
    storage::Page* resolve_offset(storage::VolatilePagePointer page_pointer) const ALWAYS_INLINE {
        return resolve_offset(page_pointer.components.numa_node, page_pointer.components.offset);
    }

    /** number of NUMA nodes in this engine. */
    uint16_t        numa_node_count_;
    /** where a valid page entry starts. only for sanity checking. */
    PagePoolOffset  begin_;
    /** where a valid page entry ends. only for sanity checking. */
    PagePoolOffset  end_;

    /** base address to calculate from/to offset. index is NUMA node ID. */
    Base            bases_[MAX_NUMA_NODE];
};

}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_PAGE_RESOLVER_HPP_
