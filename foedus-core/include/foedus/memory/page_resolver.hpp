/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_PAGE_RESOLVER_HPP_
#define FOEDUS_MEMORY_PAGE_RESOLVER_HPP_
#include <foedus/compiler.hpp>
#include <foedus/cxx11.hpp>
#include <foedus/memory/memory_id.hpp>
#include <foedus/storage/page.hpp>
#include <cassert>
namespace foedus {
namespace memory {
/**
 * @brief Resolves an offset in page pool to an actual pointer and vice versa.
 * @ingroup MEMORY
 * @details
 * This class abstracts how we convert 4-byte offset and 8-byte pointer.
 * This method must be \b VERY efficient, thus everything is inlined.
 * Also, we need this abstraction because we might later have multiple base-addresses
 * to better utilize NUMA memory hierarchy.
 * This object is copiable and the copying is very efficient.
 * This object currently is and forever will be very cheap to copy.
 */
class PageResolver CXX11_FINAL {
 public:
    PageResolver() : base_(CXX11_NULLPTR), begin_(0), end_(0) {}
    PageResolver(storage::Page* base, PagePoolOffset begin, PagePoolOffset end)
        : base_(base), begin_(begin), end_(end) {}

    /** Resolves storage::Page* to offset in this pool. */
    ALWAYS_INLINE PagePoolOffset resolve_page(storage::Page *page) const {
        PagePoolOffset offset = page - base_;
        assert(offset >= begin_);
        assert(offset < end_);
        return offset;
    }
    /** Resolves offset in this pool to storage::Page*. */
    ALWAYS_INLINE storage::Page* resolve_offset(PagePoolOffset offset) const {
        assert(offset >= begin_);
        assert(offset < end_);
        return base_ + offset;
    }

 private:
    storage::Page*  base_;
    PagePoolOffset  begin_;
    PagePoolOffset  end_;
};

}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_PAGE_RESOLVER_HPP_
