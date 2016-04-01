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
#ifndef FOEDUS_MEMORY_PAGE_RESOLVER_HPP_
#define FOEDUS_MEMORY_PAGE_RESOLVER_HPP_
#include <cstring>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/thread_id.hpp"

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
    storage::Page* page = resolve_offset_newpage(offset);
    // this is NOT a new page, so we have sanity checks
    storage::assert_valid_volatile_page(page, offset);
    return page;
  }
  /** As the name suggests, this version is for new pages, which don't have sanity checks. */
  storage::Page* resolve_offset_newpage(PagePoolOffset offset) const ALWAYS_INLINE {
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
 * @brief Resolves an offset in a volatile page pool to an actual pointer and vice versa.
 * @ingroup MEMORY
 * @details
 * This class abstracts how we convert 4-byte page-pool offset plus NUMA node id to/from
 * 8-byte pointer. This method must be \b VERY efficient, thus everything is inlined.
 * This also abstracts how we access in-memory pages in other NUMA node for volatile pages.
 *
 * \b Note Note that this is only for volatile pages. As snapshot cache is per-node, there is no
 * global snapshot page resolver (just the node-local one should be enough).
 *
 * This object is copiable and the copying is moderately efficient.
 * @todo global page resolve can't cheaply provide resolve_page(). Do we need it?
 */
struct GlobalVolatilePageResolver CXX11_FINAL {
  typedef storage::Page* Base;
  enum Constants {
    kMaxNumaNode = 256,
  };
  GlobalVolatilePageResolver() : numa_node_count_(0), begin_(0), end_(0) {}
  GlobalVolatilePageResolver(const Base *bases, uint16_t numa_node_count,
             PagePoolOffset begin, PagePoolOffset end)
    : numa_node_count_(numa_node_count), begin_(begin), end_(end) {
    ASSERT_ND(numa_node_count <= kMaxNumaNode);
    std::memcpy(bases_, bases, sizeof(Base) * numa_node_count_);
  }
  // default assignment/copy constructors are a bit wasteful when #numa node is small, thus:
  GlobalVolatilePageResolver(const GlobalVolatilePageResolver &other) {
    operator=(other);
  }
  GlobalVolatilePageResolver& operator=(const GlobalVolatilePageResolver &other) {
    ASSERT_ND(other.numa_node_count_ > 0);
    ASSERT_ND(other.numa_node_count_ <= kMaxNumaNode);
    ASSERT_ND(other.end_ > other.begin_);
    numa_node_count_ = other.numa_node_count_;
    begin_ = other.begin_;
    end_ = other.end_;
    std::memcpy(bases_, other.bases_, sizeof(Base) * numa_node_count_);
    return *this;
  }

  /** Resolves offset plus NUMA node ID to storage::Page*. */
  storage::Page* resolve_offset(uint8_t numa_node, PagePoolOffset offset) const ALWAYS_INLINE {
    storage::Page* page = resolve_offset_newpage(numa_node, offset);
    // this is NOT a new page, so we have sanity checks
    storage::assert_valid_volatile_page(page, offset);
    ASSERT_ND(storage::construct_volatile_page_pointer(page->get_header().page_id_).
      components.numa_node == numa_node);
    return page;
  }
  /** Resolves volatile page ID to storage::Page*. */
  storage::Page* resolve_offset(storage::VolatilePagePointer page_pointer) const ALWAYS_INLINE {
    return resolve_offset(page_pointer.components.numa_node, page_pointer.get_offset());
  }

  /** As the name suggests, this version is for new pages, which don't have sanity checks. */
  storage::Page* resolve_offset_newpage(
    uint8_t numa_node,
    PagePoolOffset offset) const ALWAYS_INLINE {
    ASSERT_ND(numa_node < numa_node_count_);
    ASSERT_ND(offset >= begin_);
    ASSERT_ND(offset < end_);
    storage::assert_aligned_page(bases_[numa_node] + offset);
    return bases_[numa_node] + offset;
  }
  /** As the name suggests, this version is for new pages, which don't have sanity checks. */
  storage::Page* resolve_offset_newpage(
    storage::VolatilePagePointer page_pointer) const ALWAYS_INLINE {
    return resolve_offset_newpage(
      page_pointer.components.numa_node,
      page_pointer.get_offset());
  }

  /** number of NUMA nodes in this engine. */
  uint16_t        numa_node_count_;
  /** where a valid page entry starts. only for sanity checking. */
  PagePoolOffset  begin_;
  /** where a valid page entry ends. only for sanity checking. */
  PagePoolOffset  end_;

  /** base address to calculate from/to offset. index is NUMA node ID. */
  Base            bases_[kMaxNumaNode];
};

}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_PAGE_RESOLVER_HPP_
