/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_PAGE_PREFETCH_HPP_
#define FOEDUS_STORAGE_PAGE_PREFETCH_HPP_

#include "foedus/assorted/cacheline.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace storage {
/**
 * @brief Prefetch a page to L2/L3 cache.
 * @param[in] page page to prefetch.
 * @ingroup STORAGE
 * @details
 * In order to prefetch a specific part of the page to L1,
 * directly use foedus::assorted::prefetch_cacheline().
 */
inline void prefetch_page_l2(const void* page) {
  assorted::prefetch_l2(page, kPageSize / assorted::kCachelineSize);
}

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_PAGE_PREFETCH_HPP_
