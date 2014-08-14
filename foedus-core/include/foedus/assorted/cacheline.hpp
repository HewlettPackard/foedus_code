/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ASSORTED_CACHELINE_HPP_
#define FOEDUS_ASSORTED_CACHELINE_HPP_

#include <stdint.h>
#include <xmmintrin.h>

#include "foedus/compiler.hpp"

/**
 * @file foedus/assorted/cacheline.hpp
 * @ingroup ASSORTED
 * @brief Constants and methods related to CPU cacheline and its prefetching.
 */

namespace foedus {
namespace assorted {

/**
 * @brief Byte count of one cache line.
 * @ingroup ASSORTED
 * @details
 * Several places use this to avoid false sharing of cache lines, for example separating
 * two variables that are frequently accessed with atomic requirements.
 * @todo This should be automatically detected by cmakedefine.
 */
const uint16_t kCachelineSize = 64;

/**
 * @brief Prefetch one cacheline to L1 cache.
 * @param[in] address memory address to prefetch.
 * @ingroup ASSORTED
 */
inline void prefetch_cacheline(const void* address) {
  ::_mm_prefetch(address, ::_MM_HINT_T0);
}

/**
 * @brief Prefetch multiple contiguous cachelines to L1 cache.
 * @param[in] address memory address to prefetch.
 * @param[in] cacheline_count count of cachelines to prefetch.
 * @ingroup ASSORTED
 */
inline void prefetch_cachelines(const void* address, int cacheline_count) {
  for (int i = 0; i < cacheline_count; ++i) {
    const void* shifted = reinterpret_cast<const char*>(address) + kCachelineSize * cacheline_count;
    ::_mm_prefetch(shifted, ::_MM_HINT_T0);
  }
}

/**
 * @brief Prefetch multiple contiguous cachelines to L2/L3 cache.
 * @param[in] address memory address to prefetch.
 * @param[in] cacheline_count count of cachelines to prefetch.
 * @ingroup ASSORTED
 */
inline void prefetch_l2(const void* address, int cacheline_count) {
  for (int i = 0; i < cacheline_count; ++i) {
    const void* shifted = reinterpret_cast<const char*>(address) + kCachelineSize * cacheline_count;
    ::_mm_prefetch(shifted, ::_MM_HINT_T1);
  }
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_CACHELINE_HPP_
