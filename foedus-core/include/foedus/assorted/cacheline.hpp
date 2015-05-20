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
#ifndef FOEDUS_ASSORTED_CACHELINE_HPP_
#define FOEDUS_ASSORTED_CACHELINE_HPP_

#include <stdint.h>

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
#if defined(__GNUC__)
#if defined(__aarch64__)
  ::__builtin_prefetch(address, 1, 3);
#else  // defined(__aarch64__)
  ::__builtin_prefetch(address, 1, 3);
  // ::_mm_prefetch(address, ::_MM_HINT_T0);
#endif  // defined(__aarch64__)
#endif  // defined(__GNUC__)
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
    prefetch_cacheline(shifted);
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
    prefetch_cacheline(shifted);  // this also works for L2/L3
  }
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_CACHELINE_HPP_
