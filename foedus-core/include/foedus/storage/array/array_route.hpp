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
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_ROUTE_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_ROUTE_HPP_

#include <stdint.h>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/const_div.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/array/array_id.hpp"

namespace foedus {
namespace storage {
namespace array {
/**
 * @brief Compactly represents the route to reach the given offset.
 * @ingroup ARRAY
 * @details
 * Fanout cannot exceed 256 (as empty-payload is not allowed, minimal entry size is 16 bytes
 * in both leaf and interior, 4096/16=256), uint8_t is enough to represent the route.
 * Also, interior page always has a big fanout close to 256, so 8 levels are more than enough.
 */
union LookupRoute {
  /** This is a 64bit data. */
  uint64_t word;
  /**
   * [0] means record ordinal in leaf, [1] in its parent page, [2]...
   * [levels - 1] is the ordinal in root page.
   */
  uint8_t route[8];

  ArrayRange calculate_page_range(
    uint8_t page_level,
    uint8_t total_levels,
    uint16_t payload_size,
    ArrayOffset array_size) const;

  bool operator==(const LookupRoute& rhs) const { return word == rhs.word; }
  bool operator!=(const LookupRoute& rhs) const { return word != rhs.word; }
  bool operator<(const LookupRoute& rhs) const {
    for (uint16_t i = 1; i <= 8U; ++i) {
      // compare the higher level first. if the machine is big-endian, we can just compare word.
      // but, this method is not used in performance-sensitive place, so let's be explicit.
      if (route[8U - i] != rhs.route[8U - i]) {
        return route[8U - i] < rhs.route[8U - i];
      }
    }
    return false;
  }
  bool operator<=(const LookupRoute& rhs) const { return *this == rhs || *this < rhs; }
};

inline uint16_t to_records_in_leaf(uint16_t payload_size) {
  return kDataSize / (assorted::align8(payload_size) + kRecordOverhead);
}

/**
 * @brief Packages logic and required properties to calculate LookupRoute in array storage
 * from offset.
 * @ingroup ARRAY
 * @details
 * This class is completely header-only, immutable, and also a POD.
 * @par Optimization
 * This class uses efficient division to determine the route.
 * We originally used simple std::lldiv(), which caused 20% of CPU cost in read-only
 * experiment. wow. The code below now costs only 6%.
 */
class LookupRouteFinder {
 public:
  LookupRouteFinder()
  : levels_(0),
    records_in_leaf_(0),
    leaf_fanout_div_(1),
    interior_fanout_div_(1) {
  }
  LookupRouteFinder(uint8_t levels,  uint16_t payload_size)
    : levels_(levels),
      records_in_leaf_(to_records_in_leaf(payload_size)),
      leaf_fanout_div_(records_in_leaf_),
      interior_fanout_div_(kInteriorFanout) {
  }

  LookupRoute find_route(ArrayOffset offset) const ALWAYS_INLINE;

  /**
   * @brief find_route() plus calculates where page switches.
   * @param[in] offset array offset
   * @param[out] page_starts minimal offset that belongs to the same page as the given offset
   * @param[out] page_ends minimal offset that belongs to a page different from the given offset
   * @invariant page_starts <= offset < page_ends
   * @invariant (page_starts / records_in_leaf_) == (offset / records_in_leaf_)
   * @invariant (page_ends / records_in_leaf_) == (offset / records_in_leaf_) + 1
   * @invariant page_starts % records_in_leaf_ == 0 && page_ends % records_in_leaf_ == 0
   * @details
   * Using the additional outputs, the caller can avoid re-calculating route if the offset
   * is within page_starts and page_ends. This is currently used in ArrayComposer.
   */
  LookupRoute find_route_and_switch(
    ArrayOffset offset,
    ArrayOffset *page_starts,
    ArrayOffset *page_ends) const ALWAYS_INLINE;

  uint8_t     get_levels() const ALWAYS_INLINE { return levels_; }
  uint16_t    get_records_in_leaf() const ALWAYS_INLINE { return records_in_leaf_; }

 private:
  uint8_t                 levels_;
  /** Number of records in leaf page. */
  uint16_t                records_in_leaf_;
  /** ConstDiv(records_in_leaf_) to speed up integer division in lookup(). */
  assorted::ConstDiv      leaf_fanout_div_;
  /** ConstDiv(kInteriorFanout) to speed up integer division in lookup(). */
  assorted::ConstDiv      interior_fanout_div_;
};

inline LookupRoute LookupRouteFinder::find_route(ArrayOffset offset) const {
  LookupRoute ret;
  ret.word = 0;
  ArrayOffset old = offset;
  offset = leaf_fanout_div_.div64(offset);
  ret.route[0] = old - offset * records_in_leaf_;
  for (uint8_t level = 1; level < levels_ - 1; ++level) {
    old = offset;
    offset = interior_fanout_div_.div64(offset);
    ret.route[level] = old - offset * kInteriorFanout;
  }
  if (levels_ > 1) {
    // the last level is done manually because we don't need any division there
    ASSERT_ND(offset < kInteriorFanout);
    ret.route[levels_ - 1] = offset;
  }

  return ret;
}

inline LookupRoute LookupRouteFinder::find_route_and_switch(
  ArrayOffset offset,
  ArrayOffset *page_starts,
  ArrayOffset *page_ends) const {
  LookupRoute ret;
  ret.word = 0;
  ArrayOffset old = offset;
  offset = leaf_fanout_div_.div64(offset);
  *page_starts = offset * records_in_leaf_;
  *page_ends = *page_starts + records_in_leaf_;
  ret.route[0] = old - (*page_starts);
  for (uint8_t level = 1; level < levels_ - 1; ++level) {
    old = offset;
    offset = interior_fanout_div_.div64(offset);
    ret.route[level] = old - offset * kInteriorFanout;
  }
  if (levels_ > 1) {
    // the last level is done manually because we don't need any division there
    ASSERT_ND(offset < kInteriorFanout);
    ret.route[levels_ - 1] = offset;
  }

  return ret;
}

inline ArrayRange LookupRoute::calculate_page_range(
  uint8_t page_level,
  uint8_t total_levels,
  uint16_t payload_size,
  ArrayOffset array_size) const {
  ASSERT_ND(total_levels > page_level);
  ASSERT_ND(payload_size > 0);
  ASSERT_ND(array_size > 0);
  uint16_t records_in_leaf = to_records_in_leaf(payload_size);
  uint64_t interval = records_in_leaf;
  // for example:
  // page_level==0, route={xxx, 3, 4} (levels=3): r*3 + r*kInteriorFanout*4 ~ +r
  // page_level==1, route={xxx, yyy, 4}  (levels=3): r*kInteriorFanout*4 ~ +r*kInteriorFanout
  ArrayRange ret(0, 0);
  if (page_level == 0) {
    ret.end_ = records_in_leaf;
  }
  for (uint16_t level = 1; level < total_levels; ++level) {
    if (level == page_level) {
      ASSERT_ND(ret.begin_ == 0);
      ASSERT_ND(ret.end_ == 0);
      ret.end_ = interval * kInteriorFanout;
    } else if (level > page_level) {
      uint64_t shift = interval * route[level];
      ret.begin_ += shift;
      ret.end_ += shift;
    }
    interval *= kInteriorFanout;
  }
  ASSERT_ND(ret.begin_ < ret.end_);
  ASSERT_ND(ret.begin_ < array_size);
  if (ret.end_ > array_size) {
    ret.end_ = array_size;
  }
  return ret;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_ROUTE_HPP_
