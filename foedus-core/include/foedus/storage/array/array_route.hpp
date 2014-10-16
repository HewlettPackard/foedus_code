/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
};

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
      records_in_leaf_(kDataSize / (assorted::align8(payload_size) + kRecordOverhead)),
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

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_ROUTE_HPP_
