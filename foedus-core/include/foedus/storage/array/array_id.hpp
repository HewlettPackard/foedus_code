/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_ID_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_ID_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/storage/storage_id.hpp"

/**
 * @file foedus/storage/array/array_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup ARRAY
 */
namespace foedus {
namespace storage {
namespace array {

/**
 * @brief The only key type in array storage.
 * @ingroup ARRAY
 * @details
 * The key in array storage is \e offset, or an integer starting from zero.
 * This means we don't support multi-dimensional, dynamic, sparse, nor any other fancy arrays.
 * However, those arrays can be provided by the relational layer based on this array storage.
 * The offset-conversion is fairly straightforward.
 */
typedef uint64_t ArrayOffset;

/**
 * @brief Represents an offset range in an array storage.
 * @ingroup ARRAY
 * @details
 * Begin is inclusive, end is exclusive.
 */
struct ArrayRange {
  ArrayRange() : begin_(0), end_(0) {}
  ArrayRange(ArrayOffset begin, ArrayOffset end) : begin_(begin), end_(end) {}

  bool    contains(ArrayOffset offset) const { return offset >= begin_ && offset < end_; }

  /** Inclusive beginning of the offset range. */
  ArrayOffset begin_;
  /** Exclusive end of the offset range. */
  ArrayOffset end_;
};

/**
 * @brief Byte size of header in each page of array storage.
 * @ingroup ARRAY
 */
const uint16_t kHeaderSize = 48;
/**
 * @brief Byte size of data region in each page of array storage.
 * @ingroup ARRAY
 */
const uint16_t kDataSize = foedus::storage::kPageSize - kHeaderSize;
/**
 * @brief Byte size of an entry in interior page of array storage.
 * @ingroup ARRAY
 */
const uint16_t kInteriorSize = 16;
/**
 * @brief Max number of entries in an interior page of array storage.
 * @ingroup ARRAY
 */
const uint16_t kInteriorFanout = (foedus::storage::kPageSize - kHeaderSize) / kInteriorSize;

/**
 * @brief Code in array storage assumes this number as the maximum number of levels.
 * @ingroup ARRAY
 * @details
 * Interior page always has a big fanout close to 256, so 8 levels are more than enough.
 */
const uint8_t kMaxLevels = 8;

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_ID_HPP_
