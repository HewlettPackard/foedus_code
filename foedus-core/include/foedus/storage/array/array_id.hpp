/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_ID_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_ID_HPP_
#include <foedus/storage/storage_id.hpp>
#include <stdint.h>
#include <iosfwd>
/**
 * @file foedus/storage/array/array_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup ARRAY
 */
namespace foedus {
namespace storage {
namespace array {

/**
 * @brief bluh
 * @ingroup ARRAY
 * @details
 * bluh
 */
typedef uint64_t ArrayOffset;

/**
 * @brief Represents an offset range in an array storage.
 * @ingroup ARRAY
 * @details
 */
struct ArrayRange {
    /** Inclusive beginning of the offset range. */
    ArrayOffset begin_;
    /** Exclusive end of the offset range. */
    ArrayOffset end_;
};

/**
 * @brief bluh.
 * @ingroup ARRAY
 */
const uint16_t HEADER_SIZE = 32;
/**
 * @brief bluh.
 * @ingroup ARRAY
 */
const uint16_t INTERIOR_SIZE = 16;
/**
 * @brief bluh.
 * @ingroup ARRAY
 */
const uint16_t INTERIOR_FANOUT = (foedus::storage::PAGE_SIZE - HEADER_SIZE) / INTERIOR_SIZE;

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_ID_HPP_
