/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_ID_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_ID_HPP_
/**
 * @file foedus/storage/sequential/sequential_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup SEQUENTIAL
 */

#include <stdint.h>

#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace storage {
namespace sequential {
/**
 * We have to represent the record count in 15 bits.
 * @ingroup SEQUENTIAL
 */
const uint16_t kMaxSlots = 1 << 15;
/**
 * Byte size of header in each data page of sequential storage.
 * @ingroup SEQUENTIAL
 */
const uint16_t kHeaderSize = 56;
/**
 * Byte size of data region in each data page of sequential storage.
 * @ingroup SEQUENTIAL
 */
const uint16_t kDataSize = foedus::storage::kPageSize - kHeaderSize;

/**
 * Payload must be shorter than this length.
 * @ingroup SEQUENTIAL
 */
const uint16_t kMaxPayload = kDataSize;

/**
 * Byte size of header in each root page of sequential storage.
 * @ingroup SEQUENTIAL
 */
const uint16_t kRootPageHeaderSize = 48;

/**
 * Maximum number of head pointers in one root page.
 * @ingroup SEQUENTIAL
 */
const uint16_t kRootPageMaxHeadPointers = (foedus::storage::kPageSize - kRootPageHeaderSize) / 8;

/**
 * Byte size of data region in each root page of sequential storage.
 * @ingroup SEQUENTIAL
 */
const uint16_t kRootPageDataSize = foedus::storage::kPageSize - kRootPageHeaderSize;

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_ID_HPP_
