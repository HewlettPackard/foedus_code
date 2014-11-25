/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_HASH_HASH_ID_HPP_
#define FOEDUS_STORAGE_HASH_HASH_ID_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/storage/storage_id.hpp"

/**
 * @file foedus/storage/hash/hash_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup HASH
 */
namespace foedus {
namespace storage {
namespace hash {

/**
 * @brief Byte size of header in root page of hash storage.
 * @ingroup HASH
 */
const uint16_t kHashRootPageHeaderSize  = 32 + 8 * 2;

/**
 * @brief Number of pointers in a root page of hash storage.
 * @ingroup HASH
 */
const uint16_t kHashRootPageFanout      =
  (kPageSize - kHashRootPageHeaderSize) / sizeof(DualPagePointer);

/**
 * @brief Byte size of header in bin page of hash storage.
 * @ingroup HASH
 */
const uint16_t kHashBinPageHeaderSize   = 64;

/**
 * @brief Byte size of header in data page of hash storage.
 * @ingroup HASH
 */
const uint16_t kHashDataPageHeaderSize  = 256;

/**
 * @brief Represents a compact \e tag of hash values.
 * @ingroup HASH
 * @details
 * This value is used to efficiently differentiate entries placed in the same hash bin
 * and also to calculate the alternative hash bin. For more details, see [FAN13].
 * The larger this type is, the more entries per bin we can differentiate at the cost of
 * larger space in bin pages. Due to the 16 bytes overhead (DualPagePointer) per bin,
 * our bin stores a relatively large number of entries, so we picked 2 bytes rather than 1 byte.
 */
typedef uint16_t HashTag;

/**
 * @brief Byte size of one hash bin.
 * @ingroup HASH
 */
const uint16_t kHashBinSize             = 64;

/**
 * @brief Max number of entries in one hash bin.
 * @ingroup HASH
 */
const uint16_t kMaxEntriesPerBin        =
  (kHashBinSize - sizeof(DualPagePointer) - sizeof(uint16_t)) / sizeof(HashTag);

/**
 * @brief Number of bins in one hash bin page.
 * @ingroup HASH
 */
const uint16_t kBinsPerPage             = (kPageSize - kHashBinPageHeaderSize) / kHashBinSize;

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_ID_HPP_
