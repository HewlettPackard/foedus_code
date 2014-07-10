/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_ID_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_ID_HPP_
/**
 * @file foedus/storage/masstree/masstree_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup MASSTREE
 */

#include <stdint.h>

#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {
/**
 * As defined in the paper, each Masstree page can have only 16 entries.
 * @ingroup MASSTREE
 */
const uint16_t kMaxSlots = 16;
/**
 * Byte size of header in each data page of masstree storage.
 * @ingroup MASSTREE
 */
const uint16_t kHeaderSize = 16;
/**
 * Byte size of data region in each data page of masstree storage.
 * @ingroup MASSTREE
 */
const uint16_t kDataSize = foedus::storage::kPageSize - kHeaderSize;

/**
 * Payload must be shorter than this length.
 * @ingroup MASSTREE
 */
const uint16_t kMaxPayload = kDataSize;

/**
 * Each key slice is an 8-byte integer. Masstree pages store and compare these key slices.
 * @ingroup MASSTREE
 */
typedef uint64_t KeySlice;

/**
 * Represents a permutation of 15 keys. See Masstree paper for more details.
 * @ingroup MASSTREE
 */
typedef uint64_t KeyPermutation;

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_ID_HPP_
