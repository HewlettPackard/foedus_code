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

/**
 * Each poiner page can contain 2^10 pointers (as the node is implicit, PagePoolOffset suffices)
 * and we can have at most 2^16 cores. Thus we have 2^6 pointers here.
 * This means we can waste 64*2=128 volatile pages (=512kb) per one sequential storage..
 * shouldn't be a big issue.
 * @ingroup SEQUENTIAL
 */
const uint16_t kPointerPageCount = 1U << 6;
const uint16_t kPointersPerPage = 1U << 10;

/** Calculate the page/index of the thread-private head/tail pointer. */
inline void get_pointer_page_and_index(uint16_t thread_id, uint16_t *page, uint16_t *index) {
  *page = thread_id / kPointersPerPage;
  *index = thread_id % kPointersPerPage;
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_ID_HPP_
