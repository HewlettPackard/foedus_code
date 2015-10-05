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
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_ID_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_ID_HPP_
/**
 * @file foedus/storage/sequential/sequential_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup SEQUENTIAL
 */

#include <stdint.h>

#include "foedus/epoch.hpp"
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
const uint16_t kHeaderSize = 64;
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
const uint16_t kRootPageHeaderSize = 56;

/**
 * Each pointer to a snapshot head page comes with a bit more information to help reading.
 * @ingroup SEQUENTIAL
 */
struct HeadPagePointer {
  /** ID of the page that begins the linked list */
  SnapshotPagePointer page_id_;     // +8 -> 8
  /** Inclusive beginning of epochs in the pointed pages */
  Epoch               from_epoch_;  // +4 -> 12
  /** Exclusive end of epochs in the pointed pages */
  Epoch               to_epoch_;    // +4 -> 16
  /**
    * In a sequential storage, all pages from the head page is guaranteed to be
    * contiguous (that's how SequentialComposer write them out).
    * This enables us to read a large number of sequential pages in one-go rather than
    * reading one-page at a time.
    */
  uint64_t            page_count_;  // +8 -> 24
};



/**
 * Maximum number of head pointers in one root page.
 * @ingroup SEQUENTIAL
 */
const uint16_t kRootPageMaxHeadPointers
  = (foedus::storage::kPageSize - kRootPageHeaderSize) / sizeof(HeadPagePointer);

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
