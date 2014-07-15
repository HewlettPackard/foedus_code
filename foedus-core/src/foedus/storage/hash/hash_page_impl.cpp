/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/hash/hash_page_impl.hpp"

#include <cstring>

#include "foedus/assert_nd.hpp"

namespace foedus {
namespace storage {
namespace hash {
void HashRootPage::initialize_page(StorageId storage_id, uint64_t page_id) {
  std::memset(this, 0, kPageSize);
  header_.storage_id_ = storage_id;
  header_.page_id_ = page_id;
}

void HashBinPage::initialize_page(
  StorageId storage_id,
  uint64_t page_id,
  uint64_t begin_bin,
  uint64_t end_bin) {
  std::memset(this, 0, kPageSize);
  header_.storage_id_ = storage_id;
  header_.page_id_ = page_id;
  begin_bin_ = begin_bin;
  end_bin_ = end_bin;
}

void HashDataPage::initialize_page(StorageId storage_id, uint64_t page_id, uint64_t bin) {
  header_.checksum_ = 0;
  header_.page_id_ = page_id;
  header_.storage_id_ = storage_id;
  next_page_.snapshot_pointer_ = 0;
  next_page_.volatile_pointer_.word = 0;
  page_owner_ = xct::XctId();
  ASSERT_ND(bin < (1ULL << 48));  // fits 48 bits? If not, insane (2^48*64b just for bin pages).
  bin_high_ = static_cast<uint16_t>(bin >> 32);
  bin_low_ = static_cast<uint32_t>(bin);
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
