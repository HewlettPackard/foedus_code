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
void HashRootPage::initialize_volatile_page(
  StorageId storage_id,
  VolatilePagePointer page_id,
  HashRootPage* /*parent*/) {
  std::memset(this, 0, kPageSize);
  header_.init_volatile(page_id, storage_id, kHashRootPageType);
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
