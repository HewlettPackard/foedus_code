/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_IMPL_HPP_

#include <stdint.h>

#include <cstring>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/epoch.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/**
 * @brief Represents one data page in \ref MASSTREE.
 * @ingroup MASSTREE
 * @details
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class MasstreePage final {
 public:
  // A page object is never explicitly instantiated. You must reinterpret_cast.
  MasstreePage() = delete;
  MasstreePage(const MasstreePage& other) = delete;
  MasstreePage& operator=(const MasstreePage& other) = delete;

  // simple accessors
  PageHeader&         header() { return header_; }
  const PageHeader&   header() const { return header_; }

  /** Called only when this page is initialized. */
  void                initialize_data_page(StorageId storage_id, uint64_t page_id) {
    header_.checksum_ = 0;
    header_.page_id_ = page_id;
    header_.storage_id_ = storage_id;
  }

 private:
  PageHeader            header_;          // +16 -> 16

  char                  data_[kDataSize];
};
STATIC_SIZE_CHECK(sizeof(MasstreePage), 1 << 12)

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_IMPL_HPP_
