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
 * @brief Represents one intermediate page in \ref MASSTREE.
 * @ingroup MASSTREE
 * @details
 * An intermediate page consists of bunch of separator keys and pointers to children nodes,
 * which might be another intermediate pages or boundary nodes.
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class MasstreeIntermediatePage final {
 public:
  struct MiniPage {
    uint32_t        status_counter_;
    uint8_t         status_bits1_;
    uint8_t         status_bits2_;
    uint8_t         status_bits3_;
    uint8_t         separator_count_;

    KeySlice        separators_[15];
    DualPagePointer pointers_[16];
  };

  // A page object is never explicitly instantiated. You must reinterpret_cast.
  MasstreeIntermediatePage() = delete;
  MasstreeIntermediatePage(const MasstreeIntermediatePage& other) = delete;
  MasstreeIntermediatePage& operator=(const MasstreeIntermediatePage& other) = delete;

  // simple accessors
  PageHeader&         header() { return header_; }
  const PageHeader&   header() const { return header_; }

  /** Called only when this page is initialized. */
  void                initialize_data_page(StorageId storage_id, uint64_t page_id) {
    header_.checksum_ = 0;
    header_.page_id_ = page_id;
    header_.storage_id_ = storage_id;
    status_counter_ = 0;
    status_bits1_ = 0;
    status_bits2_ = 0;
    status_bits3_ = 0;
    separator_count_ = 2;
    separators_[0] = 0;
    separators_[1] = 0;
  }

 private:
  PageHeader          header_;      // +32 -> 32

  uint32_t            status_counter_;
  uint8_t             status_bits1_;
  uint8_t             status_bits2_;
  uint8_t             status_bits3_;
  uint8_t             separator_count_;

  KeySlice            separators_[11];  // +88 -> 128

  MiniPage            mini_pages_[10];  // +284 * 10 -> 4096-128

  char                reserved_[128];   // +128 -> 4096
};
STATIC_SIZE_CHECK(sizeof(MasstreeIntermediatePage::MiniPage), 128 + 256)
STATIC_SIZE_CHECK(sizeof(MasstreeIntermediatePage), 1 << 12)

/**
 * @brief Represents one boundary page in \ref MASSTREE.
 * @ingroup MASSTREE
 * @details
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class MasstreeBoundaryPage final {
 public:
  // A page object is never explicitly instantiated. You must reinterpret_cast.
  MasstreeBoundaryPage() = delete;
  MasstreeBoundaryPage(const MasstreeBoundaryPage& other) = delete;
  MasstreeBoundaryPage& operator=(const MasstreeBoundaryPage& other) = delete;

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
  PageHeader            header_;            // +32 -> 32

  // +8
  uint32_t              status_counter_;
  uint32_t              status_bits1_;

  // +16
  uint8_t               status_bits2_;
  uint8_t               keylen_[15];

  uint64_t              dummy1_;

  // +128
  KeyPermutation        permutation_;
  KeySlice              keys_[15];

  char                  data_[3904];
};
STATIC_SIZE_CHECK(sizeof(MasstreeBoundaryPage), 1 << 12)

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_IMPL_HPP_
