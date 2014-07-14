/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_HASH_HASH_PAGE_HPP_
#define FOEDUS_STORAGE_HASH_HASH_PAGE_HPP_
#include <stdint.h>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {

namespace storage {
namespace hash {

class HashRootPage final {
 public:
  uint16_t get_count() const {
    return count_;
  }

  DualPagePointer& get_pointer(uint16_t index) {
    return pointers_[index];
  }

  const DualPagePointer& get_pointer(uint16_t index) const {
    return pointers_[index];
  }

  /** Called only when this page is initialized. */
  void                initialize_data_page(
    StorageId storage_id,
    uint64_t page_id);

 private:
  /** common header */
  PageHeader          header_;        // +16 -> 16
  uint16_t            count_;         // +2  -> 18
  char                dummy_[14];     // +14 -> 32
  DualPagePointer pointers_[kHashRootPageFanout];
};

struct Bin {
public:
private:
  uint64_t counter_;           // +8  -> 8
  uint8_t tags_[40];           // +40 -> 48
  DualPagePointer leaf_pages_; // +16 -> 64
};

struct BinPage {
public:

private:
  PageHeader header_; // +16 -> 16
  char filler[48];    // +48 -> 64
  Bin bin_list[63];   // + a lot = a lot
};

struct DataPage {
public:
    // A page object is never explicitly instantiated. You must reinterpret_cast.
  DataPage() = delete;
  DataPage(const DataPage& other) = delete;
  DataPage& operator=(const DataPage& other) = delete;

    //For the rest, can't I just get away with using what I found in sequential_page_impl.hpp and .cpp


  // simple accessors
  PageHeader&         header() { return header_; }
  const PageHeader&   header() const { return header_; }


  SnapshotPagePointer get_next_page() const { return next_page_; }
  void                set_next_page(SnapshotPagePointer page) { next_page_ = page; }

  /** Called only when this page is initialized. */
  void                initialize_root_page(StorageId storage_id, uint64_t page_id) {
    header_.checksum_ = 0;
    header_.page_id_ = page_id;
    header_.storage_id_ = storage_id;
    pointer_count_ = 0;
    next_page_ = 0;
  }

  void get_record_info(int position, uint32_t &offset, uint32_t length, uint32_t &key_length, uint32_t &flags){
     offset = *(uint32_t *) (data_+kPageSize-5-position*8);
     length = *(uint32_t *) (data_+kPageSize-5-position*8 +2);
     key_length =  *(uint32_t *) (data_+kPageSize-5-position*8 +4);
     flags = *(uint32_t *) (data_+kPageSize-5-position*8 +6);
  }

private:
  PageHeader header_; // +16 -> 16
  DualPagePointer next_page_; // +16 -> 32
  uint64_t page_id; // +8  -> 40
  /**
   * @brief Consists of the records at the start, the buffer zone in the middle, and record information at the end
   * (including where to get each record).
   */
  char data_[kPageSize - 5];
};




}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_PAGE_HPP_
