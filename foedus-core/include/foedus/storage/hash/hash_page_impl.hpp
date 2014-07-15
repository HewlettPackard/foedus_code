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

/**
 * @brief Represents a root page in \ref HASH.
 * @ingroup HASH
 * @details
 * This is one of the page types in hash.
 * A root page is simply a list of pointers to child pages.
 */
class HashRootPage final {
 public:
  HashRootPage() = delete;
  HashRootPage(const HashRootPage& other) = delete;
  HashRootPage& operator=(const HashRootPage& other) = delete;

  const PageHeader&       header() const { return header_; }
  DualPagePointer&        pointer(uint16_t index) { return pointers_[index]; }
  const DualPagePointer&  pointer(uint16_t index) const { return pointers_[index]; }

  /** Called only when this page is initialized. */
  void                    initialize_page(StorageId storage_id, uint64_t page_id);

 private:
  /** common header */
  PageHeader          header_;        // +32 -> 32

  /**
   * Pointers to child nodes.
   * It might point to either child root page or a bin page.
   */
  DualPagePointer     pointers_[kHashRootPageFanout];
};
static_assert(sizeof(HashRootPage) == kPageSize, "sizeof(HashRootPage) is not kPageSize");

/**
 * @brief Represents a bin page in \ref HASH.
 * @ingroup HASH
 * @details
 * This is one of the page types in hash.
 * A bin page contains several hash bins, each of which has a pointer to the corresponding data
 * page, set of HashTag for the records in the data page, and mod counter for search-miss.
 */
class HashBinPage final {
 public:
  HashBinPage() = delete;
  HashBinPage(const HashBinPage& other) = delete;
  HashBinPage& operator=(const HashBinPage& other) = delete;

  /**
   * One hash bin. It should contain around kMaxEntriesPerBin * 0.5~0.7 entries.
   */
  struct Bin {
    /**
     * This counter is atomically incremented whenever a new entry is inserted. So read-only
     * queries that didn't find a record can use this for version verification without going
     * down to data page.
     */
    uint16_t  mod_counter_;               // +2  -> 2
    /**
     * Hash tags of entries in this bin.
     * New tags are inserted \b before the record itself for serializability, so false positives
     * are possible.
     */
    HashTag   tags_[kMaxEntriesPerBin];   // +46 -> 48
    /**
     * Pointer to data page for this hash bin.
     */
    DualPagePointer data_pointer_;        // +16 -> 64
  };

  const PageHeader&       header() const { return header_; }
  inline const Bin& bin(uint16_t i) const ALWAYS_INLINE {
    ASSERT_ND(i < kBinsPerPage);
    return bins_[i];
  }
  inline Bin&       bin(uint16_t i) ALWAYS_INLINE {
    ASSERT_ND(i < kBinsPerPage);
    return bins_[i];
  }

  /** Called only when this page is initialized. */
  void                initialize_page(StorageId storage_id, uint64_t page_id, uint64_t bin);

  inline void         assert_bin(uint64_t bin) ALWAYS_INLINE {
    ASSERT_ND(bin >= begin_bin_);
    ASSERT_ND(bin < end_bin_);
  }

  void initialize_page(
    StorageId storage_id,
    uint64_t page_id,
    uint64_t begin_bin,
    uint64_t end_bin);

 private:
  /** common header */
  PageHeader  header_;        // +32 -> 32

  // we don't need anything else for hash bin page, but we have to make it 64bit-filled.
  // so, let's put auxiliary information for sanity-check.

  /** Inclusive beginning of bin number that belong to this page */
  uint64_t    begin_bin_;   // +8 -> 40
  /** Exclusive end of bin number that belong to this page */
  uint64_t    end_bin_;     // +8 -> 48

  char        dummy_[16];   // +16 -> 64

  /**
   * Pointers to child nodes.
   * It might point to either child root page or a bin page.
   */
  Bin         bins_[kBinsPerPage];
};
static_assert(sizeof(HashBinPage) == kPageSize, "sizeof(HashBinPage) is not kPageSize");
static_assert(sizeof(HashBinPage::Bin) == kHashBinSize, "kHashBinSize is wrong");

/**
 * @brief Represents an individual data page in \ref HASH.
 * @ingroup HASH
 * @details
 * This is one of the page types in hash.
 * A data page contains full keys and values.
 */
class HashDataPage final {
 public:
  enum Constants {
    /** Bit value for flags_ in case the record is logically deleted. */
    kFlagDeleted = 0x8000,
    /**
     * If this bit is ON, the record is stored in next page(s).
     * In that case, we store real location in the original record.
     * @todo finalize the format in this case.
     */
    kFlagStoredInNextPages = 0x4000,
  };
  /**
   * Fix-sized slot for each record, which is placed at the end of data region.
   */
  struct Slot {
    /**
     * Byte offset in data_ where this record starts.
     */
    uint16_t offset_;
    /**
     * Byte length of the record including everything; XctId, key, and payload.
     * Padding is not included.
     */
    uint16_t record_length_;
    /**
     * Byte length of key of the record.
     */
    uint16_t key_length_;
    /**
     * Various flags for the record.
     */
    uint16_t flags_;
  };

  // A page object is never explicitly instantiated. You must reinterpret_cast.
  HashDataPage() = delete;
  HashDataPage(const HashDataPage& other) = delete;
  HashDataPage& operator=(const HashDataPage& other) = delete;

  // simple accessors
  const PageHeader&       header() const { return header_; }
  inline const xct::XctId&  page_owner() const ALWAYS_INLINE { return page_owner_; }
  inline xct::XctId&        page_owner() ALWAYS_INLINE { return page_owner_; }
  inline const DualPagePointer&  next_page() const ALWAYS_INLINE { return next_page_; }
  inline DualPagePointer&   next_page() ALWAYS_INLINE { return next_page_; }
  inline uint16_t           get_record_count() const ALWAYS_INLINE { return record_count_; }
  inline const Slot&        slot(uint16_t record) const ALWAYS_INLINE { return slots_[record]; }
  inline Slot&              slot(uint16_t record) ALWAYS_INLINE { return slots_[record]; }

  inline Record*            interpret_record(uint16_t offset) ALWAYS_INLINE {
    return reinterpret_cast<Record*>(data_ + offset);
  }
  inline const Record*      interpret_record(uint16_t offset) const ALWAYS_INLINE {
    return reinterpret_cast<const Record*>(data_ + offset);
  }

  /** Called only when this page is initialized. */
  void                initialize_page(StorageId storage_id, uint64_t page_id, uint64_t bin);

  inline void         assert_bin(uint64_t bin) ALWAYS_INLINE {
    ASSERT_ND(bin == ((static_cast<uint64_t>(bin_high_) << 32) | bin_low_));
  }

 private:
  PageHeader      header_;      // +32 -> 32

  /**
   * This is used for coarse-grained locking for entries in this page (including next pages).
   * Only a successful insert locks this and updates the value (in addition to record's xct id).
   * Others just adds this to read set to be aware of new entries.
   * Note that even deletion doesn't lock it because it just puts the deletion flag.
   */
  xct::XctId      page_owner_;  // +8 -> 40

  /**
   * When records don't fit one page (eg very long key or value),
   * Could be an array of pointer here to avoid following all pages as we can have only 23 entries
   * per bin. Let's revisit later.
   */
  DualPagePointer next_page_;   // +16 -> 56

  /**
   * How many records do we \e physically have in this bin.
   * @invariant record_count_ <= kMaxEntriesPerBin
   */
  uint16_t        record_count_;  // +2 -> 58
  /** High 16 bits of hash bin (Assuming #bins fits 48 bits). Used only for sanity check. */
  uint16_t        bin_high_;  // +2 -> 60
  /** Low 32 bits of hash bin. Used only for sanity check. */
  uint32_t        bin_low_;   // +4 -> 64

  /**
   * Record slots for each record. We initially planned to have this at the end of data
   * and grow it backward, but we so far allows only 23 entries per bin.
   * So, wouldn't matter to have it here always spending this negligible size.
   * When we somehow allow more entries per bin, we will revisit this.
   */
  Slot            slots_[kMaxEntriesPerBin];  // +8*23 -> 248

  /**
   * Contiguous record data.
   * Each record consists of:
   *  \li XctId
   *  \li Key
   *  \li Data
   *  \li (Optional) padding to make the record 8-byte aligned
   */
  char            data_[kPageSize - kHashDataPageHeaderSize];
};

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_PAGE_HPP_
