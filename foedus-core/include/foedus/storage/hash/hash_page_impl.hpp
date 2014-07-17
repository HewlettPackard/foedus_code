/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_HASH_HASH_PAGE_HPP_
#define FOEDUS_STORAGE_HASH_HASH_PAGE_HPP_

#include <stdint.h>

#include <cstring>

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
  void                    initialize_volatile_page(
    StorageId storage_id,
    VolatilePagePointer page_id,
    HashRootPage* parent);

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

  /** volatile page initialize callback. */
  struct Initializer final : public VolatilePageInitializer {
    Initializer(StorageId storage_id, HashRootPage* parent, uint64_t begin_bin)
      : VolatilePageInitializer(
        storage_id,
        kHashBinPageType,
        false,
        reinterpret_cast<Page*>(parent)),
        begin_bin_(begin_bin) {
    }
    void initialize_more(Page* page) const override {
      reinterpret_cast<HashBinPage*>(page)->begin_bin_ = begin_bin_;
      reinterpret_cast<HashBinPage*>(page)->end_bin_ = begin_bin_ + kBinsPerPage;
    }
    const uint64_t begin_bin_;
  };

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

  const PageHeader& header() const { return header_; }
  inline const Bin& bin(uint16_t i) const ALWAYS_INLINE {
    ASSERT_ND(i < kBinsPerPage);
    return bins_[i];
  }
  inline Bin&       bin(uint16_t i) ALWAYS_INLINE {
    ASSERT_ND(i < kBinsPerPage);
    return bins_[i];
  }

  inline void       assert_bin(uint64_t bin) ALWAYS_INLINE {
    ASSERT_ND(bin >= begin_bin_);
    ASSERT_ND(bin < end_bin_);
  }

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

  /** volatile page initialize callback. */
  struct Initializer final : public VolatilePageInitializer {
    Initializer(StorageId storage_id, HashBinPage* parent, uint64_t bin)
      : VolatilePageInitializer(
        storage_id,
        kHashBinPageType,
        false,
        reinterpret_cast<Page*>(parent)),
        bin_(bin) {
    }
    void initialize_more(Page* page) const override {
      reinterpret_cast<HashDataPage*>(page)->set_bin(bin_);
    }
    const uint64_t bin_;
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
  const PageHeader&         header() const { return header_; }
  inline const xct::XctId&  page_owner() const ALWAYS_INLINE { return page_owner_; }
  inline xct::XctId&        page_owner() ALWAYS_INLINE { return page_owner_; }
  inline const DualPagePointer&  next_page() const ALWAYS_INLINE { return next_page_; }
  inline DualPagePointer&   next_page() ALWAYS_INLINE { return next_page_; }
  inline uint16_t           get_record_count() const ALWAYS_INLINE { return record_count_; }
  inline uint64_t           get_bin() const ALWAYS_INLINE {
    return ((static_cast<uint64_t>(bin_high_) << 32) | bin_low_);
  }
  inline void               set_bin(uint64_t bin) ALWAYS_INLINE {
    ASSERT_ND(bin < (1ULL << 48));  // fits 48 bits? If not, insane (2^48*64b just for bin pages).
    bin_high_ = static_cast<uint16_t>(bin >> 32);
    bin_low_ = static_cast<uint32_t>(bin);
  }
  inline const Slot&        slot(uint16_t record) const ALWAYS_INLINE { return slots_[record]; }
  inline Slot&              slot(uint16_t record) ALWAYS_INLINE { return slots_[record]; }

  inline Record*            interpret_record(uint16_t offset) ALWAYS_INLINE {
    return reinterpret_cast<Record*>(data_ + offset);
  }
  inline const Record*      interpret_record(uint16_t offset) const ALWAYS_INLINE {
    return reinterpret_cast<const Record*>(data_ + offset);
  }

  inline void               assert_bin(uint64_t bin) ALWAYS_INLINE { ASSERT_ND(bin == get_bin()); }


  /** Used only for inserts when record_count is small enough. */
  inline void               add_record(
    xct::XctId xct_id,
    uint16_t slot,
    uint16_t key_length,
    uint16_t payload_count,
    const char *data) {
    // this must be called by insert, which takes lock on the page.
    ASSERT_ND(page_owner_.is_keylocked());

    uint16_t record_length = key_length + payload_count + kRecordOverhead;
    uint16_t pos;
    if (slot >= record_count_) {
      // appending at last
      ASSERT_ND(record_count_ == slot);
      ASSERT_ND(record_count_ < kMaxEntriesPerBin);
      if (record_count_ == 0) {
        pos = 0;
      } else {
        const Slot& prev_slot = slots_[record_count_ - 1];
        pos = prev_slot.offset_ + assorted::align8(prev_slot.record_length_);
        ASSERT_ND(pos + record_length <= kPageSize - kHashDataPageHeaderSize);
      }
      ++record_count_;
    } else {
      pos = slots_[slot].offset_;
    }

    slots_[slot].offset_ = pos;
    slots_[slot].record_length_ = record_length;
    slots_[slot].key_length_ = key_length;
    slots_[slot].flags_ = 0;
    interpret_record(pos)->owner_id_ = xct_id;
    std::memcpy(data_ + pos + kRecordOverhead, data, key_length + payload_count);
  }

  /** Used only for inserts to find a slot we can insert to. */
  inline uint16_t           find_empty_slot(uint16_t key_length, uint16_t payload_count) const {
    // this must be called by insert, which takes lock on the page.
    ASSERT_ND(page_owner_.is_keylocked());
    if (record_count_ == 0) {
      return 0;
    }
    uint16_t record_length = key_length + payload_count + kRecordOverhead;
    // here, any deleted slot whose *physical* record size>=record_length works.
    // as record is 8-byte aligned, 1~8 are same.
    if (record_length % 8 != 0) {
      record_length = record_length / 8 * 8 + 1;
    }
    for (uint16_t i = 0; i < record_count_; ++i) {
      if ((slots_[i].flags_ & kFlagDeleted) && slots_[i].record_length_ >= record_length) {
        return i;
      }
    }

    // appending at last as a new physical record
    // TODO(Hideaki) we should overflow to next page in these cases.
    ASSERT_ND(record_count_ < kMaxEntriesPerBin);
    ASSERT_ND(slots_[record_count_ - 1].offset_ +
      record_length +
      assorted::align8(slots_[record_count_ - 1].record_length_)
      <= kPageSize - kHashDataPageHeaderSize);
    return record_count_;
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
