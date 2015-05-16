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
#ifndef FOEDUS_STORAGE_HASH_HASH_PAGE_HPP_
#define FOEDUS_STORAGE_HASH_HASH_PAGE_HPP_

#include <stdint.h>

#include <cstring>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/fwd.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/hash/hash_combo.hpp"
#include "foedus/storage/hash/hash_hashinate.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace hash {

/**
 * @brief Represents an intermediate page in \ref HASH.
 * @ingroup HASH
 * @details
 * This is one of the page types in hash.
 * An intermediate page is simply a list of pointers to child pages, which might be
 * intermediate or data pages.
 *
 * @par Page Layout
 * <table>
 *  <tr>
 *    <td>Common PageHeader</td><td>HashRange (just for sanity check)</td>
 *    <td>DualPagePointer</td><td>DualPagePointer</td><td>...</td>
 *  </tr>
 * </table>
 *
 * Each page simply contains kHashIntermediatePageFanout pointers to lower level.
 *
 * @note Let's not use the word 'leaf' in hash storage. We previously called intermediate
 * pages whose get_level()==0 as leaves, but it's confusing. A leaf page usually means a
 * last-level page that contains tuple data. Here, such a page still points to lower level
 * (data pages). So, we should call it something like "level-0 intermediate" or just "level-0".
 */
class HashIntermediatePage final {
 public:
  HashIntermediatePage() = delete;
  HashIntermediatePage(const HashIntermediatePage& other) = delete;
  HashIntermediatePage& operator=(const HashIntermediatePage& other) = delete;

  PageHeader&             header() { return header_; }
  const PageHeader&       header() const { return header_; }
  DualPagePointer&        get_pointer(uint16_t index) { return pointers_[index]; }
  const DualPagePointer&  get_pointer(uint16_t index) const { return pointers_[index]; }
  DualPagePointer*        get_pointer_address(uint16_t index) { return pointers_ + index; }
  const DualPagePointer*  get_pointer_address(uint16_t index) const { return pointers_ + index; }

  /** Called only when this page is initialized. */
  void                    initialize_volatile_page(
    StorageId storage_id,
    VolatilePagePointer page_id,
    const HashIntermediatePage* parent,
    uint8_t level,
    HashBin start_bin);

  void                    initialize_snapshot_page(
    StorageId storage_id,
    SnapshotPagePointer page_id,
    uint8_t level,
    HashBin start_bin);

  void release_pages_recursive_parallel(Engine* engine);
  void release_pages_recursive(
    const memory::GlobalVolatilePageResolver& page_resolver,
    memory::PageReleaseBatch* batch);

  const HashBinRange&   get_bin_range() const { return bin_range_; }
  inline uint8_t   get_level() const { return header_.get_in_layer_level(); }

  inline void      assert_bin(HashBin bin) const ALWAYS_INLINE {
    ASSERT_ND(bin_range_.contains(bin));
  }
  inline void      assert_range() const ALWAYS_INLINE {
    ASSERT_ND(bin_range_.length() == kHashMaxBins[get_level() + 1U]);
  }

  /** defined in hash_page_debug.cpp. */
  friend std::ostream& operator<<(std::ostream& o, const HashIntermediatePage& v);

 private:
  /** common header */
  PageHeader          header_;        // +32 -> 32

  /**
   * these are used only for sanity checks in debug builds. they are always implicit.
   * the end is always "begin + kFanout^(level+1)". it doesn't matter what the maximum
   * hash bin value is for the storage.
   * @invariant bin_range_.length() == kHashMaxBins[get_level() + 1U]
   */
  HashBinRange        bin_range_;     // +16 -> 48

  /**
   * Pointers to child nodes.
   * It might point to either child intermediate page or a data page.
   */
  DualPagePointer     pointers_[kHashIntermediatePageFanout];
};

typedef uint16_t DataPageSlotIndex;
const DataPageSlotIndex kSlotNotFound = 0xFFFFU;

/**
 * @brief Represents an individual data page in \ref HASH.
 * @ingroup HASH
 * @details
 * This is one of the page types in hash.
 * A data page contains full keys and values.
 *
 * @par Page Layout
 * <table>
 *  <tr><td>Headers, including bloom filter for faster check</td></tr>
 *  <tr><td>Record Data part, which grows forward</td></tr>
 *  <tr><th>Unused part</th></tr>
 *  <tr><td>Slot part (32 bytes per record), which grows backward</td></tr>
 * </table>
 */
class HashDataPage final {
 public:
  /**
   * Fix-sized slot for each record, which is placed at the end of data region.
   */
  struct Slot {
    /**
     * TID of the record.
     */
    xct::LockableXctId tid_;            // +16 -> 16

    /**
     * Byte offset in data_ where this record starts.
     * This is immutable once the record is allocated.
     */
    uint16_t offset_;                   // +2  -> 18
    /**
     * Byte count this record occupies.
     * This is \b immutable once the record is allocated.
     * @invariant physical_record_length_ % 8 == 0
     * @invariant physical_record_length_ >= align8(key_length_) + align8(payload_length_)
     * @invariant offset_ + physical_record_length_ == next record's offset_
     */
    uint16_t physical_record_length_;   // +2  -> 20
    /**
     * Byte length of key of the record. Padding is not included.
     * This is \b immutable once the record is allocated.
     */
    uint16_t key_length_;               // +2  -> 22
    /**
     * Byte length of the payload. Padding is not included.
     * This is \b mutable. Only changed with update.
     * If the updated payload is too large for physical_record_length_, we \e move the record,
     * leaving the \e deleted and \e moved flags in this old record.
     */
    uint16_t payload_length_;           // +2  -> 24

    /** Full hash of the key of the record. This is \b immutable once the record is allocated. */
    HashValue hash_;                    // +8  -> 32

    inline uint16_t get_aligned_key_length() const { return assorted::align8(key_length_); }
    inline uint16_t get_max_payload() const {
      return physical_record_length_ - get_aligned_key_length();
    }
  };

  // A page object is never explicitly instantiated. You must reinterpret_cast.
  HashDataPage() = delete;
  HashDataPage(const HashDataPage& other) = delete;
  HashDataPage& operator=(const HashDataPage& other) = delete;

  void initialize_volatile_page(
    StorageId storage_id,
    VolatilePagePointer page_id,
    const Page* parent,
    HashBin bin,
    uint8_t bin_bits,
    uint8_t bin_shifts);
  void initialize_snapshot_page(
    StorageId storage_id,
    SnapshotPagePointer page_id,
    HashBin bin,
    uint8_t bin_bits,
    uint8_t bin_shifts);
  void release_pages_recursive(
    const memory::GlobalVolatilePageResolver& page_resolver,
    memory::PageReleaseBatch* batch);


  // simple accessors
  PageHeader&               header() { return header_; }
  const PageHeader&         header() const { return header_; }

  inline const DualPagePointer&  next_page() const ALWAYS_INLINE { return next_page_; }
  inline DualPagePointer&   next_page() ALWAYS_INLINE { return next_page_; }
  inline const DualPagePointer*  next_page_address() const ALWAYS_INLINE { return &next_page_; }
  inline DualPagePointer*   next_page_address() ALWAYS_INLINE { return &next_page_; }

  inline const DataPageBloomFilter& bloom_filter() const ALWAYS_INLINE { return bloom_filter_; }
  inline DataPageBloomFilter& bloom_filter() ALWAYS_INLINE { return bloom_filter_; }

  inline uint16_t           get_record_count() const ALWAYS_INLINE { return header_.key_count_; }

  inline const Slot&        get_slot(DataPageSlotIndex record) const ALWAYS_INLINE {
    return reinterpret_cast<const Slot*>(this + 1)[-record - 1];
  }
  inline Slot&              get_slot(DataPageSlotIndex record) ALWAYS_INLINE {
    return reinterpret_cast<Slot*>(this + 1)[-record - 1];
  }
  /** same as &get_slot(), but this is more explicit and easier to understand/maintain */
  inline const Slot*        get_slot_address(DataPageSlotIndex record) const ALWAYS_INLINE {
    return reinterpret_cast<const Slot*>(this + 1) - record - 1;
  }
  inline Slot*              get_slot_address(DataPageSlotIndex record) ALWAYS_INLINE {
    return reinterpret_cast<Slot*>(this + 1) - record - 1;
  }

  inline char*        record_from_offset(uint16_t offset) { return data_ + offset; }
  inline const char*  record_from_offset(uint16_t offset) const { return data_ + offset; }

  // methods about available/required spaces

  /** Returns usable space in bytes. */
  inline uint16_t     available_space() const {
    uint16_t consumed = next_offset() + get_record_count() * sizeof(Slot);
    ASSERT_ND(consumed <= sizeof(data_));
    if (consumed > sizeof(data_)) {  // just to be conservative on release build
      return 0;
    }
    return sizeof(data_) - consumed;
  }

  /** Returns offset of a next record */
  inline uint16_t     next_offset() const {
    DataPageSlotIndex count = get_record_count();
    if (count == 0) {
      return 0;
    }
    const Slot& last_slot = get_slot(count - 1);
    ASSERT_ND((last_slot.offset_ + last_slot.physical_record_length_) % 8 == 0);
    return last_slot.offset_ + last_slot.physical_record_length_;
  }

  /** returns physical_record_length_ for a new record */
  inline static uint16_t required_space(uint16_t key_length, uint16_t payload_length) {
    return assorted::align8(key_length) + assorted::align8(payload_length) + sizeof(Slot);
  }


  // search, insert, migrate, etc to manipulate records in this page

  /**
   * @brief A system transaction that creates a logically deleted record in this page
   * for the given key.
   * @param[in] hash hash value of the key.
   * @param[in] fingerprint Bloom Filter fingerprint of the key.
   * @param[in] key full key.
   * @param[in] key_length full key length.
   * @param[in] payload_length the new record can contain at least this length of payload
   * @return slot index of the newly created record
   * @pre the page is locked
   * @pre the page must not already contain a record with the exact key except it's moved
   * @pre required_space(key_length, payload_length) <= available_space()
   * @details
   * This method also adds the fingerprint to the bloom filter.
   */
  DataPageSlotIndex reserve_record(
    HashValue hash,
    const BloomFilterFingerprint& fingerprint,
    const char* key,
    uint16_t key_length,
    uint16_t payload_length);

  /** overload for HashCombo */
  inline DataPageSlotIndex reserve_record(const HashCombo& combo, uint16_t payload_length) {
    return reserve_record(
      combo.hash_,
      combo.fingerprint_,
      combo.key_,
      combo.key_length_,
      payload_length);
  }

  /**
   * @brief A simplified/efficient version to insert an active record, which must be used
   * only in snapshot pages.
   * @param[in] xct_id XctId of the record.
   * @param[in] hash hash value of the key.
   * @param[in] fingerprint Bloom Filter fingerprint of the key.
   * @param[in] key full key.
   * @param[in] key_length full key length.
   * @param[in] payload the payload of the new record
   * @param[in] payload_length length of payload
   * @pre the page is a snapshot page being constructed
   * @pre the page doesn't have the key
   * @pre required_space(key_length, payload_length) <= available_space()
   * @details
   * On constructing snapshot pages, we don't need any concurrency control, and we can
   * loosen all restrictions. We thus have this method as an optimized version for snapshot pages.
   * Inlined to make it more efficient.
   */
  void create_record_in_snapshot(
    xct::XctId xct_id,
    HashValue hash,
    const BloomFilterFingerprint& fingerprint,
    const void* key,
    uint16_t key_length,
    const void* payload,
    uint16_t payload_length) ALWAYS_INLINE;

  /**
   * @brief Search for a physical slot that exactly contains the given key.
   * @param[in] hash hash value of the key.
   * @param[in] fingerprint Bloom Filter fingerprint of the key.
   * @param[in] key full key.
   * @param[in] key_length full key length.
   * @param[in] record_count how many records this page \e supposedly contains. See below.
   * @param[out] observed When the key is found, we save its TID to this variable
   * @return index of the slot that has the key. kSlotNotFound if not found (including
   * the case where an exactly-matched record's TID says it's "moved").
   * @invariant hash == hashinate(key, key_length)
   * @details
   * If you have acquired record_count in a protected way (after a page lock, which you still keep)
   * then this method is an exact search. Otherwise, a concurrent thread might be now inserting,
   * so this method might have false negative (which is why you should take PageVersion for
   * miss-search). However, no false positive possible.
   *
   * This method searches for a physical slot no matter whether the record is logically deleted.
   * However, "moved" records are completely ignored.
   */
  DataPageSlotIndex search_key(
    HashValue hash,
    const BloomFilterFingerprint& fingerprint,
    const char* key,
    uint16_t key_length,
    uint16_t record_count,
    xct::XctId* observed) const;

  /** This version receives HashCombo */
  inline DataPageSlotIndex search_key(
    const HashCombo& combo,
    uint16_t record_count,
    xct::XctId* observed) const {
    return search_key(
      combo.hash_,
      combo.fingerprint_,
      combo.key_,
      combo.key_length_,
      record_count,
      observed);
  }

  /** returns whether the slot contains the exact key specified */
  inline bool compare_slot_key(
    DataPageSlotIndex index,
    HashValue hash,
    const char* key,
    uint16_t key_length) const {
    ASSERT_ND(index < get_record_count());  // record count purely increasing
    const Slot& slot = get_slot(index);
    ASSERT_ND(hashinate(record_from_offset(slot.offset_), slot.key_length_) == slot.hash_);
    // quick check first
    if (slot.hash_ != hash || slot.key_length_ != key_length) {
      return false;
    }
    const char* data = record_from_offset(slot.offset_);
    return std::memcmp(data, key, key_length) == 0;
  }
  inline bool compare_slot_key(DataPageSlotIndex index, const HashCombo& combo) const {
    ASSERT_ND(get_bin() == combo.bin_);
    return compare_slot_key(index, combo.hash_, combo.key_, combo.key_length_);
  }

  HashBin     get_bin() const { return bin_; }
  uint8_t     get_bin_bits() const { return bin_bits_; }
  uint8_t     get_bin_shifts() const { return bin_shifts_; }
  inline void assert_bin(HashBin bin) const ALWAYS_INLINE { ASSERT_ND(bin_ == bin); }

  void        assert_entries() const ALWAYS_INLINE {
#ifndef NDEBUG
    assert_entries_impl();
#endif  // NDEBUG
  }
  /** defined in hash_page_debug.cpp. */
  void        assert_entries_impl() const;
  /** defined in hash_page_debug.cpp. */
  friend std::ostream& operator<<(std::ostream& o, const HashDataPage& v);

 private:
  PageHeader      header_;        // +32 -> 32

  /**
   * When this hash bin receives many records or very long key or values,
   * this link forms a singly-linked list of pages.
   * Although this is a dual page pointer, we use only one of them.
   * Snapshot hash pages of course point to only snapshot hash pages.
   * Volatile hash pages, to simplify, also point to only volatile hash pages.
   */
  DualPagePointer next_page_;     // +16 -> 48

  /**
   * Used only for sanity check, so we actually don't need it. Kind of a padding.
   */
  HashBin         bin_;           // +8 -> 56
  /** same above. always same as storage's bin_bits */
  uint8_t         bin_bits_;      // +1 -> 57
  /** same above. always same as storage's bin_shifts */
  uint8_t         bin_shifts_;    // +1 -> 58
  uint8_t         paddings_[6];   // +6 -> 64

  /**
   * Registers the keys this page contains. 64 bytes might sound too generous, but
   * we anyway pre-fetch a first few cachelines in the header, so it wouldn't hurt.
   * If this is 16 bytes, header is just 1 cacheline, but then too small as a bloom filter.
   */
  DataPageBloomFilter bloom_filter_;  // +64 -> 128

  /**
   * Dynamic data part in this page, which consist of 1) key/payload part growing forward,
   * 2) unused part, and 3) Slot part growing backward.
   */
  char            data_[kPageSize - kHashDataPageHeaderSize];
};


/**
 * volatile page initialize callback for HashIntermediatePage.
 * @ingroup ARRAY
 * @see foedus::storage::VolatilePageInit
 */
void hash_intermediate_volatile_page_init(const VolatilePageInitArguments& args);

/**
 * volatile page initialize callback for HashDataPage.
 * @ingroup ARRAY
 * @see foedus::storage::VolatilePageInit
 */
void hash_data_volatile_page_init(const VolatilePageInitArguments& args);

inline void HashDataPage::create_record_in_snapshot(
  xct::XctId xct_id,
  HashValue hash,
  const BloomFilterFingerprint& fingerprint,
  const void* key_arg,
  uint16_t key_length,
  const void* payload_arg,
  uint16_t payload_length) {
  ASSERT_ND(header_.snapshot_);
  ASSERT_ND(available_space() >= required_space(key_length, payload_length));
  ASSERT_ND(reinterpret_cast<uintptr_t>(this) % kPageSize == 0);
  ASSERT_ND(reinterpret_cast<uintptr_t>(key_arg) % 8 == 0);
  ASSERT_ND(reinterpret_cast<uintptr_t>(payload_arg) % 8 == 0);

  const void* key = ASSUME_ALIGNED(key_arg, 8U);
  const void* payload = ASSUME_ALIGNED(payload_arg, 8U);
  DataPageSlotIndex index = get_record_count();
  Slot& slot = get_slot(index);
  slot.tid_.xct_id_ = xct_id;
  slot.offset_ = next_offset();
  slot.hash_ = hash;
  slot.key_length_ = key_length;
  uint16_t aligned_key_length = assorted::align8(key_length);
  slot.physical_record_length_ = aligned_key_length + assorted::align8(payload_length);
  slot.payload_length_ = payload_length;

  ASSERT_ND(reinterpret_cast<uintptr_t>(record_from_offset(slot.offset_)) % 8 == 0);
  char* record = reinterpret_cast<char*>(ASSUME_ALIGNED(record_from_offset(slot.offset_), 8U));

  ASSERT_ND(key_length > 0);
  std::memcpy(record, key, key_length);
  if (key_length != aligned_key_length) {
    std::memset(record + key_length, 0, aligned_key_length - key_length % 8);
  }

  if (payload_length > 0) {
    char* dest = reinterpret_cast<char*>(ASSUME_ALIGNED(record + aligned_key_length, 8U));
    uint16_t aligned_payload_length = assorted::align8(payload_length);
    std::memcpy(dest, payload, payload_length);
    if (key_length != aligned_payload_length) {
      std::memset(dest + payload_length, 0, aligned_payload_length - payload_length);
    }
  }

  bloom_filter_.add(fingerprint);

  header_.increment_key_count();
}


static_assert(
  sizeof(HashIntermediatePage) == kPageSize,
  "sizeof(HashIntermediatePage) is not kPageSize");

static_assert(
  sizeof(HashDataPage) == kPageSize,
  "sizeof(HashDataPage) is not kPageSize");

}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_PAGE_HPP_
