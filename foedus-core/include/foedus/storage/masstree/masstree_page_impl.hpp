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
#include "foedus/assorted/cacheline.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_page_version.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/**
 * @brief Common base of MasstreeIntermediatePage and MasstreeBorderPage.
 * @ingroup MASSTREE
 * @details
 * Do NOT use sizeof on this class because it is smaller than kPageSize.
 * To be a base class of two page types, this class defines only the common properties.
 * Also, as usual, no virtual methods! We just reinterpret byte arrays.
 */
class MasstreePage {
 public:
  MasstreePage() = delete;
  MasstreePage(const MasstreePage& other) = delete;
  MasstreePage& operator=(const MasstreePage& other) = delete;

  // simple accessors
  PageHeader&         header() { return header_; }
  const PageHeader&   header() const { return header_; }

  KeySlice            get_low_fence() const { return low_fence_; }
  KeySlice            get_high_fence() const { return high_fence_; }
  MasstreeIntermediatePage* get_in_layer_parent() const { return in_layer_parent_; }

  /** Layer-0 stores the first 8 byte slice, Layer-1 next 8 byte... */
  uint8_t             get_layer() const { return page_version_.get_layer(); }

  /**
   * prefetch upto keys/separators, whether this page is border or interior.
   * Use this to prefetch a page that is not sure border or interior.
   * Checking the page type itself has to read the header, so just do it conservatively.
   * 4 cachelines too much? that's a different argument...
   */
  void                prefetch_general() const {
    assorted::prefetch_cachelines(this, 4);  // max(border's prefetch, interior's prefetch)
  }

  /**
   * @brief Spins until we observe a non-inserting and non-splitting version.
   * @return version of this page that wasn't during modification.
   */
  MasstreePageVersion get_stable_version() const ALWAYS_INLINE {
    return page_version_.stable_version();
  }
  const MasstreePageVersion& get_version() const ALWAYS_INLINE { return page_version_; }
  MasstreePageVersion& get_version() ALWAYS_INLINE { return page_version_; }

  /**
   * @brief Locks the page, spinning if necessary.
   * @details
   * After taking lock, you might want to additionally set inserting/splitting bits.
   * Those can be done just as a usual write once you get a lock.
   */
  void              lock() ALWAYS_INLINE {
    if (!header_.snapshot_) {
      page_version_.lock_version();
    }
  }

  /**
   * @brief Unlocks the page, assuming the caller has locked it.
   * @pre !header_.snapshot_ (only for volatile pages)
   * @pre page_version_ & kPageVersionLockedBit (we must have locked it)
   * @pre this thread locked it (can't check it, but this is the rule)
   * @details
   * This method also takes fences before/after unlock to make it safe.
   */
  void              unlock() ALWAYS_INLINE {
    if (!header_.snapshot_) {
      page_version_.unlock_version();
    }
  }

  void              release_pages_recursive_common(
    const memory::GlobalVolatilePageResolver& page_resolver,
    memory::PageReleaseBatch* batch);

 protected:
  PageHeader          header_;      // +32 -> 32

  /** Inclusive low fence of this page. Mainly used for sanity checking */
  KeySlice            low_fence_;   // +8 -> 40
  /** Inclusive high fence of this page. Mainly used for sanity checking */
  KeySlice            high_fence_;  // +8 -> 48

  /**
   * Similar to header_.volatile_parent_. The difference is that this is null if the page
   * is the root of a non-first layer whereas header_.volatile_parent_ points to previous layer's
   * border node.
   */
  MasstreeIntermediatePage* in_layer_parent_;  // +8 -> 56

  MasstreePageVersion page_version_;  // +8 -> 64

  void                initialize_volatile_common(
    StorageId storage_id,
    VolatilePagePointer page_id,
    PageType page_type,
    uint8_t layer,
    MasstreePage* parent);
};

/**
 * @brief Represents one intermediate page in \ref MASSTREE.
 * @ingroup MASSTREE
 * @details
 * An intermediate page consists of bunch of separator keys and pointers to children nodes,
 * which might be another intermediate pages or border nodes.
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class MasstreeIntermediatePage final : public MasstreePage {
 public:
  struct MiniPage {
    // +8 -> 8
    MasstreePageVersion mini_version_;

    // +8*15 -> 128
    /** Same semantics as separators_ in enclosing class. */
    KeySlice        separators_[kMaxIntermediateMiniSeparators];
    // +16*16 -> 384
    DualPagePointer pointers_[kMaxIntermediateMiniSeparators + 1];

    /** prefetch upto separators. */
    void prefetch() const {
      assorted::prefetch_cachelines(this, 2);
    }
    MasstreePageVersion get_stable_version() const ALWAYS_INLINE {
      return mini_version_.stable_version();
    }
    /**
    * @brief Navigates a searching key-slice to one of pointers in this mini-page.
    */
    uint8_t find_pointer(const MasstreePageVersion &stable, KeySlice slice) const ALWAYS_INLINE {
      uint8_t separator_count = stable.get_key_count();
      ASSERT_ND(separator_count <= kMaxIntermediateMiniSeparators);
      for (uint8_t i = 0; i < separator_count; ++i) {
        if (slice < separators_[i]) {
          return i;
        }
      }
      return separator_count;
    }
  };

  // A page object is never explicitly instantiated. You must reinterpret_cast.
  MasstreeIntermediatePage() = delete;
  MasstreeIntermediatePage(const MasstreeIntermediatePage& other) = delete;
  MasstreeIntermediatePage& operator=(const MasstreeIntermediatePage& other) = delete;

  /** prefetch upto separators. */
  void prefetch() const {
    assorted::prefetch_cachelines(this, 3);
  }

  /**
   * @brief Navigates a searching key-slice to one of the mini pages in this page.
   */
  uint8_t find_minipage(const MasstreePageVersion &stable, KeySlice slice) const ALWAYS_INLINE {
    uint8_t separator_count = stable.get_key_count();
    ASSERT_ND(separator_count <= kMaxIntermediateSeparators);
    for (uint8_t i = 0; i < separator_count; ++i) {
      if (slice < separators_[i]) {
        return i;
      }
    }
    return separator_count;
  }

  MiniPage&         get_minipage(uint8_t index) ALWAYS_INLINE { return mini_pages_[index]; }
  const MiniPage&   get_minipage(uint8_t index) const ALWAYS_INLINE { return mini_pages_[index]; }

  void              release_pages_recursive(
    const memory::GlobalVolatilePageResolver& page_resolver,
    memory::PageReleaseBatch* batch);

 private:
  // 64

  /**
   * Separators to navigate search to mini pages in this page.
   * Iff separators_[i-1] <= Slice < separators_[i], the search is navigated to mini_pages_[i].
   * Iff Slice < separators_[0] or key_count==0, mini_pages_[0].
   * Iff Slice >= separators_[key_count-1] or key_count==0, mini_pages_[key_count].
   */
  KeySlice            separators_[kMaxIntermediateSeparators];  // +72 -> 136

  char                reserved_[120];    // -> 256

  MiniPage            mini_pages_[10];  // +384 * 10 -> 4096
};
STATIC_SIZE_CHECK(sizeof(MasstreeIntermediatePage::MiniPage), 128 + 256)
STATIC_SIZE_CHECK(sizeof(MasstreeIntermediatePage), 1 << 12)

/**
 * @brief Represents one border page in \ref MASSTREE.
 * @ingroup MASSTREE
 * @details
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class MasstreeBorderPage final : public MasstreePage {
 public:
  enum Constants {
    kMaxKeys = 128,
    /** XctId and at least DualPagePointer size. same as sizeof(LayerRecord) */
    kMinRecordSize = 32,
    /** 4096 - 64 */
    kDataSize = 4032,
    /** 4096 - 64 - 24 (=min record size)*/
    kMaxRecordOffset = 4008,
    /**
     * If this bit in flags_ is on, the stored record points to next layer.
     * When this flag is on, the payload of the record is DualPagePointer.
     */
    kSlotFlagLayer = 0x0001,
  };
  /** Used in FindKeyForReserveResult */
  enum MatchType {
    kNotFound = 0,
    kExactMatchLocalRecord = 1,
    kExactMatchLayerPointer = 2,
    kConflictingLocalRecord = 3,
  };

  /**
   * Forward-growing slots of this page.
   * One slot is reserved for one \e physical record, which is never moved except snapshotting.
   * A thread first installs a new record by atomically modifying border_page_status_, then
   * set up the record with deletion flag on, then set up the slot with stable flag, upto here
   * no failure is allowed, and it always succeeds (because there is only one thread that
   * atomically incremented record count to the specific number).
   * Flipping the delete flag of the record is done by apply() of transaction, which might fail.
   * If it fails, the record is left as deleted until snapshotting. No reuse.
   * Everything is cleared up while snapshotting, so why bother.
   */
  struct Slot {
    /**
     * @invariant slice[0] to slice[key_count_ - 1] are all different.
     * @invariant slice[0] to slice[key_count_ - 1] are all exclusively within fence keys.
     */
    KeySlice slice_;

    /**
     * Stores key length excluding previous layers, but including this layer (which might be less
     * than 8!) and suffix if exists.
     * If this points to next layer, this value has not much meaning. The only contract is that
     * the value is 9 or more..
     */
    uint16_t remaining_key_length_;
    /**
     * length of only the pure payload. If this is smaller than sizeof(DualPagePointer),
     * we actually reserve at least sizeof(DualPagePointer)
     * so that we can later replace this record to next-layer pointer.
     */
    uint16_t payload_length_;
    /** Offset of the beginning of record. */
    uint16_t offset_;
    /** Various bit flags of this record. */
    uint16_t flags_;

    bool does_point_to_layer () const ALWAYS_INLINE { return (flags_ & kSlotFlagLayer) != 0; }
    uint16_t get_suffix_length() const ALWAYS_INLINE {
      if (remaining_key_length_ >= sizeof(KeySlice)) {
        return remaining_key_length_ - sizeof(KeySlice);
      } else {
        return 0;
      }
    }
  };
  struct LayerRecord {
    xct::XctId      owner_id_;
    /** To make pointer_ 16 byte-aligned. (Do we need to do so?) */
    uint64_t        dummy_;
    DualPagePointer pointer_;
  };

  /** return value for find_key_for_reserve(). POD. */
  struct FindKeyForReserveResult {
    FindKeyForReserveResult(uint8_t index, MatchType match_type)
      : index_(index), match_type_(match_type) {}
    uint8_t index_;
    MatchType match_type_;
  };

  // A page object is never explicitly instantiated. You must reinterpret_cast.
  MasstreeBorderPage() = delete;
  MasstreeBorderPage(const MasstreeBorderPage& other) = delete;
  MasstreeBorderPage& operator=(const MasstreeBorderPage& other) = delete;

  void initialize_volatile_page(
    StorageId storage_id,
    VolatilePagePointer page_id,
    uint8_t layer,
    MasstreePage* parent);

  /** prefetch upto keys. */
  void prefetch() const {
    assorted::prefetch_cachelines(this, 4);
  }

  /**
   * @brief Navigates a searching key-slice to one of the mini pages in this page.
   * @return index of key found in this page, or kMaxKeys if not found.
   */
  uint8_t find_key(
    const MasstreePageVersion &stable,
    KeySlice slice,
    const void* suffix,
    uint16_t remaining) const ALWAYS_INLINE;

  /**
   * Specialized version for 8 byte native integer search. Because such a key never goes to
   * second layer, this is much simpler.
   */
  uint8_t find_key_normalized(
    uint8_t from_index,
    uint8_t to_index,
    KeySlice slice) const ALWAYS_INLINE;


  /**
   * This is for the case we are looking for either the matching slot or the slot we will modify.
   */
  FindKeyForReserveResult find_key_for_reserve(
    uint8_t from_index,
    uint8_t to_index,
    KeySlice slice,
    const void* suffix,
    uint16_t remaining) const ALWAYS_INLINE;

  const Slot& get_slot(uint8_t index) const ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    return reinterpret_cast<const Slot*>(data_)[index];
  }
  Slot& get_slot(uint8_t index) ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    return reinterpret_cast<Slot*>(data_)[index];
  }

  LayerRecord* layer_record(uint16_t offset) ALWAYS_INLINE {
    ASSERT_ND(offset <= kMaxRecordOffset);
    return reinterpret_cast<LayerRecord*>(data_ + offset);
  }
  const LayerRecord* layer_record(uint16_t offset) const ALWAYS_INLINE {
    ASSERT_ND(offset <= kMaxRecordOffset);
    return reinterpret_cast<const LayerRecord*>(data_ + offset);
  }

  Record* body_record(uint16_t offset) ALWAYS_INLINE {
    ASSERT_ND(offset <= kMaxRecordOffset);
    return reinterpret_cast<Record*>(data_ + offset);
  }
  const Record* body_record(uint16_t offset) const ALWAYS_INLINE {
    ASSERT_ND(offset <= kMaxRecordOffset);
    return reinterpret_cast<const Record*>(data_ + offset);
  }

  uint16_t calculate_suffix_length(uint16_t remaining_length) const ALWAYS_INLINE {
    if (remaining_length >= sizeof(KeySlice)) {
      return remaining_length - sizeof(KeySlice);
    } else {
      return 0;
    }
  }
  uint16_t calculate_record_size(
    uint16_t remaining_length,
    uint16_t payload_count) const ALWAYS_INLINE {
    uint16_t suffix_length = calculate_suffix_length(remaining_length);
    uint16_t record_size = assorted::align16(suffix_length + payload_count + kRecordOverhead);
    if (record_size < kMinRecordSize) {
      record_size = kMinRecordSize;
    }
    return record_size;
  }

  bool    can_accomodate(
    uint8_t new_index,
    uint16_t remaining_length,
    uint16_t payload_count) const ALWAYS_INLINE {
    if (new_index == 0) {
      ASSERT_ND(remaining_length + payload_count <= 4000);
      return true;
    }
    uint16_t record_size = calculate_record_size(remaining_length, payload_count);
    const Slot& last_slot = get_slot(new_index - 1);
    return (new_index + 1) * sizeof(Slot) + record_size <= last_slot.offset_;
  }
  /**
   * Installs a new physical record that doesn't exist logically (delete bit on).
   * This sets 1) slot, 2) suffix key, and 3) XctId. Payload is not set yet.
   * This is executed as a system transaction.
   */
  void    reserve_record_space(
    uint8_t index,
    xct::XctId initial_owner_id,
    KeySlice slice,
    const void* suffix,
    uint16_t remaining_length,
    uint16_t payload_count);

  /**
   * Copy the initial record that will be the only record for a new root page.
   * This is called when a new layer is created, and done in a thread-private memory.
   * So, no synchronization needed.
   */
  void    copy_initial_record(const MasstreeBorderPage* copy_from, uint8_t copy_index);

  void    release_pages_recursive(
    const memory::GlobalVolatilePageResolver& page_resolver,
    memory::PageReleaseBatch* batch);

 private:
  // 64

  /**
   * The main data region of this page. which consists of forward-growing slots and
   * backward-growing record part.
   */
  char                  data_[kDataSize];
};
STATIC_SIZE_CHECK(sizeof(MasstreeBorderPage::Slot), 16)
STATIC_SIZE_CHECK(sizeof(MasstreeBorderPage), 1 << 12)

inline uint8_t MasstreeBorderPage::find_key(
  const MasstreePageVersion &stable,
  KeySlice slice,
  const void* suffix,
  uint16_t remaining) const {
  uint8_t key_count = stable.get_key_count();
  ASSERT_ND(key_count <= kMaxKeys);
  for (uint8_t i = 0; i < key_count; ++i) {
    if (i == 8U && key_count > 12U) {
      // one slot is 16 bytes. we initially prefetched 64*4 = 256 bytes = header + 12 slots.
      // if we will read more than that, prefetch now.
      assorted::prefetch_cachelines(data_ + 256 - 64, 4);
    }

    const Slot& slot = get_slot(i);
    if (slice != slot.slice_) {
      continue;
    }
    // one slice might be used for up to 10 keys, length 0 to 8 and pointer to next layer.
    if (remaining <= sizeof(KeySlice)) {
      // no suffix nor next layer, so just compare length
      if (slot.remaining_key_length_ == remaining) {
        ASSERT_ND(!slot.does_point_to_layer());
        return i;
      } else {
        continue;  // did not match
      }
    }
    ASSERT_ND(remaining > sizeof(KeySlice));  // keep this in mind below

    if (slot.does_point_to_layer()) {
      // as it points to next layer, no suffix.
      // so far we don't delete layers, so in this case the record is always valid.
      ASSERT_ND(slot.remaining_key_length_ > sizeof(KeySlice));
      return i;
    }

    ASSERT_ND(!slot.does_point_to_layer());

    // now, our key is > 8 bytes and we found some local record.
    if (slot.remaining_key_length_ == remaining) {
      // compare suffix.
      const char* record_suffix = data_ + slot.offset_ + kRecordOverhead;
      if (std::memcmp(record_suffix, suffix, remaining - sizeof(KeySlice)) == 0) {
        return i;
      }
    }

    // suppose the record has > 8 bytes key. it must be the only such record in this page
    // because otherwise we must have created a next layer!
    if (slot.remaining_key_length_ > sizeof(KeySlice)) {
      break;  // no more check needed
    } else {
      continue;
    }
  }
  return kMaxKeys;
}

inline uint8_t MasstreeBorderPage::find_key_normalized(
  uint8_t from_index,
  uint8_t to_index,
  KeySlice slice) const {
  ASSERT_ND(to_index <= kMaxKeys);
  ASSERT_ND(from_index <= to_index);
  for (uint8_t i = from_index; i < to_index; ++i) {
    if (i == 8U && to_index > 12U) {
      assorted::prefetch_cachelines(data_ + 256 - 64, 4);
    }

    const Slot& slot = get_slot(i);
    if (slice == slot.slice_ && slot.remaining_key_length_ == sizeof(KeySlice)) {
      ASSERT_ND(!slot.does_point_to_layer());
      return i;
    }
  }
  return kMaxKeys;
}

inline MasstreeBorderPage::FindKeyForReserveResult MasstreeBorderPage::find_key_for_reserve(
  uint8_t from_index,
  uint8_t to_index,
  KeySlice slice,
  const void* suffix,
  uint16_t remaining) const {
  ASSERT_ND(to_index <= kMaxKeys);
  ASSERT_ND(from_index <= to_index);
  for (uint8_t i = from_index; i < to_index; ++i) {
    if (i == 8U && to_index > 12U) {
      assorted::prefetch_cachelines(data_ + 256 - 64, 4);
    }

    const Slot& slot = get_slot(i);
    if (slice != slot.slice_) {
      continue;
    }
    if (remaining <= sizeof(KeySlice)) {
      if (slot.remaining_key_length_ == remaining) {
        ASSERT_ND(!slot.does_point_to_layer());
        return FindKeyForReserveResult(i, kExactMatchLocalRecord);
      } else {
        continue;
      }
    }
    ASSERT_ND(remaining > sizeof(KeySlice));

    if (slot.does_point_to_layer()) {
      ASSERT_ND(slot.remaining_key_length_ > sizeof(KeySlice));
      return FindKeyForReserveResult(i, kExactMatchLayerPointer);
    }

    ASSERT_ND(!slot.does_point_to_layer());

    if (slot.remaining_key_length_ <= sizeof(KeySlice)) {
      continue;
    }

    // now, both the searching key and this key are more than 8 bytes.
    // whether the key really matches or not, this IS the slot we are looking for.
    // Either 1) the keys really match, or 2) we will make this record point to next layer.
    const char* record_suffix = data_ + slot.offset_ + kRecordOverhead;
    if (slot.remaining_key_length_ == remaining &&
      std::memcmp(record_suffix, suffix, remaining - sizeof(KeySlice)) == 0) {
      // case 1)
      return FindKeyForReserveResult(i, kExactMatchLocalRecord);
    } else {
      // case 2)
      return FindKeyForReserveResult(i, kConflictingLocalRecord);
    }
  }
  return FindKeyForReserveResult(kMaxKeys, kNotFound);
}

inline void MasstreeBorderPage::reserve_record_space(
  uint8_t index,
  xct::XctId initial_owner_id,
  KeySlice slice,
  const void* suffix,
  uint16_t remaining_length,
  uint16_t payload_count) {
  ASSERT_ND(page_version_.is_locked());
  ASSERT_ND(page_version_.is_inserting());
  ASSERT_ND(page_version_.get_key_count() == index + 1U);
  ASSERT_ND(can_accomodate(index, remaining_length, payload_count));
  uint16_t suffix_length = calculate_suffix_length(remaining_length);
  uint16_t record_size = calculate_record_size(remaining_length, payload_count);
  uint16_t previous_offset;
  if (index == 0) {
    previous_offset = kDataSize;
  } else {
    previous_offset = get_slot(index - 1).offset_;
  }
  Slot& slot = get_slot(index);
  slot.slice_ = slice;
  slot.remaining_key_length_ = remaining_length;
  slot.payload_length_ = payload_count;
  slot.offset_ = previous_offset - record_size;
  slot.flags_ = 0;
  Record* record = body_record(slot.offset_);
  record->owner_id_ = initial_owner_id;
  if (suffix_length > 0) {
    std::memcpy(record->payload_, suffix, suffix_length);
  }
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_IMPL_HPP_
