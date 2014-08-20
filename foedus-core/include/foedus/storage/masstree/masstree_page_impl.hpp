/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_IMPL_HPP_

#include <stdint.h>

#include <algorithm>
#include <cstring>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_code.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/** Used only for debugging as this is not space efficient. */
struct HighFence {
  HighFence(KeySlice slice, bool supremum) : slice_(slice), supremum_(supremum) {}
  KeySlice slice_;
  bool supremum_;
};

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

  bool                is_border() const ALWAYS_INLINE {
    ASSERT_ND(header_.get_page_type() == kMasstreeBorderPageType ||
      header_.get_page_type() == kMasstreeIntermediatePageType);
    return header_.get_page_type() == kMasstreeBorderPageType;
  }
  KeySlice            get_low_fence() const ALWAYS_INLINE { return low_fence_; }
  KeySlice            get_high_fence() const ALWAYS_INLINE { return high_fence_; }
  bool                is_high_fence_supremum() const ALWAYS_INLINE {
    return high_fence_ == kSupremumSlice;
  }
  KeySlice            get_foster_fence() const ALWAYS_INLINE { return foster_fence_; }
  MasstreePage*       get_foster_minor() const ALWAYS_INLINE { return foster_twin_[0]; }
  MasstreePage*       get_foster_major() const ALWAYS_INLINE { return foster_twin_[1]; }

  bool                within_fences(KeySlice slice) const ALWAYS_INLINE {
    return slice >= low_fence_ && (is_high_fence_supremum() || slice < high_fence_);
  }
  bool                within_foster_minor(KeySlice slice) const ALWAYS_INLINE {
    ASSERT_ND(within_fences(slice));
    ASSERT_ND(has_foster_child());
    return slice < foster_fence_;
  }
  bool                within_foster_major(KeySlice slice) const ALWAYS_INLINE {
    ASSERT_ND(within_fences(slice));
    ASSERT_ND(has_foster_child());
    return slice >= foster_fence_;
  }
  bool                has_foster_child() const ALWAYS_INLINE {
    return header_.page_version_.is_moved();
  }

  /** Layer-0 stores the first 8 byte slice, Layer-1 next 8 byte... */
  uint8_t             get_layer() const ALWAYS_INLINE { return header_.masstree_layer_; }
  /** \e physical key count (those keys might be deleted) in this page. */
  uint16_t            get_key_count() const ALWAYS_INLINE { return header_.key_count_; }
  void                set_key_count(uint16_t count) ALWAYS_INLINE { header_.set_key_count(count); }
  void                increment_key_count() ALWAYS_INLINE { header_.increment_key_count(); }

  /**
   * prefetch upto keys/separators, whether this page is border or interior.
   * Use this to prefetch a page that is not sure border or interior.
   * Checking the page type itself has to read the header, so just do it conservatively.
   * 4 cachelines too much? that's a different argument...
   */
  void                prefetch_general() const ALWAYS_INLINE {
    assorted::prefetch_cachelines(this, 4);  // max(border's prefetch, interior's prefetch)
  }

  const PageVersion& get_version() const ALWAYS_INLINE { return header_.page_version_; }
  PageVersion& get_version() ALWAYS_INLINE { return header_.page_version_; }
  const PageVersion* get_version_address() const ALWAYS_INLINE { return &header_.page_version_; }
  PageVersion* get_version_address() ALWAYS_INLINE { return &header_.page_version_; }
  xct::McsLock* get_lock_address() ALWAYS_INLINE { return &header_.page_version_.lock_; }

  /**
   * @brief Locks the page, spinning if necessary.
   */
  void              lock(thread::Thread* context) ALWAYS_INLINE {
    if (!header_.snapshot_) {
      header_.page_version_.lock(context);
    }
  }
  bool              is_locked() const ALWAYS_INLINE { return header_.page_version_.is_locked(); }
  bool              is_moved() const ALWAYS_INLINE { return header_.page_version_.is_moved(); }
  bool              is_retired() const ALWAYS_INLINE { return header_.page_version_.is_retired(); }
  void              set_moved() ALWAYS_INLINE { header_.page_version_.set_moved(); }
  void              set_retired() ALWAYS_INLINE { header_.page_version_.set_retired(); }

  void              release_pages_recursive_common(
    const memory::GlobalVolatilePageResolver& page_resolver,
    memory::PageReleaseBatch* batch);

 protected:
  PageHeader          header_;      // +32 -> 32

  /** Inclusive low fence of this page. Mainly used for sanity checking */
  KeySlice            low_fence_;   // +8 -> 40
  /** Inclusive high fence of this page. Mainly used for sanity checking */
  KeySlice            high_fence_;  // +8 -> 48
  /** Inclusive low_fence of foster child. undefined if foster child is not set*/
  KeySlice            foster_fence_;  // +8 -> 56

  /**
   * tentative child page whose key ranges are right-half of this page.
   * undefined if has_foster_child of page_version is false.
   * In case of intermediate page, this is the only foster child.
   * In case of border page, this is the major foster child. minor foster child is stored
   * in another field.
   *
   * When this page is fostering kids, this is the younger brother, or the new page of
   * this page itself. This is null if the split was a no-record split.
   * This is core of the foster-twin protocol.
   */
  MasstreePage*       foster_twin_[2];  // +16 -> 72

  void                initialize_volatile_common(
    StorageId           storage_id,
    VolatilePagePointer page_id,
    PageType            page_type,
    uint8_t             layer,
    KeySlice            low_fence,
    KeySlice            high_fence);
};

struct BorderSplitStrategy {
  /**
   * whether this page seems to have had sequential insertions, in which case we do
   * "no-record split" as optimization. This also requires the trigerring insertion key
   * is equal or larger than the largest slice in this page.
   */
  bool no_record_split_;
  uint16_t original_key_count_;
  KeySlice smallest_slice_;
  KeySlice largest_slice_;
  /**
   * This will be the new foster fence.
   * Ideally, #records below and above this are same.
   */
  KeySlice mid_slice_;
};

/**
 * Constructed by hierarchically reading all separators and pointers in old page.
 */
struct IntermediateSplitStrategy {
  enum Constants {
    kMaxSeparators = 170,
  };
  /**
   * pointers_[n] points to page that is responsible for keys
   * separators_[n - 1] <= key < separators_[n].
   * separators_[-1] is infimum.
   */
  KeySlice separators_[kMaxSeparators];  // ->1360
  DualPagePointer pointers_[kMaxSeparators];  // -> 4080
  KeySlice mid_separator_;  // -> 4088
  uint16_t total_separator_count_;  // -> 4090
  uint16_t mid_index_;  // -> 4092
  uint32_t dummy_;      // -> 4096
};
STATIC_SIZE_CHECK(sizeof(IntermediateSplitStrategy), 1 << 12)

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
    MiniPage() = delete;
    MiniPage(const MiniPage& other) = delete;
    MiniPage& operator=(const MiniPage& other) = delete;

    // +8 -> 8
    uint8_t         key_count_;
    uint8_t         reserved_[7];

    // +8*15 -> 128
    /** Same semantics as separators_ in enclosing class. */
    KeySlice        separators_[kMaxIntermediateMiniSeparators];
    // +16*16 -> 384
    DualPagePointer pointers_[kMaxIntermediateMiniSeparators + 1];

    /** prefetch upto separators. */
    void prefetch() const {
      assorted::prefetch_cachelines(this, 2);
    }
    /**
    * @brief Navigates a searching key-slice to one of pointers in this mini-page.
    */
    uint8_t find_pointer(KeySlice slice) const ALWAYS_INLINE {
      uint8_t key_count = key_count_;
      ASSERT_ND(key_count <= kMaxIntermediateMiniSeparators);
      for (uint8_t i = 0; i < key_count; ++i) {
        if (slice < separators_[i]) {
          return i;
        }
      }
      return key_count;
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
  uint8_t find_minipage(KeySlice slice) const ALWAYS_INLINE {
    uint8_t key_count = get_key_count();
    ASSERT_ND(key_count <= kMaxIntermediateSeparators);
    for (uint8_t i = 0; i < key_count; ++i) {
      if (slice < separators_[i]) {
        return i;
      }
    }
    return key_count;
  }

  MiniPage&         get_minipage(uint8_t index) ALWAYS_INLINE { return mini_pages_[index]; }
  const MiniPage&   get_minipage(uint8_t index) const ALWAYS_INLINE { return mini_pages_[index]; }
  KeySlice          get_separator(uint8_t index) const ALWAYS_INLINE { return separators_[index]; }

  void              release_pages_recursive(
    const memory::GlobalVolatilePageResolver& page_resolver,
    memory::PageReleaseBatch* batch);

  void initialize_volatile_page(
    StorageId           storage_id,
    VolatilePagePointer page_id,
    uint8_t             layer,
    KeySlice            low_fence,
    KeySlice            high_fence);

  /**
   * Splits this page as a physical-only operation, creating a new foster twin, adopting
   * the given child to one of them.
   * @param[in] context Thread context
   * @param[in,out] trigger_child the child page that has a foster child which caused this split.
   * @pre !header_.snapshot_ (split happens to only volatile pages)
   * @pre is_locked() (the page must be locked)
   */
  ErrorCode split_foster_and_adopt(thread::Thread* context, MasstreePage* trigger_child);

  /**
   * @brief Adopts a foster-child of given child as an entry in this page.
   * @pre this and child pages are volatile pages (snapshot pages don't have foster child,
   * so this is always trivially guaranteed).
   * @details
   * This method doesn't assume this and other pages are locked.
   * So, when we lock child, we might find out that the foster child is already adopted.
   * In that case, and in other cases where adoption is impossible, we do nothing.
   * This method can also cause split.
   */
  ErrorCode adopt_from_child(
    thread::Thread* context,
    KeySlice searching_slice,
    uint8_t minipage_index,
    uint8_t pointer_index,
    MasstreePage* child);


  void verify_separators() const;

 private:
  // 72

  /**
   * Separators to navigate search to mini pages in this page.
   * Iff separators_[i-1] <= Slice < separators_[i], the search is navigated to mini_pages_[i].
   * Iff Slice < separators_[0] or key_count==0, mini_pages_[0].
   * Iff Slice >= separators_[key_count-1] or key_count==0, mini_pages_[key_count].
   */
  KeySlice            separators_[kMaxIntermediateSeparators];  // +72 -> 144

  char                reserved_[112];    // -> 256

  MiniPage            mini_pages_[10];  // +384 * 10 -> 4096

  ErrorCode local_rebalance(thread::Thread* context);
  void split_foster_decide_strategy(IntermediateSplitStrategy* out) const;
  void split_foster_migrate_records(
    const IntermediateSplitStrategy &strategy,
    uint16_t from,
    uint16_t to,
    KeySlice expected_last_separator);
  void adopt_from_child_norecord_first_level(
    thread::Thread* context,
    uint8_t minipage_index,
    MasstreePage* child);
};
STATIC_SIZE_CHECK(sizeof(MasstreeIntermediatePage::MiniPage), 128 + 256)
STATIC_SIZE_CHECK(sizeof(MasstreeIntermediatePage), 1 << 12)

/**
 * @brief Represents one border page in \ref MASSTREE.
 * @ingroup MASSTREE
 * @details
 * @par Slots
 * One border page has at most 64 slots.
 * One slot is reserved for one \e physical record, which is never moved except snapshotting
 * and split/compact.
 * A thread first installs a new record by atomically modifying page_version, then
 * set up the record with deletion flag on.
 * Flipping the delete flag of the record is done by apply() of transaction, which might fail.
 * If it fails, the record is left as deleted until snapshotting or split/compact.
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class MasstreeBorderPage final : public MasstreePage {
 public:
  /**
   * Represents offset of records in data_.
   * This is divided by 16 (up to 16*256=4kb to represent) because we align records in 16 bytes.
   */
  typedef uint8_t DataOffset;

  enum Constants {
    /** Max number of keys in this page */
    kMaxKeys = 64,

    /** Special value for remaining_key_length_. Means it now points to next layer. */
    kKeyLengthNextLayer = 255,
    /** Maximum value for remaining_key_length_. */
    kKeyLengthMax = 254,

    kDataSize = 4096 - 1872,
  };
  /** Used in FindKeyForReserveResult */
  enum MatchType {
    kNotFound = 0,
    kExactMatchLocalRecord = 1,
    kExactMatchLayerPointer = 2,
    kConflictingLocalRecord = 3,
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
    StorageId           storage_id,
    VolatilePagePointer page_id,
    uint8_t             layer,
    KeySlice            low_fence,
    KeySlice            high_fence);

  /**
   * @brief Navigates a searching key-slice to one of the mini pages in this page.
   * @return index of key found in this page, or kMaxKeys if not found.
   */
  uint8_t find_key(
    KeySlice slice,
    const void* suffix,
    uint8_t remaining) const ALWAYS_INLINE;

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
    uint8_t remaining) const ALWAYS_INLINE;

  char* get_record(uint8_t index) ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    ASSERT_ND(offsets_[index] < (kDataSize >> 4));
    return data_ + (static_cast<uint16_t>(offsets_[index]) << 4);
  }
  const char* get_record(uint8_t index) const ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    ASSERT_ND(offsets_[index] < (kDataSize >> 4));
    return data_ + (static_cast<uint16_t>(offsets_[index]) << 4);
  }
  DualPagePointer* get_next_layer(uint8_t index) ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    ASSERT_ND(offsets_[index] < (kDataSize >> 4));
    return reinterpret_cast<DualPagePointer*>(
      (data_ + (static_cast<uint16_t>(offsets_[index]) << 4)));
  }
  const DualPagePointer* get_next_layer(uint8_t index) const ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    ASSERT_ND(offsets_[index] < (kDataSize >> 4));
    return reinterpret_cast<const DualPagePointer*>(
      (data_ + (static_cast<uint16_t>(offsets_[index]) << 4)));
  }
  bool does_point_to_layer(uint8_t index) const ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    return remaining_key_length_[index] == kKeyLengthNextLayer;
  }

  KeySlice get_slice(uint8_t index) const ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    return slices_[index];
  }
  uint16_t get_offset_in_bytes(uint8_t index) const ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    return static_cast<uint16_t>(offsets_[index]) << 4;
  }

  xct::LockableXctId* get_owner_id(uint8_t index) ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    return owner_ids_ + index;
  }
  const xct::LockableXctId* get_owner_id(uint8_t index) const ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    return owner_ids_ + index;
  }

  uint16_t get_remaining_key_length(uint8_t index) const ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    return remaining_key_length_[index];
  }
  uint16_t get_suffix_length(uint8_t index) const ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    ASSERT_ND(!does_point_to_layer(index));
    if (remaining_key_length_[index] <= sizeof(KeySlice)) {
      return 0;
    } else {
      return remaining_key_length_[index] - sizeof(KeySlice);
    }
  }
  uint16_t get_payload_length(uint8_t index) const ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    return payload_length_[index];
  }

  static uint8_t calculate_suffix_length(uint8_t remaining_length) ALWAYS_INLINE {
    ASSERT_ND(remaining_length != kKeyLengthNextLayer);
    if (remaining_length >= sizeof(KeySlice)) {
      return remaining_length - sizeof(KeySlice);
    } else {
      return 0;
    }
  }
  static uint16_t calculate_record_size(
    uint8_t remaining_length,
    uint16_t payload_count) ALWAYS_INLINE {
    uint16_t suffix_length = calculate_suffix_length(remaining_length);
    uint16_t record_length = suffix_length + payload_count;
    if (record_length < sizeof(DualPagePointer)) {
      return sizeof(DualPagePointer);
    }
    return assorted::align16(suffix_length + payload_count);
  }

  bool    can_accomodate(
    uint8_t new_index,
    uint8_t remaining_length,
    uint16_t payload_count) const ALWAYS_INLINE {
    if (new_index == 0) {
      ASSERT_ND(remaining_length + payload_count <= kDataSize);
      return true;
    } else if (new_index >= kMaxKeys) {
      return false;
    }
    uint16_t record_size = calculate_record_size(remaining_length, payload_count);
    uint16_t last_offset = static_cast<uint16_t>(offsets_[new_index - 1]) << 4;
    return record_size <= last_offset;
  }

  bool    compare_key(uint8_t index, const void* be_key, uint16_t key_length) const ALWAYS_INLINE {
    ASSERT_ND(index < kMaxKeys);
    uint16_t remaining = key_length - get_layer() * sizeof(KeySlice);
    if (remaining != remaining_key_length_[index]) {
      return false;
    }
    KeySlice slice = slice_layer(be_key, key_length, get_layer());
    if (slice != slices_[index]) {
      return false;
    }
    if (remaining > sizeof(KeySlice)) {
      return std::memcmp(
        reinterpret_cast<const char*>(be_key) + (get_layer() + 1) * sizeof(KeySlice),
        get_record(index),
        remaining - sizeof(KeySlice)) == 0;
    } else {
      return true;
    }
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
    uint8_t remaining_length,
    uint16_t payload_count);

  /** morph the specified record to a next layer pointer. this needs a record lock to execute. */
  void    set_next_layer(uint8_t index, const DualPagePointer& pointer) {
    ASSERT_ND(get_owner_id(index)->lock_.is_keylocked());
    ASSERT_ND(remaining_key_length_[index] > sizeof(KeySlice));
    remaining_key_length_[index] = kKeyLengthNextLayer;
    *get_next_layer(index) = pointer;
  }

  /**
   * Copy the initial record that will be the only record for a new root page.
   * This is called when a new layer is created, and done in a thread-private memory.
   * So, no synchronization needed.
   */
  void    initialize_layer_root(const MasstreeBorderPage* copy_from, uint8_t copy_index);

  void    release_pages_recursive(
    const memory::GlobalVolatilePageResolver& page_resolver,
    memory::PageReleaseBatch* batch);

  /** prefetch upto 1/4 of slices. */
  void prefetch() const ALWAYS_INLINE {
    assorted::prefetch_cachelines(this, 4);
  }
  void prefetch_additional_if_needed(uint8_t key_count) const ALWAYS_INLINE {
    if (key_count > 16U) {
      // we initially prefetched 64*4 = 256 bytes: header, keylen, and 16 key slices.
      // if we have more, prefetch now while we are still searching.
      uint16_t cachelines = ((key_count - 16U) >> 3) + 1;
      assorted::prefetch_cachelines(reinterpret_cast<const char*>(this) + 256, cachelines);
    }
  }

  /**
   * Splits this page as a system transaction, creating a new foster child.
   * @param[in] context Thread context
   * @param[in] trigger The key that triggered this split
   * @param[out] target the page the new key will be inserted. Either foster_child or foster_minor.
   * @param[out] target_lock this thread's MCS block index for target.
   * @pre !header_.snapshot_ (split happens to only volatile pages)
   * @pre is_locked() (the page must be locked)
   * @post iff successfully exits, target->is_locked(), and target_lock is the MCS block index
   */
  ErrorCode split_foster(
    thread::Thread* context,
    KeySlice trigger,
    MasstreeBorderPage** target,
    xct::McsBlockIndex *target_lock);

  /**
   * @return whether we could track it. the only case it fails to track is the record moved
   * to deeper layers. we can also track it down to other layers, but it's rare. so, just retry
   * the whole transaction.
   */
  bool track_moved_record(
    xct::LockableXctId* owner_address,
    MasstreeBorderPage** located_page,
    uint8_t* located_index);

  void assert_entries() ALWAYS_INLINE {
#ifndef NDEBUG
    ASSERT_ND(is_locked());  // the following logic holds only when this page is locked
    struct Sorter {
      explicit Sorter(const MasstreeBorderPage* target) : target_(target) {}
      bool operator() (uint8_t left, uint8_t right) {
        KeySlice left_slice = target_->get_slice(left);
        KeySlice right_slice = target_->get_slice(right);
        if (left_slice < right_slice) {
          return true;
        } else if (left_slice == right_slice) {
          return target_->get_remaining_key_length(left) < target_->get_remaining_key_length(right);
        } else {
          return false;
        }
      }
      const MasstreeBorderPage* target_;
    };
    uint8_t key_count = get_key_count();
    uint8_t order[kMaxKeys];
    for (uint8_t i = 0; i < key_count; ++i) {
      order[i] = i;
    }
    std::sort(order, order + key_count, Sorter(this));

    for (uint8_t i = 1; i < key_count; ++i) {
      uint8_t pre = order[i - 1];
      uint8_t cur = order[i];
      ASSERT_ND(slices_[pre] <= slices_[cur]);
      if (slices_[pre] == slices_[cur]) {
        ASSERT_ND(remaining_key_length_[pre] < remaining_key_length_[cur]);
        ASSERT_ND(remaining_key_length_[pre] <= sizeof(KeySlice));
      }
    }
#endif  // NDEBUG
  }

 private:
  // 72

  /**
   * Stores key length excluding previous layers, but including this layer (which might be less
   * than 8!) and suffix if exists. 8 is kind of optimal, storing everything in slice.
   * 0-7 also stores everything in slice, but note that you have to distinct the same slice
   * with different length. 9- stores a suffix in this page.
   * If this points to next layer, this value is kKeyLengthNextLayer.
   */
  uint8_t     remaining_key_length_[kMaxKeys];    // +64 -> 136

  /**
   * Key slice of this page. remaining_key_length_ and slice_ are essential to find the record
   * (other fields are also used, but only occasionally when slice completely matches)
   * so they are placed at the beginning and we do prefetching. slices_ are bigger, so
   * we issue another prefetch while searching when appropriate.
   */
  KeySlice    slices_[kMaxKeys];                  // +512 -> 648

  /** Offset of the beginning of record in data_, divided by 8. */
  uint8_t     offsets_[kMaxKeys];                 // +64  -> 712
  /**
   * length of the payload.
   */
  uint16_t    payload_length_[kMaxKeys];          // +128 -> 840

  /** To make the following 16-bytes aligned */
  uint64_t    dummy_;                             // +8 -> 848

  /**
   * Lock of each record. We separate this out from record to avoid destructive change
   * while splitting and page compaction. We have to make sure xct_id is always in a separated
   * area.
   */
  xct::LockableXctId  owner_ids_[kMaxKeys];               // +1024 -> 1872

  /**
   * The main data region of this page. Suffix and payload contiguously.
   * Starts at the tail and grows backwards.
   * All records are 16-byte aligned so that we can later replace records to next-layer pointer.
   */
  char        data_[kDataSize];

  /**
   * @brief Subroutin of split_foster() to decide how we will split this page.
   */
  BorderSplitStrategy split_foster_decide_strategy(uint8_t key_count, KeySlice trigger) const;

  void split_foster_migrate_records(
    const MasstreeBorderPage& copy_from,
    uint8_t key_count,
    KeySlice from,
    KeySlice to);

  /**
   * @brief Subroutin of split_foster()
   * @return MCS block index of the \e first lock acqired. As this is done in a single transaction,
   * following locks trivially have sequential block index from it.
   * @details
   * First, we have to lock all (physically) active records to advance versions.
   * This is required because other transactions might be already in pre-commit phase to
   * modify records in this page.
   */
  xct::McsBlockIndex split_foster_lock_existing_records(thread::Thread* context, uint8_t key_count);
};
STATIC_SIZE_CHECK(sizeof(MasstreeBorderPage), 1 << 12)

inline uint8_t MasstreeBorderPage::find_key(
  KeySlice slice,
  const void* suffix,
  uint8_t remaining) const {
  uint8_t key_count = get_key_count();
  ASSERT_ND(remaining <= kKeyLengthMax);
  ASSERT_ND(key_count <= kMaxKeys);
  prefetch_additional_if_needed(key_count);
  for (uint8_t i = 0; i < key_count; ++i) {
    if (LIKELY(slice != slices_[i])) {
      continue;
    }
    // one slice might be used for up to 10 keys, length 0 to 8 and pointer to next layer.
    if (remaining <= sizeof(KeySlice)) {
      // no suffix nor next layer, so just compare length
      if (remaining_key_length_[i] == remaining) {
        return i;
      } else {
        continue;  // did not match
      }
    }
    ASSERT_ND(remaining > sizeof(KeySlice));  // keep this in mind below

    if (does_point_to_layer(i)) {
      // as it points to next layer, no suffix.
      // so far we don't delete layers, so in this case the record is always valid.
      return i;
    }

    ASSERT_ND(!does_point_to_layer(i));

    // now, our key is > 8 bytes and we found some local record.
    if (remaining_key_length_[i] == remaining) {
      // compare suffix.
      const char* record_suffix = get_record(i);
      if (std::memcmp(record_suffix, suffix, remaining - sizeof(KeySlice)) == 0) {
        return i;
      }
    }

    // suppose the record has > 8 bytes key. it must be the only such record in this page
    // because otherwise we must have created a next layer!
    if (remaining_key_length_[i] > sizeof(KeySlice)) {
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
  if (from_index == 0) {  // we don't need prefetching in second time
    prefetch_additional_if_needed(to_index);
  }
  for (uint8_t i = from_index; i < to_index; ++i) {
    if (UNLIKELY(slice == slices_[i] && remaining_key_length_[i] == sizeof(KeySlice))) {
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
  uint8_t remaining) const {
  ASSERT_ND(to_index <= kMaxKeys);
  ASSERT_ND(from_index <= to_index);
  ASSERT_ND(remaining <= kKeyLengthMax);
  if (from_index == 0) {
    prefetch_additional_if_needed(to_index);
  }
  for (uint8_t i = from_index; i < to_index; ++i) {
    if (LIKELY(slice != slices_[i])) {
      continue;
    }
    if (remaining <= sizeof(KeySlice)) {
      if (remaining_key_length_[i] == remaining) {
        ASSERT_ND(!does_point_to_layer(i));
        return FindKeyForReserveResult(i, kExactMatchLocalRecord);
      } else {
        continue;
      }
    }
    ASSERT_ND(remaining > sizeof(KeySlice));

    if (does_point_to_layer(i)) {
      return FindKeyForReserveResult(i, kExactMatchLayerPointer);
    }

    ASSERT_ND(!does_point_to_layer(i));

    if (remaining_key_length_[i] <= sizeof(KeySlice)) {
      continue;
    }

    // now, both the searching key and this key are more than 8 bytes.
    // whether the key really matches or not, this IS the slot we are looking for.
    // Either 1) the keys really match, or 2) we will make this record point to next layer.
    const char* record_suffix = get_record(i);
    if (remaining_key_length_[i] == remaining &&
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
  uint8_t remaining_length,
  uint16_t payload_count) {
  ASSERT_ND(initial_owner_id.is_deleted());
  ASSERT_ND(index < kMaxKeys);
  ASSERT_ND(remaining_length <= kKeyLengthMax);
  ASSERT_ND(is_locked());
  ASSERT_ND(get_key_count() == index);
  ASSERT_ND(can_accomodate(index, remaining_length, payload_count));
  uint16_t suffix_length = calculate_suffix_length(remaining_length);
  DataOffset record_size = calculate_record_size(remaining_length, payload_count) >> 4;
  DataOffset previous_offset;
  if (index == 0) {
    previous_offset = kDataSize >> 4;
  } else {
    previous_offset = offsets_[index - 1];
  }
  slices_[index] = slice;
  remaining_key_length_[index] = remaining_length;
  payload_length_[index] = payload_count;
  offsets_[index] = previous_offset - record_size;
  owner_ids_[index].lock_.reset();
  owner_ids_[index].xct_id_ = initial_owner_id;
  if (suffix_length > 0) {
    std::memcpy(get_record(index), suffix, suffix_length);
  }
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_IMPL_HPP_
