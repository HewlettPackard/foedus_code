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
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_IMPL_HPP_

#include <stdint.h>

#include <algorithm>
#include <cstring>
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_code.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {

KeyLength calculate_suffix_length(KeyLength remainder_length) ALWAYS_INLINE;
KeyLength calculate_suffix_length_aligned(KeyLength remainder_length) ALWAYS_INLINE;

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
  VolatilePagePointer get_volatile_page_id() const {
    ASSERT_ND(!header_.snapshot_);
    return VolatilePagePointer(header_.page_id_);
  }
  SnapshotPagePointer get_snapshot_page_id() const {
    ASSERT_ND(header_.snapshot_);
    return static_cast<SnapshotPagePointer>(header_.page_id_);
  }

  bool                is_border() const ALWAYS_INLINE {
    ASSERT_ND(header_.get_page_type() == kMasstreeBorderPageType ||
      header_.get_page_type() == kMasstreeIntermediatePageType);
    return header_.get_page_type() == kMasstreeBorderPageType;
  }
  /**
   * An empty-range page, either intermediate or border, never has any entries.
   * Such a page exists for short duration after a special page-split for
   * record compaction/expansion and page restructuring.
   * Such a page always appears as one of foster-twins: it will be never adopted to be a real child.
   * Also guaranteed to not have any foster twins under it.
   * Wan safely skip such a page while following foster twins.
   */
  bool                is_empty_range() const ALWAYS_INLINE { return low_fence_ == high_fence_; }
  KeySlice            get_low_fence() const ALWAYS_INLINE { return low_fence_; }
  KeySlice            get_high_fence() const ALWAYS_INLINE { return high_fence_; }
  bool                is_high_fence_supremum() const ALWAYS_INLINE {
    return high_fence_ == kSupremumSlice;
  }
  bool                is_low_fence_infimum() const ALWAYS_INLINE {
    return low_fence_ == kInfimumSlice;
  }
  bool                is_layer_root() const ALWAYS_INLINE {
    return is_low_fence_infimum() && is_high_fence_supremum();
  }
  KeySlice            get_foster_fence() const ALWAYS_INLINE { return foster_fence_; }
  bool  is_foster_minor_null() const ALWAYS_INLINE { return foster_twin_[0].is_null(); }
  bool  is_foster_major_null() const ALWAYS_INLINE { return foster_twin_[1].is_null(); }
  VolatilePagePointer get_foster_minor() const ALWAYS_INLINE { return foster_twin_[0]; }
  VolatilePagePointer get_foster_major() const ALWAYS_INLINE { return foster_twin_[1]; }
  void                set_foster_twin(VolatilePagePointer minor, VolatilePagePointer major) {
    foster_twin_[0] = minor;
    foster_twin_[1] = major;
  }
  void                install_foster_twin(
    VolatilePagePointer minor,
    VolatilePagePointer major,
    KeySlice foster_fence) {
    set_foster_twin(minor, major);
    foster_fence_ = foster_fence;
  }

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
  /** @copydoc foedus::storage::PageHeader::masstree_in_layer_level_ */
  uint8_t get_btree_level() const ALWAYS_INLINE { return header_.masstree_in_layer_level_; }
  /** \e physical key count (those keys might be deleted) in this page. */
  SlotIndex           get_key_count() const ALWAYS_INLINE { return header_.key_count_; }
  void                set_key_count(SlotIndex count) ALWAYS_INLINE { header_.set_key_count(count); }
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
  xct::McsWwLock* get_lock_address() ALWAYS_INLINE { return &header_.page_version_.lock_; }

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


  /** As the name suggests, this should be used only by composer. foster twin should be immutable */
  void              set_foster_major_offset_unsafe(memory::PagePoolOffset offset) ALWAYS_INLINE {
    ASSERT_ND(header_.snapshot_);
    foster_twin_[1].set_offset_unsafe(offset);
  }
  /** As the name suggests, this should be used only by composer. fence should be immutable */
  void              set_high_fence_unsafe(KeySlice high_fence) ALWAYS_INLINE {
    ASSERT_ND(header_.snapshot_);
    high_fence_ = high_fence;
  }

  /** defined in masstree_page_debug.cpp. */
  friend std::ostream& operator<<(std::ostream& o, const MasstreePage& v);

 protected:
  PageHeader          header_;      // +40 -> 40

  /** Inclusive low fence of this page. Mainly used for sanity checking */
  KeySlice            low_fence_;   // +8 -> 48
  /** Inclusive high fence of this page. Mainly used for sanity checking */
  KeySlice            high_fence_;  // +8 -> 56
  /** Inclusive low_fence of foster child. undefined if foster child is not set*/
  KeySlice            foster_fence_;  // +8 -> 64

  /**
   * Points to foster children, or tentative child pages.
   * Null if has_foster_child of page_version is false.
   * This is core of the foster-twin protocol.
   *
   * [0]: Left-half of this page, or minor foster child.
   * [1]: Right-half of this page, or major foster child.
   */
  VolatilePagePointer foster_twin_[2];  // +16 -> 80

  void                initialize_volatile_common(
    StorageId           storage_id,
    VolatilePagePointer page_id,
    PageType            page_type,
    uint8_t             layer,
    uint8_t             level,
    KeySlice            low_fence,
    KeySlice            high_fence);

  void                initialize_snapshot_common(
    StorageId           storage_id,
    SnapshotPagePointer page_id,
    PageType            page_type,
    uint8_t             layer,
    uint8_t             level,
    KeySlice            low_fence,
    KeySlice            high_fence);
};

/**
 * Max number of separators stored in the first level of intermediate pages.
 * @ingroup MASSTREE
 * @todo Should be calculated from kPageSize. Super low-priority.
 */
const uint16_t kMaxIntermediateSeparators = 9U;

/**
 * Max number of separators stored in the second level of intermediate pages.
 * @ingroup MASSTREE
 * @todo Should be calculated from kPageSize. Super low-priority.
 */
const uint16_t kMaxIntermediateMiniSeparators = 15U;

/**
 * Max number of pointers (if completely filled) stored in an intermediate pages.
 * @ingroup MASSTREE
 */
const uint32_t kMaxIntermediatePointers
  = (kMaxIntermediateSeparators + 1U) * (kMaxIntermediateMiniSeparators + 1U);

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
  friend struct SplitIntermediate;
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

  /**
   * This method is used when we release a large number of volatile pages, most likely
   * when we drop a storage. In that case, we don't need to stick to the current thread.
   * rather, this thread spawns lots of threads to parallelize the work.
   */
  void              release_pages_recursive_parallel(Engine* engine);
  void              release_pages_recursive(
    const memory::GlobalVolatilePageResolver& page_resolver,
    memory::PageReleaseBatch* batch);

  void initialize_volatile_page(
    StorageId           storage_id,
    VolatilePagePointer page_id,
    uint8_t             layer,
    uint8_t             level,
    KeySlice            low_fence,
    KeySlice            high_fence);
  void initialize_snapshot_page(
    StorageId           storage_id,
    SnapshotPagePointer page_id,
    uint8_t             layer,
    uint8_t             level,
    KeySlice            low_fence,
    KeySlice            high_fence);

  /**
   * Appends a new poiner and separator in an existing mini page, used only by snapshot composer.
   * @pre header_.snapshot
   * @pre !is_full_snapshot()
   */
  void      append_pointer_snapshot(KeySlice low_fence, SnapshotPagePointer pointer);
  /**
   * Appends a new separator and the initial pointer in new mini page,
   * used only by snapshot composer.
   * @pre header_.snapshot
   * @pre !is_full_snapshot()
   */
  void      append_minipage_snapshot(KeySlice low_fence, SnapshotPagePointer pointer);
  /** Whether this page is full of poiters, used only by snapshot composer (or when no race) */
  bool      is_full_snapshot() const;
  /** Retrieves separators defining the index, used only by snapshot composer, thus no race */
  void      extract_separators_snapshot(
    uint8_t index,
    uint8_t index_mini,
    KeySlice* separator_low,
    KeySlice* separator_high) const {
    ASSERT_ND(header_.snapshot_);
    extract_separators_common(index, index_mini, separator_low, separator_high);
  }
  /**
   * Retrieves separators defining the index, used for volatile page, which requires
   * appropriate locks or retries by the caller. The caller must be careful!
   */
  void      extract_separators_volatile(
    uint8_t index,
    uint8_t index_mini,
    KeySlice* separator_low,
    KeySlice* separator_high) const {
    ASSERT_ND(!header_.snapshot_);
    extract_separators_common(index, index_mini, separator_low, separator_high);
  }
  /** Retrieves separators defining the index, used only by snapshot composer (or when no race) */
  void      extract_separators_common(
    uint8_t index,
    uint8_t index_mini,
    KeySlice* separator_low,
    KeySlice* separator_high) const;

  void verify_separators() const;

  /** Place a new separator for a new minipage */
  void set_separator(uint8_t minipage_index, KeySlice new_separator) {
    separators_[minipage_index] = new_separator;
  }

  /** defined in masstree_page_debug.cpp. */
  friend std::ostream& operator<<(std::ostream& o, const MasstreeIntermediatePage& v);

 private:
  // 80

  /**
   * Separators to navigate search to mini pages in this page.
   * Iff separators_[i-1] <= Slice < separators_[i], the search is navigated to mini_pages_[i].
   * Iff Slice < separators_[0] or key_count==0, mini_pages_[0].
   * Iff Slice >= separators_[key_count-1] or key_count==0, mini_pages_[key_count].
   */
  KeySlice            separators_[kMaxIntermediateSeparators];  // +72 -> 152

  char                reserved_[104];    // -> 256

  MiniPage            mini_pages_[10];  // +384 * 10 -> 4096
};

/**
 * @brief Represents one border page in \ref MASSTREE.
 * @ingroup MASSTREE
 * @details
 * @par Slots
 * One border page has at most kBorderPageMaxSlots slots.
 * One slot is reserved for one \e physical record, which is never moved except snapshotting
 * and split/compact.
 * A thread first installs a new record by atomically modifying page_version, then
 * set up the record with deletion flag on.
 * Flipping the delete flag of the record is done by apply() of transaction, which might fail.
 * If it fails, the record is left as deleted until snapshotting or split/compact.
 *
 * @par Page Layout
 * <table>
 *  <tr><td>Headers, including common part and border-specific part</td></tr>
 *  <tr><td>Record Data part, which grows forward</td></tr>
 *  <tr><th>Unused part</th></tr>
 *  <tr><td>Slot part (32 bytes per record), which grows backward</td></tr>
 * </table>
 *
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class MasstreeBorderPage final : public MasstreePage {
 public:
  friend struct SplitBorder;
  /**
   * A piece of Slot object that must be read/written in one-shot, meaning no one reads
   * half-written values whether it reads old values or new values.
   * Today's CPUs guarantee that for 64-bit integers, so we use union to switch uint64_t/struct.
   * Of course this object must be aligned to make this work.
   * @par When these values change
   * Unlike hash storage, this is mutable, or it changes when the record is moved to expand,
   * but it changes only in a restricted way.
   * \li Concurrent threads are safe to read the record before the change.
   * Pre-commit will detect the record has been moved by checking XID.
   * \li when a record is expanded/moved, the old data at old offset_ are untouched.
   * This means that we can retrieve the original full key for a moved record.
   * \li physical_record_length_ stays the same or increases.
   * \li A record can move at most once.
   * \li A record that is originally a next-layer record never moves.
   * \li When the slot uses the right-most record region in the page
   * (next_offset_==offset_+physical_record_length_), we might increase physical_record_length_
   * without moving offset_ because record-part grows forward.
   */
  struct SlotLengthPart {
    /**
     * Byte offset in data_ where this record starts.
     * @invariant offset_ % 8 == 0
     * @invariant offset_ < kBorderPageDataPartSize
     */
    DataOffset  offset_;                // +2  -> 2
    /**
     * Byte count this record occupies.
     * @invariant physical_record_length_ % 8 == 0
     * @invariant physical_record_length_ >= align8(suffix_length) + align8(payload_length_)
     */
    DataOffset  physical_record_length_;  // +2  -> 4
    /** unused so far */
    uint16_t    unused_;    // +2  -> 6
    /**
     * Byte length of the payload. Padding is not included.
     * This is \b mutable. Only changed with update.
     * If the updated payload is too large for physical_record_length_, we \e move the record,
     * leaving the \e deleted and \e moved flags in this old record.
     */
    PayloadLength payload_length_;        // +2  -> 8
  };
  /** @see SlotLengthPart */
  union SlotLengthUnion {
    uint64_t        word;
    SlotLengthPart  components;
  };

  /**
   * Fix-sized slot for each record, which is placed at the end of data region.
   * Each slot takes 32-bytes, which are not small but not bad either to
   * reduce cache-line contentios on TIDs. Every slot is also aligned to 32-bytes.
   */
  struct Slot {
    /**
     * TID of the record.
     */
    xct::RwLockableXctId  tid_;       // +16 -> 16

    /**
     * Stores mutable length information of the record. Because these are mutable,
     * in some cases you must read this 8-byte in one-shot. Use read_lengthes_oneshot() then.
     */
    SlotLengthUnion     lengthes_;  // +8  -> 24

    /// Followings are immutable.

    /**
     * Length of \e remainder, the key excluding slices in previous layers,
     * but including this layer (which might be less than 8!) and suffix if exists.
     * 8 is kind of optimal, storing everything in slice.
     * 0-7 also stores everything in slice, but note that you have to distinct the same slice
     * with different length. 9- stores a suffix in this page.
     * 0xFFFF (kInitiallyNextLayer) is a special value that is used only for a record
     * that was initially next-layer.
     * Unlike the original Masstree, this is an immutable property and unchanged even when
     * now this record points to next layer.
     * Immutable once made.
     * @ref MASS_TERM_REMAINDER
     */
    KeyLength           remainder_length_;          // +2  -> 26
    /**
     * The value of physical_record_length_ as of the creation of this record.
     * Immutable once made.
     */
    DataOffset  original_physical_record_length_;   // +2 -> 28
    /**
     * The value of offset_ as of the creation of this record.
     * Immutable once made.
     */
    DataOffset          original_offset_;           // +2 -> 30
    char                filler_[2];                 // +2 -> 32

    /// only reinterpret_cast
    Slot() = delete;
    Slot(const Slot&) = delete;
    Slot& operator=(const Slot&) = delete;
    ~Slot() = delete;

    inline KeyLength get_aligned_suffix_length() const ALWAYS_INLINE {
      return calculate_suffix_length_aligned(remainder_length_);
    }
    inline KeyLength get_suffix_length() const ALWAYS_INLINE {
      return calculate_suffix_length(remainder_length_);
    }
    /** This might be affected by concurrent threads */
    inline PayloadLength get_max_payload_peek() const ALWAYS_INLINE {
      return lengthes_.components.physical_record_length_ - get_aligned_suffix_length();
    }
    /** This is not affected by concurrent threads */
    inline PayloadLength get_max_payload_stable(SlotLengthPart stable_length) const ALWAYS_INLINE {
      return stable_length.physical_record_length_ - get_aligned_suffix_length();
    }
    inline bool does_point_to_layer() const ALWAYS_INLINE {
      return tid_.xct_id_.is_next_layer();
    }

    /**
     * Reads lengthes_ of this slot in one-shot.
     * It returns a complete value, not half-written, of lengthes_ at \e some point of time.
     */
    inline SlotLengthPart read_lengthes_oneshot() const ALWAYS_INLINE {
      SlotLengthUnion ret;
      ret.word = lengthes_.word;
      return ret.components;
    }
    /**
     * Writes lengthes_ in one-shot. Use this to update an existing slot.
     * You don't need to use it if you are creating a new slot as there is no race.
     */
    inline void write_lengthes_oneshot(SlotLengthPart new_value) ALWAYS_INLINE {
      SlotLengthUnion tmp;
      tmp.components = new_value;
      lengthes_.word = tmp.word;
    }
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
    FindKeyForReserveResult(SlotIndex index, MatchType match_type)
      : index_(index), match_type_(match_type) {}
    SlotIndex index_;
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
  void initialize_snapshot_page(
    StorageId           storage_id,
    SnapshotPagePointer page_id,
    uint8_t             layer,
    KeySlice            low_fence,
    KeySlice            high_fence);

  /**
   * Whether this page is receiving only sequential inserts.
   * If this is true, cursor can skip its sorting phase.
   * If this is a snapshot page, this is always true.
   */
  bool        is_consecutive_inserts() const { return consecutive_inserts_; }

  DataOffset  get_next_offset() const { return next_offset_; }
  void        increase_next_offset(DataOffset length) {
    next_offset_ += length;
    ASSERT_ND(next_offset_ <= sizeof(data_));
  }

  inline const Slot* get_slot(SlotIndex index) const ALWAYS_INLINE {
    ASSERT_ND(index < get_key_count());
    ASSERT_ND(index < kBorderPageMaxSlots);
    return reinterpret_cast<const Slot*>(this + 1) - index - 1;
  }

  inline Slot* get_slot(SlotIndex index) ALWAYS_INLINE {
    ASSERT_ND(index < get_key_count());
    ASSERT_ND(index < kBorderPageMaxSlots);
    return reinterpret_cast<Slot*>(this + 1) - index - 1;
  }

  inline Slot* get_new_slot(SlotIndex index) ALWAYS_INLINE {
    ASSERT_ND(index == get_key_count());
    ASSERT_ND(index < kBorderPageMaxSlots);
    return reinterpret_cast<Slot*>(this + 1) - index - 1;
  }

  inline SlotIndex to_slot_index(const Slot* slot) const ALWAYS_INLINE {
    ASSERT_ND(slot);
    int64_t index = reinterpret_cast<const Slot*>(this + 1) - slot - 1;
    ASSERT_ND(index >= 0);
    ASSERT_ND(index < static_cast<int64_t>(sizeof(data_) / sizeof(Slot)));
    return static_cast<SlotIndex>(index);
  }

  // methods about available/required spaces

  /** Returns usable data space in bytes. */
  inline DataOffset   available_space() const {
    uint16_t consumed = next_offset_ + get_key_count() * sizeof(Slot);
    ASSERT_ND(consumed <= sizeof(data_));
    if (consumed > sizeof(data_)) {  // just to be conservative on release build
      return 0;
    }
    return sizeof(data_) - consumed;
  }

  /**
   * returns minimal physical_record_length_ for the given remainder/payload length.
   * @attention this doesn't count the space for Slot nor slices.
   * @see required_data_space()
   */
  inline static DataOffset to_record_length(
    KeyLength remainder_length,
    PayloadLength payload_length) {
    KeyLength suffix_length_aligned = calculate_suffix_length_aligned(remainder_length);
    return suffix_length_aligned + assorted::align8(payload_length);
  }

  /**
   * returns the byte size of required contiguous space in data_ to insert a new record
   * of the given remainder/payload length. This includes Slot size.
   * Slices are not included as they are anyway statically placed.
   * @see to_record_length()
   */
  inline static DataOffset required_data_space(
    KeyLength remainder_length,
    PayloadLength payload_length) {
    return to_record_length(remainder_length, payload_length) + sizeof(Slot);
  }

  /**
   * @brief Navigates a searching key-slice to one of the record in this page.
   * @return index of key found in this page, or kBorderPageMaxSlots if not found.
   */
  SlotIndex find_key(
    KeySlice slice,
    const void* suffix,
    KeyLength remainder) const ALWAYS_INLINE;

  /**
   * Specialized version for 8 byte native integer search. Because such a key never goes to
   * second layer, this is much simpler.
   */
  SlotIndex find_key_normalized(
    SlotIndex from_index,
    SlotIndex to_index,
    KeySlice slice) const ALWAYS_INLINE;


  /**
   * This is for the case we are looking for either the matching slot or the slot we will modify.
   */
  FindKeyForReserveResult find_key_for_reserve(
    SlotIndex from_index,
    SlotIndex to_index,
    KeySlice slice,
    const void* suffix,
    KeyLength remainder) const ALWAYS_INLINE;
  /**
   * This one is used for snapshot pages.
   * Because keys are fully sorted in snapshot pages, this returns the index of the first record
   * whose key is strictly larger than given key (key_count if not exists).
   * @pre header_.snapshot
   */
  FindKeyForReserveResult find_key_for_snapshot(
    KeySlice slice,
    const void* suffix,
    KeyLength remainder) const ALWAYS_INLINE;

  char* get_record(SlotIndex index) ALWAYS_INLINE {
    ASSERT_ND(index < kBorderPageMaxSlots);
    return get_record_from_offset(get_offset_in_bytes(index));
  }
  const char* get_record(SlotIndex index) const ALWAYS_INLINE {
    ASSERT_ND(index < kBorderPageMaxSlots);
    return get_record_from_offset(get_offset_in_bytes(index));
  }
  char* get_record_payload(SlotIndex index) ALWAYS_INLINE {
    char* record = get_record(index);
    KeyLength skipped = get_suffix_length_aligned(index);
    return record + skipped;
  }
  const char* get_record_payload(SlotIndex index) const ALWAYS_INLINE {
    const char* record = get_record(index);
    KeyLength skipped = get_suffix_length_aligned(index);
    return record + skipped;
  }
  DualPagePointer* get_next_layer(SlotIndex index) ALWAYS_INLINE {
    return reinterpret_cast<DualPagePointer*>(get_record_payload(index));
  }
  const DualPagePointer* get_next_layer(SlotIndex index) const ALWAYS_INLINE {
    return reinterpret_cast<const DualPagePointer*>(get_record_payload(index));
  }

  /// Offset versions
  char* get_record_from_offset(DataOffset record_offset) ALWAYS_INLINE {
    ASSERT_ND(record_offset % 8 == 0);
    ASSERT_ND(record_offset < sizeof(data_));
    return data_ + record_offset;
  }
  const char* get_record_from_offset(DataOffset record_offset) const ALWAYS_INLINE {
    ASSERT_ND(record_offset % 8 == 0);
    ASSERT_ND(record_offset < sizeof(data_));
    return data_ + record_offset;
  }
  const char* get_record_payload_from_offsets(
    DataOffset record_offset,
    KeyLength remainder_length) const ALWAYS_INLINE {
    const char* record = get_record_from_offset(record_offset);
    KeyLength skipped = calculate_suffix_length_aligned(remainder_length);
    return record + skipped;
  }
  char* get_record_payload_from_offsets(
    DataOffset record_offset,
    KeyLength remainder_length) ALWAYS_INLINE {
    char* record = get_record_from_offset(record_offset);
    KeyLength skipped = calculate_suffix_length_aligned(remainder_length);
    return record + skipped;
  }
  DualPagePointer* get_next_layer_from_offsets(
    DataOffset record_offset,
    KeyLength remainder_length) ALWAYS_INLINE {
    char* payload = get_record_payload_from_offsets(record_offset, remainder_length);
    return reinterpret_cast<DualPagePointer*>(payload);
  }
  const DualPagePointer* get_next_layer_from_offsets(
    DataOffset record_offset,
    KeyLength remainder_length) const ALWAYS_INLINE {
    const char* payload = get_record_payload_from_offsets(record_offset, remainder_length);
    return reinterpret_cast<const DualPagePointer*>(payload);
  }


  bool does_point_to_layer(SlotIndex index) const ALWAYS_INLINE {
    ASSERT_ND(index < kBorderPageMaxSlots);
    return get_slot(index)->tid_.xct_id_.is_next_layer();
  }

  KeySlice get_slice(SlotIndex index) const ALWAYS_INLINE {
    ASSERT_ND(index < kBorderPageMaxSlots);
    return slices_[index];
  }
  void     set_slice(SlotIndex index, KeySlice slice) ALWAYS_INLINE {
    ASSERT_ND(index < kBorderPageMaxSlots);
    slices_[index] = slice;
  }
  DataOffset get_offset_in_bytes(SlotIndex index) const ALWAYS_INLINE {
    return get_slot(index)->lengthes_.components.offset_;
  }

  xct::RwLockableXctId* get_owner_id(SlotIndex index) ALWAYS_INLINE {
    return &get_slot(index)->tid_;
  }
  const xct::RwLockableXctId* get_owner_id(SlotIndex index) const ALWAYS_INLINE {
    return &get_slot(index)->tid_;
  }

  KeyLength get_remainder_length(SlotIndex index) const ALWAYS_INLINE {
    return get_slot(index)->remainder_length_;
  }
  KeyLength get_suffix_length(SlotIndex index) const ALWAYS_INLINE {
    const KeyLength remainder_length = get_remainder_length(index);
    return calculate_suffix_length(remainder_length);
  }
  KeyLength get_suffix_length_aligned(SlotIndex index) const ALWAYS_INLINE {
    const KeyLength remainder_length = get_remainder_length(index);
    return calculate_suffix_length_aligned(remainder_length);
  }
  /** @returns the current logical payload length, which might change later. */
  PayloadLength  get_payload_length(SlotIndex index) const ALWAYS_INLINE {
    return get_slot(index)->lengthes_.components.payload_length_;
  }
  /**
   * @returns the maximum payload length the physical record allows.
   */
  PayloadLength get_max_payload_length(SlotIndex index) const ALWAYS_INLINE {
    return get_slot(index)->get_max_payload_peek();
  }

  bool    can_accomodate(
    SlotIndex new_index,
    KeyLength remainder_length,
    PayloadLength payload_count) const ALWAYS_INLINE;
  /**
   * Slightly different from can_accomodate() as follows:
   * \li No race, so no need to receive new_index. It just uses get_key_count().
   * \li Always guarantees that the payload can be later expanded to sizeof(DualPagePointer).
   * @see replace_next_layer_snapshot()
   * @see MasstreeComposeContext::append_border()
   */
  bool    can_accomodate_snapshot(
    KeyLength remainder_length,
    PayloadLength payload_count) const ALWAYS_INLINE;
  /** actually this method should be renamed to equal_key... */
  bool  compare_key(
    SlotIndex index,
    const void* be_key,
    KeyLength key_length) const ALWAYS_INLINE;
  /** let's gradually migrate from compare_key() to this. */
  bool  equal_key(SlotIndex index, const void* be_key, KeyLength key_length) const ALWAYS_INLINE;

  /** compare the key. returns negative, 0, positive when the given key is smaller,same,larger. */
  int   ltgt_key(SlotIndex index, const char* be_key, KeyLength key_length) const ALWAYS_INLINE;
  /** Overload to receive slice+suffix */
  int   ltgt_key(
    SlotIndex index,
    KeySlice slice,
    const char* suffix,
    KeyLength remainder) const ALWAYS_INLINE;
  /**
   * Returns whether inserting the key will cause creation of a new next layer.
   * This is mainly used for assertions.
   */
  bool will_conflict(SlotIndex index, const char* be_key, KeyLength key_length) const ALWAYS_INLINE;
  /** Overload to receive slice */
  bool will_conflict(SlotIndex index, KeySlice slice, KeyLength remainder) const ALWAYS_INLINE;
  /** Returns whether the record is a next-layer pointer that would contain the key */
  bool will_contain_next_layer(
    SlotIndex index,
    const char* be_key,
    KeyLength key_length) const ALWAYS_INLINE;
  bool will_contain_next_layer(
    SlotIndex index,
    KeySlice slice,
    KeyLength remainder) const ALWAYS_INLINE;

  /**
   * Installs a new physical record that doesn't exist logically (delete bit on).
   * This sets 1) slot, 2) suffix key, and 3) XctId. Payload is not set yet.
   * This is executed as a system transaction.
   */
  void    reserve_record_space(
    SlotIndex index,
    xct::XctId initial_owner_id,
    KeySlice slice,
    const void* suffix,
    KeyLength remainder_length,
    PayloadLength payload_count);

  /** For creating a record that is initially a next-layer */
  void    reserve_initially_next_layer(
    SlotIndex index,
    xct::XctId initial_owner_id,
    KeySlice slice,
    const DualPagePointer& pointer);

  /**
   * Installs a next layer pointer. This is used only from snapshot composer, so no race.
   */
  void    append_next_layer_snapshot(
    xct::XctId initial_owner_id,
    KeySlice slice,
    SnapshotPagePointer pointer);
  /**
   * Same as above, except this is used to transform an existing record at end to a next
   * layer pointer. Unlike set_next_layer, this shrinks the payload part.
   */
  void    replace_next_layer_snapshot(SnapshotPagePointer pointer);

  /**
   * Copy the initial record that will be the only record for a new root page.
   * This is called when a new layer is created, and done in a thread-private memory.
   * So, no synchronization needed.
   */
  void    initialize_layer_root(const MasstreeBorderPage* copy_from, SlotIndex copy_index);

  void    release_pages_recursive(
    const memory::GlobalVolatilePageResolver& page_resolver,
    memory::PageReleaseBatch* batch);

  /** prefetch upto 256th bytes. */
  void prefetch() const ALWAYS_INLINE {
    assorted::prefetch_cachelines(this, 4);
  }
  void prefetch_additional_if_needed(SlotIndex key_count) const ALWAYS_INLINE {
    const uint32_t kInitialPrefetchBytes = 4U * assorted::kCachelineSize;  // 256 bytes
    const SlotIndex kInitiallyFetched
      = (kInitialPrefetchBytes - kCommonPageHeaderSize - kBorderPageAdditionalHeaderSize)
        / sizeof(KeySlice);
    if (key_count > kInitiallyFetched) {
      // we initially prefetched 64*4 = 256 bytes: header and 22 key slices.
      // if we have more, prefetch now while we are still searching.
      uint16_t cachelines = ((key_count - kInitiallyFetched) / sizeof(KeySlice)) + 1;
      assorted::prefetch_cachelines(
        reinterpret_cast<const char*>(this) + kInitialPrefetchBytes,
        cachelines);
    }
  }

  /**
   * A physical-only method to expand a record within this page without any logical change.
   * @pre !header_.snapshot_: only for volatile page
   * @pre is_locked() && !is_moved()
   * @pre get_slot(record_index)->is_locked() && !get_slot(record_index)->is_moved()
   * @return This method might fail if there isn't enough space. In that case it returns false.
   */
  bool try_expand_record_in_page_physical(PayloadLength payload_count, SlotIndex record_index);
  /**
   * A physical-only method to initialize this page as a volatile page of a layer-root
   * pointed from the given parent record. It merely migrates the parent record without
   * any logical change.
   * @pre !parent->header_.snapshot_: only for volatile page
   * @pre parent->is_locked() && !parent->is_moved()
   * @pre parent->get_slot(parent_index)->is_locked()
   * @pre !parent->get_slot(parent_index)->is_moved()
   * @pre !parent->get_slot(parent_index)->does_point_to_layer()
   */
  void initialize_as_layer_root_physical(
    VolatilePagePointer page_id,
    MasstreeBorderPage* parent,
    SlotIndex parent_index);

  /** @see StorageManager::track_moved_record() */
  xct::TrackMovedRecordResult track_moved_record(
    Engine* engine,
    xct::RwLockableXctId* old_address,
    xct::WriteXctAccess* write_set);
  /** This one further tracks it to next layer. Instead it requires a non-null write_set. */
  xct::TrackMovedRecordResult track_moved_record_next_layer(
    Engine* engine,
    xct::RwLockableXctId* old_address);

  /** @returns whether the length information seems okay. used only for assertions. */
  bool verify_slot_lengthes(SlotIndex index) const;

  void assert_entries() ALWAYS_INLINE {
#ifndef NDEBUG
    assert_entries_impl();
#endif  // NDEBUG
  }
  /** defined in masstree_page_debug.cpp. */
  void assert_entries_impl() const;
  /** defined in masstree_page_debug.cpp. */
  friend std::ostream& operator<<(std::ostream& o, const MasstreeBorderPage& v);

 private:
  // 80
  /**
   * How many bytes this page has consumed from the beginning of data_.
   * In other words, the offset of a next new record.
   * @invariant next_offset_ % 8 == 0
   * @invariant next_offset_ + get_key_count() * sizeof(Slot) <= kBorderPageDataSize
   */
  DataOffset  next_offset_;                 // +2 -> 82
  /**
   * Whether this page is receiving only sequential inserts.
   * If this is true, cursor can skip its sorting phase.
   * If this is a snapshot page, this is always true.
   */
  bool        consecutive_inserts_;         // +1 -> 83

  /** To make the following part a multiply of 8-bytes. */
  char        dummy_[5];                    // +5 -> 88

  /**
   * Key slice of this page. Unlike other information in the slots and records,
   * slices are most frequently read to find a key in this page.
   * Thus, they are placed at the beginning and we do prefetching the first few cachelines.
   * When slices_ are bigger than a few cachelines,
   * we issue another prefetch while searching when appropriate.
   * These values are immutable once the record is made.
   * @par Fixed space consumption
   * kBorderPageMaxSlots is around 100, thus we statically allocate 800 bytes for slices_.
   * We also considered more complicated schemes to spend the bytes only when we needed,
   * but it's not worth doing. Only 800 out of 4096, rather it is so important to
   * access slices_ efficiently.
   */
  KeySlice    slices_[kBorderPageMaxSlots];

  /**
   * Dynamic data part in this page, which consist of 1) key/payload part growing forward,
   * 2) unused part, and 3) Slot part growing backward.
   */
  char        data_[kBorderPageDataPartSize];

  MasstreeBorderPage* track_foster_child(
    KeySlice slice,
    const memory::GlobalVolatilePageResolver& resolver);
};

/**
 * Handy iterator for MasstreeIntermediate. Note that this object is not thread safe.
 * Use it only where it's safe (eg snapshot page).
 */
struct MasstreeIntermediatePointerIterator final {
  explicit MasstreeIntermediatePointerIterator(const MasstreeIntermediatePage* page)
    : page_(page), index_(0), index_mini_(0) {
      if (page->is_empty_range()) {
        // Empty-range page has zero pointers, which is special.
        ASSERT_ND(!page->header().snapshot_);
        index_ = 1;  // so that initial is_valid returns false.
      }
    }

  void next() {
    if (!is_valid()) {
      return;
    }
    ++index_mini_;
    if (index_mini_ > page_->get_minipage(index_).key_count_) {
      ++index_;
      index_mini_ = 0;
    }
  }
  bool is_valid() const {
    return index_ <= page_->get_key_count()
      && index_mini_ <= page_->get_minipage(index_).key_count_;
  }
  KeySlice get_low_key() const {
    ASSERT_ND(is_valid());
    const MasstreeIntermediatePage::MiniPage& minipage = page_->get_minipage(index_);
    if (index_mini_ > 0) {
      return minipage.separators_[index_mini_ - 1];
    } else if (index_ > 0) {
      return page_->get_separator(index_ - 1);
    } else {
      return page_->get_low_fence();
    }
  }
  KeySlice get_high_key() const {
    ASSERT_ND(is_valid());
    const MasstreeIntermediatePage::MiniPage& minipage = page_->get_minipage(index_);
    if (index_mini_ < minipage.key_count_) {
      return minipage.separators_[index_mini_];
    } else if (index_ < page_->get_key_count()) {
      return page_->get_separator(index_ );
    } else {
      return page_->get_high_fence();
    }
  }
  const DualPagePointer& get_pointer() const {
    ASSERT_ND(is_valid());
    const MasstreeIntermediatePage::MiniPage& minipage = page_->get_minipage(index_);
    return minipage.pointers_[index_mini_];
  }

  const MasstreeIntermediatePage* const page_;
  uint16_t  index_;
  uint16_t  index_mini_;
};


inline KeyLength calculate_suffix_length(KeyLength remainder_length) {
  if (remainder_length == kInitiallyNextLayer) {
    return 0;
  }
  ASSERT_ND(remainder_length <= kMaxKeyLength);
  if (remainder_length >= sizeof(KeySlice)) {
    return remainder_length - sizeof(KeySlice);
  } else {
    return 0;
  }
}
inline KeyLength calculate_suffix_length_aligned(KeyLength remainder_length) {
  return assorted::align8(calculate_suffix_length(remainder_length));
}

inline SlotIndex MasstreeBorderPage::find_key(
  KeySlice slice,
  const void* suffix,
  KeyLength remainder) const {
  SlotIndex key_count = get_key_count();
  ASSERT_ND(remainder <= kMaxKeyLength);
  ASSERT_ND(key_count <= kBorderPageMaxSlots);
  prefetch_additional_if_needed(key_count);

  // one slice might be used for up to 10 keys, length 0 to 8 and pointer to next layer.
  if (remainder <= sizeof(KeySlice)) {
    // then we are looking for length 0-8 only.
    for (SlotIndex i = 0; i < key_count; ++i) {
      const KeySlice rec_slice = get_slice(i);
      if (LIKELY(slice != rec_slice)) {
        continue;
      }
      // no suffix nor next layer, so just compare length. if not match, continue
      const KeyLength klen = get_remainder_length(i);
      if (klen == remainder) {
        return i;
      }
    }
  } else {
    // then we are only looking for length>8.
    for (SlotIndex i = 0; i < key_count; ++i) {
      const KeySlice rec_slice = get_slice(i);
      if (LIKELY(slice != rec_slice)) {
        continue;
      }

      if (does_point_to_layer(i)) {
        // as it points to next layer, no need to check suffix. We are sure this is it.
        // so far we don't delete layers, so in this case the record is always valid.
        return i;
      }

      // now, our key is > 8 bytes and we found some local record.
      const KeyLength klen = get_remainder_length(i);
      if (klen <= sizeof(KeySlice)) {
        continue;  // length 0-8, surely not a match. skip.
      }

      if (klen == remainder) {
        // compare suffix.
        const char* record_suffix = get_record(i);
        if (std::memcmp(record_suffix, suffix, remainder - sizeof(KeySlice)) == 0) {
          return i;
        }
      }

      // The record has > 8 bytes key of the same slice. it must be the only such record in this
      // page because otherwise we must have created a next layer!
      break;  // no more check needed
    }
  }
  return kBorderPageMaxSlots;
}

inline SlotIndex MasstreeBorderPage::find_key_normalized(
  SlotIndex from_index,
  SlotIndex to_index,
  KeySlice slice) const {
  ASSERT_ND(to_index <= kBorderPageMaxSlots);
  ASSERT_ND(from_index <= to_index);
  if (from_index == 0) {  // we don't need prefetching in second time
    prefetch_additional_if_needed(to_index);
  }
  for (SlotIndex i = from_index; i < to_index; ++i) {
    const KeySlice rec_slice = get_slice(i);
    const KeyLength klen = get_remainder_length(i);
    if (UNLIKELY(slice == rec_slice && klen == sizeof(KeySlice))) {
      return i;
    }
  }
  return kBorderPageMaxSlots;
}

inline MasstreeBorderPage::FindKeyForReserveResult MasstreeBorderPage::find_key_for_reserve(
  SlotIndex from_index,
  SlotIndex to_index,
  KeySlice slice,
  const void* suffix,
  KeyLength remainder) const {
  ASSERT_ND(to_index <= kBorderPageMaxSlots);
  ASSERT_ND(from_index <= to_index);
  ASSERT_ND(remainder <= kMaxKeyLength);
  if (from_index == 0) {
    prefetch_additional_if_needed(to_index);
  }
  if (remainder <= sizeof(KeySlice)) {
    for (SlotIndex i = from_index; i < to_index; ++i) {
      const KeySlice rec_slice = get_slice(i);
      if (LIKELY(slice != rec_slice)) {
        continue;
      }
      const KeyLength klen = get_remainder_length(i);
      if (klen == remainder) {
        ASSERT_ND(!does_point_to_layer(i));
        return FindKeyForReserveResult(i, kExactMatchLocalRecord);
      }
    }
  } else {
    for (SlotIndex i = from_index; i < to_index; ++i) {
      const KeySlice rec_slice = get_slice(i);
      if (LIKELY(slice != rec_slice)) {
        continue;
      }

      const bool next_layer = does_point_to_layer(i);
      const KeyLength klen = get_remainder_length(i);

      if (next_layer) {
        ASSERT_ND(klen > sizeof(KeySlice));
        return FindKeyForReserveResult(i, kExactMatchLayerPointer);
      } else if (klen <= sizeof(KeySlice)) {
        continue;
      }

      // now, both the searching key and this key are more than 8 bytes.
      // whether the key really matches or not, this IS the slot we are looking for.
      // Either 1) the keys really match, or 2) we will make this record point to next layer.
      const char* record_suffix = get_record(i);
      if (klen == remainder &&
        std::memcmp(record_suffix, suffix, remainder - sizeof(KeySlice)) == 0) {
        // case 1)
        return FindKeyForReserveResult(i, kExactMatchLocalRecord);
      } else {
        // case 2)
        return FindKeyForReserveResult(i, kConflictingLocalRecord);
      }
    }
  }
  return FindKeyForReserveResult(kBorderPageMaxSlots, kNotFound);
}

inline MasstreeBorderPage::FindKeyForReserveResult MasstreeBorderPage::find_key_for_snapshot(
  KeySlice slice,
  const void* suffix,
  KeyLength remainder) const {
  ASSERT_ND(header_.snapshot_);
  ASSERT_ND(remainder <= kMaxKeyLength);
  // Remember, unlike other cases above, there are no worry on concurrency.
  const SlotIndex key_count = get_key_count();
  if (remainder <= sizeof(KeySlice)) {
    for (SlotIndex i = 0; i < key_count; ++i) {
      const KeySlice rec_slice = get_slice(i);
      if (rec_slice < slice) {
        continue;
      } else if (rec_slice > slice) {
        // as keys are sorted in snapshot page, "i" is the place the slice would be inserted.
        return FindKeyForReserveResult(i, kNotFound);
      }
      ASSERT_ND(rec_slice == slice);
      const KeyLength klen = get_remainder_length(i);
      if (klen == remainder) {
        ASSERT_ND(!does_point_to_layer(i));
        return FindKeyForReserveResult(i, kExactMatchLocalRecord);
      } else if (klen < remainder) {
        ASSERT_ND(!does_point_to_layer(i));
        continue;  // same as "rec_slice < slice" case
      } else {
        return FindKeyForReserveResult(i, kNotFound);  // same as "rec_slice > slice" case
      }
    }
  } else {
    for (SlotIndex i = 0; i < key_count; ++i) {
      const KeySlice rec_slice = get_slice(i);
      if (rec_slice < slice) {
        continue;
      } else if (rec_slice > slice) {
        // as keys are sorted in snapshot page, "i" is the place the slice would be inserted.
        return FindKeyForReserveResult(i, kNotFound);
      }
      ASSERT_ND(rec_slice == slice);
      const KeyLength klen = get_remainder_length(i);

      if (does_point_to_layer(i)) {
        return FindKeyForReserveResult(i, kExactMatchLayerPointer);
      }

      ASSERT_ND(!does_point_to_layer(i));

      if (klen <= sizeof(KeySlice)) {
        continue;  // same as "rec_slice < slice" case
      }

      // see the comment in MasstreeComposerContext::PathLevel.
      // Even if the record is larger than the current key, we consider it "matched" and cause
      // next layer creation, but keeping the larger record in a dummy original page in nexy layer.
      const char* record_suffix = get_record(i);
      if (klen == remainder &&
        std::memcmp(record_suffix, suffix, remainder - sizeof(KeySlice)) == 0) {
        return FindKeyForReserveResult(i, kExactMatchLocalRecord);
      } else {
        return FindKeyForReserveResult(i, kConflictingLocalRecord);
      }
    }
  }
  return FindKeyForReserveResult(key_count, kNotFound);
}

inline void MasstreeBorderPage::reserve_record_space(
  SlotIndex index,
  xct::XctId initial_owner_id,
  KeySlice slice,
  const void* suffix,
  KeyLength remainder_length,
  PayloadLength payload_count) {
  ASSERT_ND(header().snapshot_ || initial_owner_id.is_deleted());
  ASSERT_ND(index < kBorderPageMaxSlots);
  ASSERT_ND(remainder_length <= kMaxKeyLength || remainder_length == kInitiallyNextLayer);
  ASSERT_ND(remainder_length != kInitiallyNextLayer || payload_count == sizeof(DualPagePointer));
  ASSERT_ND(header().snapshot_ || is_locked());
  ASSERT_ND(get_key_count() == index);
  ASSERT_ND(can_accomodate(index, remainder_length, payload_count));
  ASSERT_ND(next_offset_ % 8 == 0);
  const KeyLength suffix_length = calculate_suffix_length(remainder_length);
  const DataOffset record_size = to_record_length(remainder_length, payload_count);
  ASSERT_ND(record_size % 8 == 0);
  const DataOffset new_offset = next_offset_;
  set_slice(index, slice);
  // This is a new slot, so no worry on race.
  Slot* slot = get_new_slot(index);
  slot->lengthes_.components.offset_ = new_offset;
  slot->lengthes_.components.unused_ = 0;
  slot->lengthes_.components.physical_record_length_ = record_size;
  slot->lengthes_.components.payload_length_ = payload_count;
  slot->original_physical_record_length_ = record_size;
  slot->remainder_length_ = remainder_length;
  slot->original_offset_ = new_offset;
  next_offset_ += record_size;
  if (index == 0) {
    consecutive_inserts_ = true;
  } else if (consecutive_inserts_) {
    // do we have to turn off consecutive_inserts_ now?
    const KeySlice prev_slice = get_slice(index - 1);
    const KeyLength prev_klen = get_remainder_length(index - 1);
    if (prev_slice > slice || (prev_slice == slice && prev_klen > remainder_length)) {
      consecutive_inserts_ = false;
    }
  }
  slot->tid_.lock_.reset();
  slot->tid_.xct_id_ = initial_owner_id;
  if (suffix_length > 0) {
    char* record = get_record_from_offset(new_offset);
    std::memcpy(record, suffix, suffix_length);
    KeyLength suffix_length_aligned = assorted::align8(suffix_length);
    // zero-padding
    if (suffix_length_aligned > suffix_length) {
      std::memset(record + suffix_length, 0, suffix_length_aligned - suffix_length);
    }
  }
}

inline void MasstreeBorderPage::reserve_initially_next_layer(
  SlotIndex index,
  xct::XctId initial_owner_id,
  KeySlice slice,
  const DualPagePointer& pointer) {
  ASSERT_ND(index < kBorderPageMaxSlots);
  ASSERT_ND(header().snapshot_ || is_locked());
  ASSERT_ND(get_key_count() == index);
  ASSERT_ND(can_accomodate(index, sizeof(KeySlice), sizeof(DualPagePointer)));
  ASSERT_ND(next_offset_ % 8 == 0);
  ASSERT_ND(initial_owner_id.is_next_layer());
  ASSERT_ND(!initial_owner_id.is_deleted());
  const KeyLength remainder = kInitiallyNextLayer;
  const DataOffset record_size = to_record_length(remainder, sizeof(DualPagePointer));
  ASSERT_ND(record_size % 8 == 0);
  const DataOffset new_offset = next_offset_;
  set_slice(index, slice);
  // This is a new slot, so no worry on race.
  Slot* slot = get_new_slot(index);
  slot->lengthes_.components.offset_ = new_offset;
  slot->lengthes_.components.unused_ = 0;
  slot->lengthes_.components.physical_record_length_ = record_size;
  slot->lengthes_.components.payload_length_ = sizeof(DualPagePointer);
  slot->original_physical_record_length_ = record_size;
  slot->remainder_length_ = remainder;
  slot->original_offset_ = new_offset;
  next_offset_ += record_size;
  if (index == 0) {
    consecutive_inserts_ = true;
  } else if (consecutive_inserts_) {
    // This record must be the only full-length slice, so the check is simpler
    const KeySlice prev_slice = get_slice(index - 1);
    if (prev_slice > slice) {
      consecutive_inserts_ = false;
    }
  }
  slot->tid_.lock_.reset();
  slot->tid_.xct_id_ = initial_owner_id;
  *get_next_layer_from_offsets(new_offset, remainder) = pointer;
}


inline void MasstreeBorderPage::append_next_layer_snapshot(
  xct::XctId initial_owner_id,
  KeySlice slice,
  SnapshotPagePointer pointer) {
  ASSERT_ND(header().snapshot_);  // this is used only from snapshot composer
  const SlotIndex index = get_key_count();
  const KeyLength kRemainder = kInitiallyNextLayer;
  ASSERT_ND(can_accomodate(index, kRemainder, sizeof(DualPagePointer)));
  const DataOffset record_size = to_record_length(kRemainder, sizeof(DualPagePointer));
  const DataOffset offset = next_offset_;
  set_slice(index, slice);
  // This is in snapshot page, so no worry on race.
  Slot* slot = get_new_slot(index);
  slot->lengthes_.components.offset_ = offset;
  slot->lengthes_.components.unused_ = 0;
  slot->lengthes_.components.physical_record_length_ = record_size;
  slot->lengthes_.components.payload_length_ = sizeof(DualPagePointer);
  slot->original_physical_record_length_ = record_size;
  slot->remainder_length_ = kRemainder;
  slot->original_offset_ = offset;
  next_offset_ += record_size;

  slot->tid_.xct_id_ = initial_owner_id;
  slot->tid_.xct_id_.set_next_layer();
  DualPagePointer* dual_pointer = get_next_layer_from_offsets(offset, kRemainder);
  dual_pointer->volatile_pointer_.clear();
  dual_pointer->snapshot_pointer_ = pointer;
  set_key_count(index + 1U);
}

inline void MasstreeBorderPage::replace_next_layer_snapshot(SnapshotPagePointer pointer) {
  ASSERT_ND(header().snapshot_);  // this is used only from snapshot composer
  ASSERT_ND(get_key_count() > 0);
  const SlotIndex index = get_key_count() - 1U;

  ASSERT_ND(!does_point_to_layer(index));
  ASSERT_ND(!get_owner_id(index)->xct_id_.is_next_layer());

  // Overwriting an existing slot with kInitiallyNextLayer is not generally safe,
  // but this is in snapshot page, so no worry on race.
  const KeyLength kRemainder = kInitiallyNextLayer;
  const DataOffset new_record_size = sizeof(DualPagePointer);
  ASSERT_ND(new_record_size == to_record_length(kRemainder, sizeof(DualPagePointer)));

  // This is in snapshot page, so no worry on race.
  Slot* slot = get_slot(index);

  // This is the last record in this page, right?
  ASSERT_ND(next_offset_
    == slot->lengthes_.components.offset_ + slot->lengthes_.components.physical_record_length_);

  // Here, we assume the snapshot border page always leaves sizeof(DualPagePointer) whenever
  // it creates a record. Otherwise we might not have enough space!
  // For this reason, we use can_accomodate_snapshot() rather than can_accomodate().
  ASSERT_ND(slot->lengthes_.components.offset_ + new_record_size + sizeof(Slot) * get_key_count()
    <= sizeof(data_));
  slot->lengthes_.components.physical_record_length_ = new_record_size;
  slot->lengthes_.components.payload_length_ = sizeof(DualPagePointer);
  slot->original_physical_record_length_ = new_record_size;
  slot->remainder_length_ = kRemainder;
  next_offset_ = slot->lengthes_.components.offset_ + new_record_size;

  slot->tid_.xct_id_.set_next_layer();
  DualPagePointer* dual_pointer = get_next_layer(index);
  dual_pointer->volatile_pointer_.clear();
  dual_pointer->snapshot_pointer_ = pointer;

  ASSERT_ND(does_point_to_layer(index));
}

inline bool MasstreeIntermediatePage::is_full_snapshot() const {
  uint16_t keys = get_key_count();
  ASSERT_ND(keys <= kMaxIntermediateSeparators);
  const MiniPage& minipage = mini_pages_[keys];
  uint16_t keys_mini = minipage.key_count_;
  ASSERT_ND(keys_mini <= kMaxIntermediateMiniSeparators);
  return keys == kMaxIntermediateSeparators && keys_mini == kMaxIntermediateMiniSeparators;
}

inline void MasstreeIntermediatePage::append_pointer_snapshot(
  KeySlice low_fence,
  SnapshotPagePointer pointer) {
  ASSERT_ND(header_.snapshot_);
  ASSERT_ND(!is_full_snapshot());
  uint16_t index = get_key_count();
  MiniPage& mini = mini_pages_[index];
  uint16_t index_mini = mini.key_count_;
  ASSERT_ND(low_fence > get_low_fence());
  ASSERT_ND(is_high_fence_supremum() || low_fence < get_high_fence());
  ASSERT_ND(pointer == 0 || extract_snapshot_id_from_snapshot_pointer(pointer) != 0);
  if (index_mini < kMaxIntermediateMiniSeparators) {
    // p0 s0 p1  + "s p" -> p0 s0 p1 s1 p2
    ASSERT_ND(pointer == 0  // composer init_root uses this method to set null pointers..
      || mini.pointers_[index_mini].snapshot_pointer_ != pointer);  // otherwise dup.
    ASSERT_ND(index_mini == 0 || low_fence > mini.separators_[index_mini - 1]);
    ASSERT_ND(index == 0 || low_fence > separators_[index - 1]);
    mini.separators_[index_mini] = low_fence;
    mini.pointers_[index_mini + 1].volatile_pointer_.clear();
    mini.pointers_[index_mini + 1].snapshot_pointer_ = pointer;
    ++mini.key_count_;
  } else {
    append_minipage_snapshot(low_fence, pointer);
  }
}

inline void MasstreeIntermediatePage::append_minipage_snapshot(
  KeySlice low_fence,
  SnapshotPagePointer pointer) {
  ASSERT_ND(header_.snapshot_);
  ASSERT_ND(!is_full_snapshot());
  uint16_t key_count = get_key_count();
  ASSERT_ND(key_count < kMaxIntermediateSeparators);
  ASSERT_ND(mini_pages_[key_count].key_count_ == kMaxIntermediateMiniSeparators);
  ASSERT_ND(low_fence > get_minipage(key_count).separators_[kMaxIntermediateMiniSeparators - 1]);
  separators_[key_count] = low_fence;
  MiniPage& new_minipage = mini_pages_[key_count + 1];
  new_minipage.key_count_ = 0;
  new_minipage.pointers_[0].volatile_pointer_.clear();
  new_minipage.pointers_[0].snapshot_pointer_ = pointer;
  set_key_count(key_count + 1);
}

inline void MasstreeIntermediatePage::extract_separators_common(
  uint8_t index,
  uint8_t index_mini,
  KeySlice* separator_low,
  KeySlice* separator_high) const {
  ASSERT_ND(!is_border());
  uint8_t key_count = get_key_count();
  ASSERT_ND(index <= key_count);
  const MasstreeIntermediatePage::MiniPage& minipage = get_minipage(index);
  ASSERT_ND(index_mini <= minipage.key_count_);
  if (index_mini == 0) {
    if (index == 0) {
      *separator_low = get_low_fence();
    } else {
      *separator_low = get_separator(index - 1U);
    }
  } else {
    *separator_low = minipage.separators_[index_mini - 1U];
  }
  if (index_mini == minipage.key_count_) {
    if (index == key_count) {
      *separator_high = get_high_fence();
    } else {
      *separator_high = get_separator(index);
    }
  } else {
    *separator_high = minipage.separators_[index_mini];
  }
}


inline bool MasstreeBorderPage::can_accomodate(
  SlotIndex new_index,
  KeyLength remainder_length,
  PayloadLength payload_count) const {
  if (new_index == 0) {
    ASSERT_ND(remainder_length <= kMaxKeyLength || remainder_length == kInitiallyNextLayer);
    ASSERT_ND(payload_count <= kMaxPayloadLength);
    return true;
  } else if (new_index >= kBorderPageMaxSlots) {
    return false;
  }
  const DataOffset required = required_data_space(remainder_length, payload_count);
  const DataOffset available = available_space();
  return required <= available;
}
inline bool MasstreeBorderPage::can_accomodate_snapshot(
  KeyLength remainder_length,
  PayloadLength payload_count) const {
  ASSERT_ND(header_.snapshot_);
  SlotIndex new_index = get_key_count();
  if (new_index == 0) {
    return true;
  } else if (new_index >= kBorderPageMaxSlots) {
    return false;
  }
  PayloadLength adjusted_payload = std::max<PayloadLength>(payload_count, sizeof(DualPagePointer));
  const DataOffset required = required_data_space(remainder_length, adjusted_payload);
  const DataOffset available = available_space();
  return required <= available;
}
inline bool MasstreeBorderPage::compare_key(
  SlotIndex index,
  const void* be_key,
  KeyLength key_length) const {
  ASSERT_ND(index < kBorderPageMaxSlots);
  KeyLength remainder = key_length - get_layer() * sizeof(KeySlice);
  const KeyLength klen = get_remainder_length(index);
  if (remainder != klen) {
    return false;
  }
  KeySlice slice = slice_layer(be_key, key_length, get_layer());
  const KeySlice rec_slice = get_slice(index);
  if (slice != rec_slice) {
    return false;
  }
  if (remainder > sizeof(KeySlice)) {
    return std::memcmp(
      reinterpret_cast<const char*>(be_key) + (get_layer() + 1) * sizeof(KeySlice),
      get_record(index),
      remainder - sizeof(KeySlice)) == 0;
  } else {
    return true;
  }
}
inline bool MasstreeBorderPage::equal_key(
  SlotIndex index,
  const void* be_key,
  KeyLength key_length) const {
  return compare_key(index, be_key, key_length);
}

inline int MasstreeBorderPage::ltgt_key(
  SlotIndex index,
  const char* be_key,
  KeyLength key_length) const {
  ASSERT_ND(key_length > get_layer() * kSliceLen);
  KeyLength remainder = key_length - get_layer() * kSliceLen;
  KeySlice slice = slice_layer(be_key, key_length, get_layer());
  const char* suffix = be_key + (get_layer() + 1) * kSliceLen;
  return ltgt_key(index, slice, suffix, remainder);
}
inline int MasstreeBorderPage::ltgt_key(
  SlotIndex index,
  KeySlice slice,
  const char* suffix,
  KeyLength remainder) const {
  ASSERT_ND(index < kBorderPageMaxSlots);
  ASSERT_ND(remainder > 0);
  ASSERT_ND(remainder <= kMaxKeyLength);
  KeyLength rec_remainder = get_remainder_length(index);
  KeySlice rec_slice = get_slice(index);
  if (slice < rec_slice) {
    return -1;
  } else if (slice > rec_slice) {
    return 1;
  }

  if (remainder <= kSliceLen) {
    if (remainder == rec_remainder) {
      return 0;
    } else if (remainder < rec_remainder) {
      return -1;
    } else {
      return 1;
    }
  }

  ASSERT_ND(remainder > kSliceLen);
  if (rec_remainder <= kSliceLen) {
    return 1;
  }
  if (does_point_to_layer(index)) {
    return 0;
  }
  ASSERT_ND(rec_remainder <= kMaxKeyLength);
  KeyLength min_remainder = std::min(remainder, rec_remainder);
  const char* rec_suffix = get_record(index);
  int cmp = std::memcmp(suffix, rec_suffix, min_remainder);
  if (cmp != 0) {
    return cmp;
  }

  if (remainder == rec_remainder) {
    return 0;
  } else if (remainder < rec_remainder) {
    return -1;
  } else {
    return 1;
  }
}
inline bool MasstreeBorderPage::will_conflict(
  SlotIndex index,
  const char* be_key,
  KeyLength key_length) const {
  ASSERT_ND(key_length > get_layer() * kSliceLen);
  KeyLength remainder = key_length - get_layer() * kSliceLen;
  KeySlice slice = slice_layer(be_key, key_length, get_layer());
  return will_conflict(index, slice, remainder);
}
inline bool MasstreeBorderPage::will_conflict(
  SlotIndex index,
  KeySlice slice,
  KeyLength remainder) const {
  ASSERT_ND(index < kBorderPageMaxSlots);
  ASSERT_ND(remainder > 0);
  if (slice != get_slice(index)) {
    return false;
  }
  KeyLength rec_remainder = get_remainder_length(index);
  if (remainder > kSliceLen && rec_remainder > kSliceLen) {
    return true;  // need to create next layer
  }
  return (remainder == rec_remainder);  // if same, it's exactly the same key
}
inline bool MasstreeBorderPage::will_contain_next_layer(
  SlotIndex index,
  const char* be_key,
  KeyLength key_length) const {
  ASSERT_ND(key_length > get_layer() * kSliceLen);
  KeyLength remainder = key_length - get_layer() * kSliceLen;
  KeySlice slice = slice_layer(be_key, key_length, get_layer());
  return will_contain_next_layer(index, slice, remainder);
}
inline bool MasstreeBorderPage::will_contain_next_layer(
  SlotIndex index,
  KeySlice slice,
  KeyLength remainder) const {
  ASSERT_ND(index < kBorderPageMaxSlots);
  ASSERT_ND(remainder > 0);
  return slice == get_slice(index) && does_point_to_layer(index) && remainder > kSliceLen;
}

// We must place static asserts at the end, otherwise doxygen gets confused (most likely its bug)
STATIC_SIZE_CHECK(sizeof(MasstreePage), kCommonPageHeaderSize)
STATIC_SIZE_CHECK(sizeof(MasstreeBorderPage), kPageSize)
STATIC_SIZE_CHECK(sizeof(MasstreeIntermediatePage::MiniPage), 128 + 256)
STATIC_SIZE_CHECK(sizeof(MasstreeIntermediatePage), kPageSize)
STATIC_SIZE_CHECK(kBorderPageDataPartOffset, kPageSize - kBorderPageDataPartSize)

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_IMPL_HPP_
