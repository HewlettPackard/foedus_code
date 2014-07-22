/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_VERSION_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_VERSION_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/raw_atomics.hpp"

namespace foedus {
namespace storage {
namespace masstree {

// some of them are 64bit uint, so can't use enum.
const uint64_t  kPageVersionLockedBit    = (1ULL << 63);
const uint64_t  kPageVersionInsertingBit = (1ULL << 62);
const uint64_t  kPageVersionSplittingBit = (1ULL << 61);
const uint64_t  kPageVersionDeletedBit   = (1ULL << 60);
const uint64_t  kPageVersionIsRootBit    = (1ULL << 59);
const uint64_t  kPageVersionIsBorderBit  = (1ULL << 58);
const uint64_t  kPageVersionInsertionCounterMask  = 0x03F8000000000000ULL;
const uint8_t   kPageVersionInsertionCounterShifts = 51;
const uint64_t  kPageVersionSplitCounterMask      = 0x0007FFFE00000000ULL;
const uint8_t   kPageVersionSplitCounterShifts    = 33;
const uint32_t  kPageVersionKeyCountMask          = 0xFFFF0000U;
const uint8_t   kPageVersionKeyCountShifts        = 16;
const uint32_t  kPageVersionLayerMask             = 0x0000FF00U;
const uint8_t   kPageVersionLayerShifts           = 8;

const uint64_t  kPageVersionUnlockMask =
  (kPageVersionIsRootBit |
  kPageVersionIsBorderBit |
  kPageVersionKeyCountMask |
  kPageVersionLayerMask);

/**
 * @brief 64bit in-page version counter and also locking mechanism.
 * @ingroup MASSTREE
 * @details
 * Each page has this in the header.
 * \li bit-0: locked
 * \li bit-1: inserting
 * \li bit-2: splitting
 * \li bit-3: deleted
 * \li bit-4: is_root
 * \li bit-5: is_border
 * \li bit-[6,13): insert counter
 * \li bit-[13,31): split counter (do we need this much..?)
 * \li bit-31: unused
 * \li bit-[32,48): \e physical key count (those keys might be deleted)
 * \li bit-[48,56): layer (not a mutable property, placed here just to save space)
 * \li bit-[56,64): unused
 * Unlike [YANDONG12], this is 64bit to also contain a key count.
 * We maintain key count and permutation differently from [YANDONG12].
 *
 * This object is a POD.
 * All methods are inlined except stream.
 */
struct MasstreePageVersion CXX11_FINAL {
  MasstreePageVersion() ALWAYS_INLINE : data_(0) {}
  explicit MasstreePageVersion(uint64_t data) ALWAYS_INLINE : data_(data) {}

  void    set_data(uint64_t data) ALWAYS_INLINE { data_ = data; }

  bool    is_locked() const ALWAYS_INLINE { return data_ & kPageVersionLockedBit; }
  bool    is_inserting() const ALWAYS_INLINE { return data_ & kPageVersionInsertingBit; }
  bool    is_splitting() const ALWAYS_INLINE { return data_ & kPageVersionSplittingBit; }
  bool    is_deleted() const ALWAYS_INLINE { return data_ & kPageVersionDeletedBit; }
  bool    is_root() const ALWAYS_INLINE { return data_ & kPageVersionIsRootBit; }
  bool    is_border() const ALWAYS_INLINE { return data_ & kPageVersionIsBorderBit; }
  uint32_t  get_insert_counter() const ALWAYS_INLINE {
    return (data_ & kPageVersionInsertionCounterMask) >> kPageVersionInsertionCounterShifts;
  }
  uint32_t  get_split_counter() const ALWAYS_INLINE {
    return (data_ & kPageVersionSplitCounterMask) >> kPageVersionSplitCounterShifts;
  }
  uint16_t  get_key_count() const ALWAYS_INLINE {
    return (data_ & kPageVersionKeyCountMask) >> kPageVersionKeyCountShifts;
  }

  /** Layer-0 stores the first 8 byte slice, Layer-1 next 8 byte... */
  uint8_t   get_layer() const ALWAYS_INLINE {
    return (data_ & kPageVersionLayerMask) >> kPageVersionLayerShifts;
  }

  void      set_inserting() ALWAYS_INLINE {
    ASSERT_ND(is_locked());
    data_ |= kPageVersionInsertingBit;
  }
  void      set_inserting_and_increment_key_count() ALWAYS_INLINE {
    set_inserting();
    data_ += (1ULL << kPageVersionKeyCountShifts);
  }

  /**
  * @brief Spins until we observe a non-inserting and non-splitting version.
  * @return version of this page that wasn't during modification.
  */
  MasstreePageVersion stable_version() const ALWAYS_INLINE;

  /**
  * @brief Locks the page, spinning if necessary.
  * @details
  * After taking lock, you might want to additionally set inserting/splitting bits.
  * Those can be done just as a usual write once you get a lock.
  */
  void lock_version() ALWAYS_INLINE;

  /**
  * @brief Unlocks the given page version, assuming the caller has locked it.
  * @pre page_version_ & kPageVersionLockedBit (we must have locked it)
  * @pre this thread locked it (can't check it, but this is the rule)
  * @details
  * This method also takes fences before/after unlock to make it safe.
  */
  void unlock_version() ALWAYS_INLINE;

  friend std::ostream& operator<<(std::ostream& o, const MasstreePageVersion& v);

  uint64_t data_;
};
STATIC_SIZE_CHECK(sizeof(MasstreePageVersion), 8)

inline MasstreePageVersion MasstreePageVersion::stable_version() const {
  assorted::memory_fence_acquire();
  SPINLOCK_WHILE(true) {
    uint64_t ver = data_;
    if ((ver & (kPageVersionInsertingBit | kPageVersionSplittingBit)) == 0) {
      return MasstreePageVersion(ver);
    } else {
      assorted::memory_fence_acquire();
    }
  }
}

inline void MasstreePageVersion::lock_version() {
  SPINLOCK_WHILE(true) {
    uint64_t ver = data_;
    if (ver & kPageVersionLockedBit) {
      continue;
    }
    uint64_t new_ver = ver | kPageVersionLockedBit;
    if (assorted::raw_atomic_compare_exchange_strong<uint64_t>(&data_, &ver, new_ver)) {
      ASSERT_ND(data_ & kPageVersionLockedBit);
      return;
    }
  }
}

inline void MasstreePageVersion::unlock_version() {
  uint64_t page_version = data_;
  ASSERT_ND(page_version & kPageVersionLockedBit);
  uint64_t base = page_version & kPageVersionUnlockMask;
  uint64_t insertion_counter = page_version & kPageVersionInsertionCounterMask;
  if (page_version & kPageVersionInsertingBit) {
    insertion_counter += (1ULL << kPageVersionInsertionCounterShifts);
  }
  uint64_t split_counter = page_version & kPageVersionInsertionCounterMask;
  if (page_version & kPageVersionSplittingBit) {
    split_counter += (1ULL << kPageVersionSplitCounterShifts);
  }
  ASSERT_ND((insertion_counter & split_counter) == 0);
  assorted::memory_fence_release();
  data_ = base | insertion_counter | split_counter;
  assorted::memory_fence_release();
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_PAGE_VERSION_HPP_
