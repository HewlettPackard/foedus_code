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
#ifndef FOEDUS_STORAGE_PAGE_HPP_
#define FOEDUS_STORAGE_PAGE_HPP_

#include <cstring>
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
/**
 * @brief The following 1-byte value is stored in the common page header.
 * @ingroup STORAGE
 * @details
 * These values are stored in snapshot pages too, so we must keep values' compatibility.
 * Specify values for that reason.
 */
enum PageType {
  kUnknownPageType = 0,
  kArrayPageType = 1,
  kMasstreeIntermediatePageType = 2,
  kMasstreeBorderPageType = 3,
  kSequentialPageType = 4,
  kSequentialRootPageType = 5,
  kHashIntermediatePageType = 6,
  kHashDataPageType = 7,
  kHashComposedBinsPageType = 8,
  kDummyLastPageType,
};

struct PageVersionStatus CXX11_FINAL {
  enum Constants {
    kRetiredBit = 1 << 31,
    kMovedBit = 1 << 30,
    /** so far used only in hash storage, where data page forms a linked list */
    kHasNextPageBit = 1 << 29,
    kReservedBit2 = 1 << 28,
    kVersionMask = 0x0FFFFFFF,
  };
  PageVersionStatus() : status_(0) {}
  void    reset() ALWAYS_INLINE { status_ = 0; }

  bool    is_moved() const ALWAYS_INLINE { return (status_ & kMovedBit) != 0; }
  bool    is_retired() const ALWAYS_INLINE { return (status_ & kRetiredBit) != 0; }
  bool    has_next_page() const ALWAYS_INLINE { return (status_ & kHasNextPageBit) != 0; }

  bool operator==(const PageVersionStatus& other) const ALWAYS_INLINE {
    return status_ == other.status_;
  }
  bool operator!=(const PageVersionStatus& other) const ALWAYS_INLINE {
    return status_ != other.status_;
  }

  void      set_moved() ALWAYS_INLINE {
    ASSERT_ND(!is_moved());
    status_ |= kMovedBit;
  }
  void      set_retired() ALWAYS_INLINE {
    ASSERT_ND(is_moved());  // we always set moved bit first. retire must happen later.
    ASSERT_ND(!is_retired());
    status_ |= kRetiredBit;
  }
  void      set_has_next_page() ALWAYS_INLINE {
    ASSERT_ND(!is_moved());
    ASSERT_ND(!is_retired());
    ASSERT_ND(!has_next_page());
    status_ |= kHasNextPageBit;
  }

  uint32_t  get_version_counter() const ALWAYS_INLINE {
    return status_ & kVersionMask;
  }
  void      increment_version_counter() ALWAYS_INLINE {
    // we do this only when we insert a new key or split, so this never overflows.
    ASSERT_ND(get_version_counter() < kVersionMask);
    ++status_;
  }

  friend std::ostream& operator<<(std::ostream& o, const PageVersionStatus& v);

  uint32_t      status_;
};

/**
 * @brief Just a synonym of XctId to be used as a page lock mechanism.
 * @ingroup STORAGE
 * @details
 * Each page has this in the header.
 * Unlike [YANDONG12], this is just an McsLock.
 * We maintain key count and permutation differently from [YANDONG12].
 *
 * "is_deleted" flag is called "is_retired" to clarify what deletion means for a page.
 * Also, epoch/ordinal has not much meaning for page. So, we only increment the ordinal part
 * for each change. Epoch part is unused.
 *
 * This object is a POD.
 * All methods are inlined except stream.
 */
struct PageVersion CXX11_FINAL {
  PageVersion() ALWAYS_INLINE : lock_(), status_() {}

  /** used only while page initialization */
  void    reset() ALWAYS_INLINE {
    lock_.reset();
    status_.reset();
  }

  bool    is_locked() const ALWAYS_INLINE { return lock_.is_locked(); }
  bool    is_moved() const ALWAYS_INLINE { return status_.is_moved(); }
  bool    is_retired() const ALWAYS_INLINE { return status_.is_retired(); }
  bool    has_next_page() const ALWAYS_INLINE { return status_.has_next_page(); }

  bool operator==(const PageVersion& other) const ALWAYS_INLINE { return status_ == other.status_; }
  bool operator!=(const PageVersion& other) const ALWAYS_INLINE { return status_ != other.status_; }

  void      set_moved() ALWAYS_INLINE {
    ASSERT_ND(is_locked());
    status_.set_moved();
  }
  void      set_retired() ALWAYS_INLINE {
    ASSERT_ND(is_locked());
    status_.set_retired();
  }
  void      set_has_next_page() ALWAYS_INLINE {
    ASSERT_ND(is_locked());
    status_.set_has_next_page();
  }

  uint32_t  get_version_counter() const ALWAYS_INLINE {
    return status_.get_version_counter();
  }
  void      increment_version_counter() ALWAYS_INLINE {
    ASSERT_ND(is_locked());
    status_.increment_version_counter();
  }

  /**
  * @brief Locks the page, spinning if necessary.
  */
  xct::McsBlockIndex lock(thread::Thread* context) ALWAYS_INLINE {
    return lock_.acquire_lock(context);
  }

  /**
  * @brief Unlocks the given page version, assuming the caller has locked it.
  * @pre is_locked()
  * @pre this thread locked it (can't check it, but this is the rule)
  * @details
  * This method also increments the version counter to declare a change in this page.
  */
  void unlock_changed(thread::Thread* context, xct::McsBlockIndex block) ALWAYS_INLINE {
    increment_version_counter();
    lock_.release_lock(context, block);
  }
  /** this one doesn't increment the counter. used when the lock owner didn't make any change */
  void unlock_nochange(thread::Thread* context, xct::McsBlockIndex block) ALWAYS_INLINE {
    lock_.release_lock(context, block);
  }


  friend std::ostream& operator<<(std::ostream& o, const PageVersion& v);

  xct::McsLock      lock_;    // +8 -> 8
  PageVersionStatus status_;  // +4 -> 12
  uint32_t          unused_;  // +4 -> 16. this space might be used for interesting range "lock".
};
STATIC_SIZE_CHECK(sizeof(PageVersion), 16)

struct PageVersionLockScope {
  PageVersionLockScope(thread::Thread* context, PageVersion* version, bool non_racy_lock = false);
  ~PageVersionLockScope() { release(); }

  /**
   * Convert an existing McsLockScope on page-version to this object.
   * This is a tentative solution. Now that page-version doesn't need version counter,
   * we should be able to use McsLockScope everywhere.
   * @pre move_from->is_locked()
   * @post !move_from->is_locked() (we steal the lock from the arg)
   */
  explicit PageVersionLockScope(xct::McsLockScope* move_from);

  /**
   * A \e move operator that takes over the other lock, consisting of:
   * \li Releases this lock if !released
   * \li Copies all members in move_from
   * \li Sets "released_" in move_from without releasing the lock
   *
   * We don't need c++11 in this case because everything in this object are pointers.
   */
  void take_over(PageVersionLockScope* move_from);

  void set_changed() { changed_ = true; }
  void release();

  thread::Thread* context_;
  PageVersion* version_;
  xct::McsBlockIndex block_;
  bool changed_;
  bool released_;
};

/**
 * @brief Just a marker to denote that a memory region represents a data page.
 * @ingroup STORAGE
 * @details
 * Each storage page class contains this at the beginning to declare common properties.
 * In other words, we can always reinterpret_cast a page pointer to this object and
 * get/set basic properties.
 */
struct PageHeader CXX11_FINAL {
  static const uint8_t kHotThreshold = 10;
  static const uint8_t kInvalidHotness = 0xff;
  /**
   * @brief Page ID of this page.
   * @details
   * If this page is a snapshot page, it stores SnapshotPagePointer.
   * If this page is a volatile page, it stores VolatilePagePointer (only numa_node/offset matters).
   */
  uint64_t      page_id_;     // +8 -> 8

  /**
   * ID of the storage this page belongs to.
   */
  StorageId     storage_id_;  // +4 -> 12

  /**
   * Checksum of the content of this page to detect corrupted pages.
   * Changes only when this page becomes a snapshot page.
   */
  Checksum      checksum_;    // +4 -> 16

  /** Actually of PageType. */
  uint8_t       page_type_;   // +1 -> 17

  /**
   * Whether this page image is of a snapshot page.
   * This is one of the properties that don't have permanent meaning.
   */
  bool          snapshot_;    // +1 -> 18

  /**
   * \e physical key count (those keys might be deleted) in this page.
   * It depends on the page type what "key count" exactly means.
   * For example, in masstree interior page, key count is a separator count, so
   * it contains pointers of key_count_+1. In many pages, key_count=record_count.
   */
  uint16_t      key_count_;   // +2 -> 20

  /**
   * used only in masstree.
   * Layer-0 stores the first 8 byte slice, Layer-1 next 8 byte...
   * @note This field is now used as "bin_shifts" in hash storage. should rename..
   */
  uint8_t       masstree_layer_;  // +1 -> 21

  /**
   * used only in masstree.
   * \e Logical B-tree level of the page.
   * Border pages are always level-0 whether they are foster-child or not.
   * Intermediate pages right above border pages are level-1, their parents are level-2, ...
   * Foster-child of an intermediate page has the same level as its foster parent.
   * Again, this is a logical level, not physical.
   *
   * Our implementation of masstree might have unbalanced sub-trees. In that case,
   * an interemediate page's level is max(child's level) + 1.
   * This imbalance can happen only at the root page of the first layer because of how
   * the masstree composer work. Other than that, all B-tree nodes are balanced.
   *
   * @todo this should be renamed to in_layer_level. now both masstree and hash use this.
   */
  uint8_t       masstree_in_layer_level_;     // +1 -> 22

  /**
   * A loosely maintained statistics for volatile pages.
   * The NUMA node of the thread that has most recently tried to update this page.
   * This is not atomically/transactionally maintained and should be used only as a stat.
   * Depending on page type, this might not be even maintained (eg implicit in sequential pages).
   */
  uint8_t       stat_last_updater_node_;       // +1 -> 23

  /**
   * Loosely maintained statistics on data temperature.
   * This is a probabilistic counter: 0->1 = 100%, 1->2 = 50%, 2->3 = 25%, etc.
   * So it grows exponentially, which should make it easy enough to tell an obviously
   * hot record/page.
   *
   * XXX(tzwang): This field only makes sense for volatile pages.
   *
   * Among the preceeding word-size-algned fields (masstree_layer_, masstree_in_layer_level_,
   * and stat_last_updater_node_), masstree_* are not changed once page constructed;
   * stat_last_updater_node_ is loosely maintained stat. So we don't need to use atomic
   * write for this field.
   *
   * Assuming 4-byte word size ($getconf WORD_BIT).
   */
  uint8_t       hotness_;                    // +1 -> 24
  /**
   * Used in several storage types as concurrency control mechanism for the page.
   */
  PageVersion   page_version_;   // +16  -> 40

  // No instantiation.
  PageHeader() CXX11_FUNC_DELETE;
  PageHeader(const PageHeader& other) CXX11_FUNC_DELETE;
  PageHeader& operator=(const PageHeader& other) CXX11_FUNC_DELETE;
  friend std::ostream& operator<<(std::ostream& o, const PageHeader& v);

  PageType get_page_type() const { return static_cast<PageType>(page_type_); }
  uint8_t get_in_layer_level() const { return masstree_in_layer_level_; }
  void set_in_layer_level(uint8_t level) { masstree_in_layer_level_ = level; }
  uint8_t* hotness_address() { return &hotness_; }

  inline void init_volatile(
    VolatilePagePointer page_id,
    StorageId storage_id,
    PageType page_type) ALWAYS_INLINE {
    page_id_ = page_id.word;
    storage_id_ = storage_id;
    checksum_ = 0;
    page_type_ = page_type;
    snapshot_ = false;
    key_count_ = 0;
    masstree_layer_ = 0;
    masstree_in_layer_level_ = 0;
    stat_last_updater_node_ = page_id.components.numa_node;
    hotness_ = 0;
    page_version_.reset();
  }

  inline void init_snapshot(
    SnapshotPagePointer page_id,
    StorageId storage_id,
    PageType page_type) ALWAYS_INLINE {
    page_id_ = page_id;
    storage_id_ = storage_id;
    checksum_ = 0;
    page_type_ = page_type;
    snapshot_ = true;
    key_count_ = 0;
    masstree_layer_ = 0;
    masstree_in_layer_level_ = 0;
    stat_last_updater_node_ = extract_numa_node_from_snapshot_pointer(page_id);
    hotness_ = kInvalidHotness;
    page_version_.reset();
  }

  void      increment_key_count() ALWAYS_INLINE {
    ASSERT_ND(snapshot_ || page_version_.is_locked());
    ++key_count_;
  }
  void      set_key_count(uint8_t key_count) ALWAYS_INLINE {
    ASSERT_ND(snapshot_ || page_version_.is_locked());
    key_count_ = key_count;
  }

  static void increase_hotness(uint8_t* haddr) ALWAYS_INLINE {
    (*haddr)++;
  }
  /* FIXME(tzwang): one option here is to decrement the counter when a transaction
   * committed without taking shared lock on a record (not 100% accurate unless
   * we keep stats per record).
   */
  static void decrease_htoness(uint8_t* haddr) ALWAYS_INLINE {
    (*haddr)--;
  }
  static void reset_hotness(uint8_t* haddr) ALWAYS_INLINE {
    *haddr = 0;
  }
  static bool contains_hot_records(uint8_t* haddr) ALWAYS_INLINE {
    return *haddr >= kHotThreshold;
  }
};

/**
 * @brief Just a marker to denote that the memory region represents a data page.
 * @ingroup STORAGE
 * @details
 * We don't instantiate this object nor derive from this. This is just a marker.
 * Because derived page objects have more header properties and even the data_ is layed out
 * differently. We thus make everything private to prevent misuse.
 * @attention Remember, anyway we don't have RTTI for data pages. They are just byte arrays used
 * with reinterpret_cast.
 */
struct Page CXX11_FINAL {
  /** At least the basic header exists in all pages. */
  PageHeader&  get_header()              { return header_; }
  const PageHeader&  get_header() const  { return header_; }
  PageType     get_page_type() const     { return header_.get_page_type(); }
  char*        get_data() { return data_; }
  const char*  get_data() const { return data_; }

 private:
  PageHeader  header_;
  char        data_[kPageSize - sizeof(PageHeader)];

  // No instantiation.

  Page() CXX11_FUNC_DELETE;
  Page(const Page& other) CXX11_FUNC_DELETE;
  Page& operator=(const Page& other) CXX11_FUNC_DELETE;
};

/**
 * @brief Set of arguments, both inputs and outputs, given to each volatile page initializer.
 * @ingroup STORAGE
 */
struct VolatilePageInitArguments {
  /** [IN] Thread on which the procedure is running */
  thread::Thread* context_;
  /** [IN] New page ID */
  VolatilePagePointer page_id;
  /** [IN, OUT] The new page to initialize. */
  Page*           page_;
  /** [IN] Parent of the new page. */
  const Page*     parent_;
  /** [IN] Some index (meaning depends on page type) of pointer in parent page to the new page. */
  uint16_t        index_in_parent_;
};

/**
 * @brief A function pointer to initialize a volatile page.
 * @ingroup STORAGE
 * @details
 * Used as a callback argument to follow_page_pointer.
 * This is used when a method might initialize a volatile page (eg following a page pointer).
 * Page initialization depends on page type and many of them need custom logic,
 * so we made it a callback.
 */
typedef void (*VolatilePageInit)(const VolatilePageInitArguments& args);

/**
 * @brief super-dirty way to obtain Page the address belongs to.
 * @ingroup STORAGE
 * @details
 * because all pages are 4kb aligned, we can just divide and multiply.
 */
inline Page* to_page(const void* address) {
  uintptr_t int_address = reinterpret_cast<uintptr_t>(address);
  uint64_t aligned_address = static_cast<uint64_t>(int_address) / kPageSize * kPageSize;
  return reinterpret_cast<Page*>(
    reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(aligned_address)));
}

inline void assert_aligned_page(const void* page) {
  ASSERT_ND(page);
  ASSERT_ND(reinterpret_cast<uintptr_t>(page) % kPageSize == 0);
}

inline void assert_valid_volatile_page(const Page* page, uint32_t offset) {
  ASSERT_ND(page);
  ASSERT_ND(offset);
#ifndef NDEBUG
  assert_aligned_page(page);
  PageType type = page->get_header().get_page_type();
  ASSERT_ND(type >= kArrayPageType);
  ASSERT_ND(type < kDummyLastPageType);
  VolatilePagePointer pointer;
  pointer.word = page->get_header().page_id_;
  ASSERT_ND(pointer.components.offset == offset);
#endif  // NDEBUG
}

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_PAGE_HPP_
