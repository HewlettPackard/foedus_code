/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_PAGE_HPP_
#define FOEDUS_STORAGE_PAGE_HPP_

#include <cstring>

#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/thread_id.hpp"

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
  kMasstreeBoundaryPageType = 3,
  kSequentialPageType = 4,
  kSequentialRootPageType = 5,
  kHashRootPageType = 6,
  kHashBinPageType = 7,
  kHashDataPageType = 8,
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
   * Whether this page is a root page, which exists only one per storage and has no parent
   * pointer. (some storage types looks like having multiple root pages, but actually
   * it has only one root-of-root even in that case.)
   */
  bool          root_;        // +1 -> 19

  /**
   * Which node's thread has updated this page most recently.
   * This is one of the properties that don't have permanent meaning.
   * We don't maintain this property transactionally. This is used only as a statistics for
   * partitioning.
   */
  thread::ThreadGroupId stat_latest_modifier_;  // +1 -> 20

  /** Same above. When the modification happened. We use this to keep hot volatile pages. */
  Epoch         stat_latest_modify_epoch_;  // +4 -> 24

  /**
   * Pointer to parent page is always only a pointer to a volatile page, not dual pointer.
   * This is another property that doesn't have permanent meaning.
   * We don't store the parent pointer in snapshot. Instead, a volatile page always has
   * a parent pointer that is non-null unless this page is the root.
   * We drop volatile pages only when we have dropped all volatile pages of its descendants,
   * so there is no case where this parent pointer becomes invalid in volatile pages.
   */
  Page*         volatile_parent_;   // +8  -> 32

  // No instantiation.
  PageHeader() CXX11_FUNC_DELETE;
  PageHeader(const PageHeader& other) CXX11_FUNC_DELETE;
  PageHeader& operator=(const PageHeader& other) CXX11_FUNC_DELETE;

  PageType get_page_type() const { return static_cast<PageType>(page_type_); }

  inline void init_volatile(
    VolatilePagePointer page_id,
    StorageId storage_id,
    PageType page_type,
    bool root,
    Page* parent) ALWAYS_INLINE {
    page_id_ = page_id.word;
    storage_id_ = storage_id;
    checksum_ = 0;
    page_type_ = page_type;
    snapshot_ = false;
    root_ = root;
    stat_latest_modifier_ = 0;
    stat_latest_modify_epoch_ = Epoch();
    volatile_parent_ = parent;
    ASSERT_ND((root && parent == CXX11_NULLPTR) || (!root && parent));
  }

  inline void init_snapshot(
    SnapshotPagePointer page_id,
    StorageId storage_id,
    PageType page_type,
    bool root) ALWAYS_INLINE {
    page_id_ = page_id;
    storage_id_ = storage_id;
    checksum_ = 0;
    page_type_ = page_type;
    snapshot_ = true;
    root_ = root;
    stat_latest_modifier_ = 0;
    stat_latest_modify_epoch_ = Epoch();
    volatile_parent_ = CXX11_NULLPTR;
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

 private:
  PageHeader  header_;
  char        data_[kPageSize - sizeof(PageHeader)];

  // No instantiation.

  Page() CXX11_FUNC_DELETE;
  Page(const Page& other) CXX11_FUNC_DELETE;
  Page& operator=(const Page& other) CXX11_FUNC_DELETE;
};

/**
 * @brief Callback interface to initialize a volatile page.
 * @ingroup STORAGE
 * @details
 * This is used when a method might initialize a volatile page (eg following a page pointer).
 * Page initialization depends on page type and some of them needs additional parameters,
 * so we made it this object. One virtual function call overhead, but more flexible and no
 * need for lambda.
 */
struct VolatilePageInitializer {
  VolatilePageInitializer(StorageId storage_id, PageType page_type, bool root, Page* parent)
    : storage_id_(storage_id), page_type_(page_type), root_(root), parent_(parent) {
  }
  // no virtual destructor for better performance. make sure derived class doesn't need
  // any explicit destruction.

  inline void initialize(Page* page, VolatilePagePointer page_id) const ALWAYS_INLINE {
    std::memset(page, 0, kPageSize);
    page->get_header().init_volatile(page_id, storage_id_, page_type_, root_, parent_);
    initialize_more(page);
  }

  /** Implement this in derived class to do additional initialization. */
  virtual void initialize_more(Page* page) const = 0;

  const StorageId storage_id_;
  const PageType  page_type_;
  const bool      root_;
  Page* const     parent_;
};

/**
 * @brief Empty implementation of VolatilePageInitializer.
 * @ingroup STORAGE
 * @details
 * Use this if new page is never created (tolerate_null_page).
 */
struct DummyVolatilePageInitializer CXX11_FINAL : public VolatilePageInitializer {
  DummyVolatilePageInitializer()
    : VolatilePageInitializer(0, kUnknownPageType, true, nullptr) {
  }
  void initialize_more(Page* /*page*/) const CXX11_OVERRIDE {}
};

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_PAGE_HPP_
