/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_PAGE_HPP_
#define FOEDUS_STORAGE_PAGE_HPP_
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/storage/storage_id.hpp"
namespace foedus {
namespace storage {
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
   * @brief Snapshot Page ID of this page.
   * @details
   * As the name suggests, this is set only when this page becomes a snapshot page.
   * When this page image is a volatile page, there is no point to store page ID
   * because the pointer address tells it.
   */
  SnapshotPagePointer snapshot_page_id_;  // +8 -> 8

  /**
   * ID of the storage this page belongs to.
   */
  StorageId           storage_id_;        // +4 -> 16

  /**
   * Checksum of the content of this page to detect corrupted pages.
   * Changes only when this page becomes a snapshot page.
   */
  Checksum            checksum_;          // +4 -> 16

  // No instantiation.
  PageHeader() CXX11_FUNC_DELETE;
  PageHeader(const PageHeader& other) CXX11_FUNC_DELETE;
  PageHeader& operator=(const PageHeader& other) CXX11_FUNC_DELETE;
};

/**
 * @brief Just a marker to denote a memory region represents a data page.
 * @ingroup STORAGE
 * @details
 * We don't instantiate this object nor derive from this. This is just a marker.
 * Because PageHeader is extended in each storage, even the data_ address is not correct.
 * We thus make everything private to prevent misuse.
 */
struct Page CXX11_FINAL {
 private:
  PageHeader  header_;
  char        data_[kPageSize - sizeof(PageHeader)];

  // No instantiation.
  Page() CXX11_FUNC_DELETE;
  Page(const Page& other) CXX11_FUNC_DELETE;
  Page& operator=(const Page& other) CXX11_FUNC_DELETE;
};

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_PAGE_HPP_
