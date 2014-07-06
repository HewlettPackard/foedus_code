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
   * @brief Page ID of this page.
   * @details
   * If this page is a snapshot page, it stores SnapshotPagePointer.
   * If this page is a volatile page, it stores VolatilePagePointer.
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

  // No instantiation.
  PageHeader() CXX11_FUNC_DELETE;
  PageHeader(const PageHeader& other) CXX11_FUNC_DELETE;
  PageHeader& operator=(const PageHeader& other) CXX11_FUNC_DELETE;
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

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_PAGE_HPP_
