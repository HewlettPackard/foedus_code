/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_PAGE_HPP_
#define FOEDUS_STORAGE_PAGE_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/epoch.hpp>
#include <foedus/storage/storage_id.hpp>
namespace foedus {
namespace storage {
/**
 * @brief Just a marker to denote that a memory region represents a data page.
 * @ingroup STORAGE
 * @details
 * Each storage class derives from this to define their page headers.
 * In other words, this object contains the common header items regardless of storage types.
 */
struct PageHeader {
    /**
     * Checksum of the content of this page to detect corrupted pages.
     * Changes only when we save it to media. No synchronization needed to access.
     */
    Checksum            checksum_;          // +8 -> 8

    /**
     * Epoch of this page when this page is written to media (as of the log gleaning).
     * This might not be maintained in volatile pages
     */
    Epoch               durable_epoch_;     // +4 -> 12

    /** ID of the array storage. immutable after the array storage is created. */
    StorageId           storage_id_;        // +4 -> 16

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
    char        data_[PAGE_SIZE - sizeof(PageHeader)];

    // No instantiation.
    Page() CXX11_FUNC_DELETE;
    Page(const Page& other) CXX11_FUNC_DELETE;
    Page& operator=(const Page& other) CXX11_FUNC_DELETE;
};

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_PAGE_HPP_
