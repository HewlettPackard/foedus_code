/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_PAGE_ID_HPP_
#define FOEDUS_STORAGE_PAGE_ID_HPP_
#include <stdint.h>
#include <iosfwd>
/**
 * @file foedus/storage/page_id.hpp
 * @brief Definitions of Page ID and a few related constant values.
 * @ingroup STORAGE
 */
namespace foedus {
namespace storage {

/**
 * A constant defining the page size (in bytes) of both snapshot pages and volatile pages.
 * @ingroup STORAGE
 */
const uint16_t PAGE_SIZE = 1 << 12;

/**
 * @brief Represents a pointer to another page (usually a child page).
 * @ingroup STORAGE
 * @details
 * @par Duality of Page Pointer
 * bluh bluh
 *
 * @par POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct DualPagePointer {
    DualPagePointer();

    friend std::ostream& operator<<(std::ostream& o, const DualPagePointer& v);

    uint64_t    snapshot_page_id_;
    uint64_t    volatile_page_id_;
};
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_PAGE_ID_HPP_
