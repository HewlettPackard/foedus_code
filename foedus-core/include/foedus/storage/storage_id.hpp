/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_ID_HPP_
#define FOEDUS_STORAGE_STORAGE_ID_HPP_
#include <stdint.h>
#include <iosfwd>
/**
 * @file foedus/storage/storage_id.hpp
 * @brief Definitions of IDs in this package and a few related constant values.
 * @ingroup STORAGE
 */
namespace foedus {
namespace storage {

/**
 * @brief A constant defining the page size (in bytes) of both snapshot pages and volatile pages.
 * @ingroup STORAGE
 * @details
 * This number must be at least 4kb (2^12) because that's Linux's page alignment.
 */
const uint16_t PAGE_SIZE = 1 << 12;

/**
 * @brief bluh
 * @ingroup STORAGE
 * @details
 * bluh
 */
typedef uint32_t StorageId;

/**
 * @brief bluh
 * @ingroup STORAGE
 * @details
 * bluh
 */
typedef uint32_t ModCount;

/**
 * @brief bluh
 * @ingroup STORAGE
 * @details
 * bluh
 */
typedef uint64_t Checksum;

/**
 * @brief bluh
 * @ingroup STORAGE
 * @details
 * bluh
 */
union VolatilePagePointer {
    uint64_t        word;

    struct components {
        ModCount    mod_count;
        uint32_t    offset;
    };
};


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
    DualPagePointer() : snapshot_page_id_(0) {
        volatile_pointer_.word = 0;
    }

    friend std::ostream& operator<<(std::ostream& o, const DualPagePointer& v);

    uint64_t            snapshot_page_id_;
    VolatilePagePointer volatile_pointer_;
};

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_ID_HPP_
