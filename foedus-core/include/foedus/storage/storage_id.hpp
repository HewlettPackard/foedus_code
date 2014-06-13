/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_ID_HPP_
#define FOEDUS_STORAGE_STORAGE_ID_HPP_
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/assorted/raw_atomics.hpp>
#include <foedus/memory/memory_id.hpp>
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
const uint16_t kPageSize = 1 << 12;

/**
 * @brief Unique ID for storage.
 * @ingroup STORAGE
 * @details
 * StorageId is an unsigned integer starting from 1. Value 0 is an always invalid ID (empty).
 * Name of storage is also unique, but it's an auxiliary information for convenience.
 * The primary way to identify storages is this StorageId.
 */
typedef uint32_t StorageId;

/**
 * @brief Page ID of a snapshot page.
 * @ingroup STORAGE
 * @details
 * Snapshot Page ID is a 48-bit integer.
 * The high 16 bits indicate which snapshot it is in (foedus::snapshot::SnapshotId).
 * As we periodically merge all snapshots, we won't have 2^16 snapshots at one time.
 * The mid 8 bits indicate the NUMA node ID, or which file it is in.
 * We have one snapshot file per NUMA node, so it won't be more than 2^8.
 * The last 40 bits indicate page offset in the file. So, one file in one snapshot must be within
 * 4kb * 2^40 = 4PB, which is surely the case.
 */
typedef uint64_t SnapshotPagePointer;

/**
 * @brief Type of the storage, such as hash.
 * @ingroup STORAGE
 */
enum StorageType {
    /** 0 indicates invalid type. */
    kInvalidStorage = 0,
    /** \ref ARRAY */
    kArrayStorage,
    /** \ref HASH */
    kHashStorage,
    /** \ref MASSTREE */
    kMasstreeStorage,
    /** \ref SEQUENTIAL */
    kSequentialStorage,
};

/**
 * @brief Checksum of a snapshot page.
 * @ingroup STORAGE
 * @details
 * Each snapshot page contains this checksum to be verified when we read it from
 * media or occasionally.
 */
typedef uint64_t Checksum;

/**
 * @brief Represents a pointer to a volatile page with modification count for preventing ABA.
 * @ingroup STORAGE
 * @details
 * The high 32 bit is a set of flags while the low 32 bit is the offset.
 * The high 32 bit consits of the following information:
 *  \li 8-bit NUMA node ID (foedus::thread::ThreadGroupId)
 *  \li 8-bit flags for concurrency control
 *  \li 16-bit modification counter
 * The offset has to be at least 32 bit (4kb * 2^32=16TB per NUMA node).
 */
union VolatilePagePointer {
    uint64_t        word;

    struct Components {
        uint8_t                 numa_node;
        uint8_t                 flags;
        uint16_t                mod_count;
        memory::PagePoolOffset  offset;
    } components;
};


/**
 * @brief Represents a pointer to another page (usually a child page).
 * @ingroup STORAGE
 * @details
 * @section DUALITY Duality of Page Pointer
 * \b All page pointers in our storages have duality; a pointer to \e volatile page
 * and a pointer to \e snapshot page. The volatile pointer always points to the
 * latest in-memory image of the page. The snapshot page points to
 * a potentially-stale snapshot image of the page which is guaranteed to be read only in
 * all regards, even in its descendent pages.
 *
 * None, either, or both of the two pointers might null.
 *  \li Both are null; the page doesn't exist yet.
 *  \li Only snapshot pointer is null; it's a newly created page. no snapshot taken yet.
 *  \li Only volatile pointer is null; the snapshot page is the latest and we don't have a
 * modification on the page since then (or not published by RCU-ing thread yet).
 *
 * @section CAS Atomic Compare-And-Exchange
 * Dual page pointer is, unfortunately, 128 bits.
 * When we have to atomically swap either or both 64-bit parts depending on the current
 * value of the entire 128 bit, we need a double-word CAS.
 * The current implementation uses a processor-specific assembly, assuming recent processors.
 * This might restrict our portability, so let's minimize the use of this operation and revisit
 * the design. Do we really need atomic swap for this object?
 *
 * @section POD POD
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct DualPagePointer {
    DualPagePointer() : snapshot_pointer_(0) {
        volatile_pointer_.word = 0;
    }

    bool operator==(const DualPagePointer& other) const {
        return snapshot_pointer_ == other.snapshot_pointer_
            && volatile_pointer_.word == other.volatile_pointer_.word;
    }
    bool operator!=(const DualPagePointer& other) const {
        return !operator==(other);
    }

    friend std::ostream& operator<<(std::ostream& o, const DualPagePointer& v);

    /** 128-bit atomic CAS (strong version) for the dual pointer. */
    bool atomic_compare_exchange_strong(
        const DualPagePointer &expected, const DualPagePointer &desired) {
        return assorted::raw_atomic_compare_exchange_strong_uint128(
            reinterpret_cast<uint64_t*>(this),
            reinterpret_cast<const uint64_t*>(&expected),
            reinterpret_cast<const uint64_t*>(&desired));
    }
    /** 128-bit atomic CAS (weak version) for the dual pointer. */
    bool atomic_compare_exchange_weak(
        const DualPagePointer &expected, const DualPagePointer &desired) {
        return assorted::raw_atomic_compare_exchange_weak_uint128(
            reinterpret_cast<uint64_t*>(this),
            reinterpret_cast<const uint64_t*>(&expected),
            reinterpret_cast<const uint64_t*>(&desired));
    }

    SnapshotPagePointer snapshot_pointer_;
    VolatilePagePointer volatile_pointer_;
};
STATIC_SIZE_CHECK(sizeof(DualPagePointer), sizeof(uint64_t) * 2)

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_ID_HPP_
