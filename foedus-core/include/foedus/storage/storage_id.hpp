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
#ifndef FOEDUS_STORAGE_STORAGE_ID_HPP_
#define FOEDUS_STORAGE_STORAGE_ID_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/fixed_string.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/thread/thread_id.hpp"

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

/** Represents a unique name of a storage. Upto 60 characters so far. */
typedef assorted::FixedString<60> StorageName;
STATIC_SIZE_CHECK(sizeof(StorageName), 64)

/**
 * @brief As partition=NUMA node, this is just a synonym of foedus::thread::ThreadGroupId.
 * @ingroup STORAGE
 */
typedef thread::ThreadGroupId PartitionId;

/**
 * @brief Page ID of a snapshot page.
 * @ingroup STORAGE
 * @details
 * Snapshot Page ID is a 64-bit integer.
 * The high 16 bits indicate which snapshot it is in (foedus::snapshot::SnapshotId).
 * As we periodically merge all snapshots, we won't have 2^16 snapshots at one time.
 * The mid 8 bits indicate the NUMA node ID, or which file it is in.
 * We have one snapshot file per NUMA node, so it won't be more than 2^8.
 * The last 40 bits indicate page offset in the file. So, one file in one snapshot must be within
 * 4kb * 2^40 = 4PB, which is surely the case.
 */
typedef uint64_t SnapshotPagePointer;

/**
 * @brief Represents a local page ID in each one snapshot file in some NUMA node.
 * @ingroup STORAGE
 * @details
 * This is the low 40 bits of SnapshotPagePointer. Just an alias for readability.
 * This value must not exceed 2^40.
 * 0 means null pointer (which is a valid value).
 */
typedef uint64_t SnapshotLocalPageId;

inline SnapshotLocalPageId extract_local_page_id_from_snapshot_pointer(
  SnapshotPagePointer pointer) {
  return pointer & 0x000000FFFFFFFFFFULL;
}
inline uint8_t extract_numa_node_from_snapshot_pointer(SnapshotPagePointer pointer) {
  return static_cast<uint8_t>(pointer >> 40);
}
inline uint16_t extract_snapshot_id_from_snapshot_pointer(SnapshotPagePointer pointer) {
  return static_cast<uint16_t>(pointer >> 48);
}

inline void assert_valid_snapshot_local_page_id(SnapshotLocalPageId page_id) {
  ASSERT_ND(page_id < (1ULL << 40));
}

inline SnapshotPagePointer to_snapshot_page_pointer(
  uint16_t snapshot_id,
  uint8_t node,
  SnapshotLocalPageId local_page_id) {
  assert_valid_snapshot_local_page_id(local_page_id);
  return static_cast<uint64_t>(snapshot_id) << 48
      | static_cast<uint64_t>(node) << 40
      | local_page_id;
}

void describe_snapshot_pointer(std::ostream* o, SnapshotPagePointer pointer);

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
 * @brief Gives a string representation of StorageType.
 * @ingroup STORAGE
 */
inline const char* to_storage_type_name(StorageType type) {
  switch (type) {
  case kInvalidStorage: return "Invalid";
  case kArrayStorage: return "Array";
  case kHashStorage: return "Hash";
  case kMasstreeStorage: return "Masstree";
  case kSequentialStorage: return "Sequential";
  default: return "Unknown";
  }
}

/**
 * @brief Status of a storage.
 * @ingroup STORAGE
 */
enum StorageStatus {
  /** Initial state, which means the storage has not been created yet. */
  kNotExists = 0,
  /** The storage has been created and ready for use. */
  kExists,
  /**
   * The storage has been marked for drop and can't be used.
   * The status becomes kNotExists at next restart.
   * the system. So far there is no path from this state to kNotExists. No reuse of StorageId.
   */
  kMarkedForDeath,
};

/**
 * @brief Checksum of a snapshot page.
 * @ingroup STORAGE
 * @details
 * Each snapshot page contains this checksum to be verified when we read it from
 * media or occasionally.
 */
typedef uint32_t Checksum;

const uint8_t kVolatilePointerFlagSwappable = 0x02;

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
 * @todo This might become just a typedef of uint64_t rather than union.
 * union data type is a bit unfriendly to some standard classes.
 */
union VolatilePagePointer {
  uint64_t        word;

  struct Components {
    uint8_t                 numa_node;
    uint8_t                 flags;
    uint16_t                mod_count;
    memory::PagePoolOffset  offset;

    /**
     * Whether this volatile page pointer might be dynamically changed except:
     *  \li null -> valid pointer (installing a new volatile version, this happens anyways)
     *  \li valid pointer -> null (drop after snapshot thread, which stops the world before it)
     *
     * In other words, this flag says whether valid pointer -> another valid pointer can happen.
     * Such a swap can happen only in root page pointers of \ref MASSTREE so far.
     * Pointers to the root pages of any layer have this flag on.
     */
    bool is_swappable() const { return flags & kVolatilePointerFlagSwappable; }
  } components;

  bool is_null() const { return components.offset == 0; }
  void clear() { word = 0; }
  void set(uint8_t numa_node, uint8_t flags, uint16_t mod_count, memory::PagePoolOffset offset) {
    components.numa_node = numa_node;
    components.flags = flags;
    components.mod_count = mod_count;
    components.offset = offset;
  }
  bool is_equivalent(const VolatilePagePointer& other) {
    return components.numa_node == other.components.numa_node
      && components.offset == other.components.offset;
  }
};
void describe_volatile_pointer(std::ostream* o, VolatilePagePointer pointer);

inline VolatilePagePointer construct_volatile_page_pointer(uint64_t word) {
  VolatilePagePointer pointer;
  pointer.word = word;
  return pointer;
}
inline VolatilePagePointer combine_volatile_page_pointer(
  uint8_t numa_node,
  uint8_t flags,
  uint16_t mod_count,
  memory::PagePoolOffset offset) {
  VolatilePagePointer ret;
  ret.components.numa_node = numa_node;
  ret.components.flags = flags;
  ret.components.mod_count = mod_count;
  ret.components.offset = offset;
  return ret;
}

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

  bool is_both_null() const {
    return snapshot_pointer_ == 0 && volatile_pointer_.components.offset == 0;
  }

  SnapshotPagePointer snapshot_pointer_;
  VolatilePagePointer volatile_pointer_;
};
STATIC_SIZE_CHECK(sizeof(DualPagePointer), sizeof(uint64_t) * 2)

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_ID_HPP_
