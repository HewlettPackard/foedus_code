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
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_HPP_

#include <iosfwd>
#include <string>

#include "foedus/attachable.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {
/**
 * @brief Represents a Masstree storage.
 * @ingroup MASSTREE
 */
class MasstreeStorage CXX11_FINAL : public Storage<MasstreeStorageControlBlock> {
 public:
  typedef MasstreeStoragePimpl   ThisPimpl;
  typedef MasstreeCreateLogType  ThisCreateLogType;
  typedef MasstreeMetadata       ThisMetadata;

  MasstreeStorage();
  MasstreeStorage(Engine* engine, MasstreeStorageControlBlock* control_block);
  MasstreeStorage(Engine* engine, StorageControlBlock* control_block);
  MasstreeStorage(Engine* engine, StorageId id);
  MasstreeStorage(Engine* engine, const StorageName& name);
  MasstreeStorage(const MasstreeStorage& other);
  MasstreeStorage& operator=(const MasstreeStorage& other);

  // Storage interface
  const MasstreeMetadata*  get_masstree_metadata()  const;
  ErrorStack          create(const Metadata &metadata);
  ErrorStack          load(const StorageControlBlock& snapshot_block);
  ErrorStack          drop();
  friend std::ostream& operator<<(std::ostream& o, const MasstreeStorage& v);


  /**
   * @brief Prefetch data pages in this storage. Key Slice version (from/to are 8 bytes or less).
   * @param[in] context Thread context.
   * @param[in] install_volatile Whether to install/prefetch volatile pages based on the recent
   * snapshot page if there is none.
   * @param[in] cache_snapshot Whether to cache/prefetch snapshot pages if exists.
   * @param[in] from inclusive begin slice of records that are specifically prefetched even in
   * data pages.
   * @param[in] to inclusive end slice of records that are specifically prefetched even in data
   * pages.
   * @details
   * This is to \e warmup the storage for the current core.
   * Data pages are prefetched within from/to.
   * So far prefetches only volatile pages, but it will also cache and prefetch snapshot pages.
   */
  ErrorCode prefetch_pages_normalized(
    thread::Thread* context,
    bool install_volatile,
    bool cache_snapshot,
    KeySlice from = kInfimumSlice,
    KeySlice to = kSupremumSlice);

  // TASK(Hideaki) implement non key-slice version of prefetch_pages. so far this is enough, tho.

  /**
   * @copydoc foedus::storage::StorageManager::track_moved_record()
   * @note Implementation note and limitation
   * Master-tree (masstree) does use moved bit, so this storage implements this method to handle
   * moved record. There are a few design choices worth noting.
   *
   * In the \e new_mpage git branch, we significantly changed the page layout in this package.
   * One purpose was to allow tracking the moved record in \b all circumstances.
   *
   * When the record is moved within the same B-trie layer, it is anyway easy to track down the
   * new location. When the record is moved to next layer, however, we have to know the entire key
   * to track the record. Previous page layout didn't allow it unless we have the
   * write_set. When there is a standalone read-access, we could not track to next layer.
   *
   * The new page layout always leaves the original key of the record in page, thus the issue
   * completely went away. This also simplified concurrency control in this package.
   * See @ref MASS_TERM_REMAINDER.
   *
   * However, this came with the price of wasted space in page.
   * To ameliorate it, we made a few improvements to reduce wasted space.
   *
   * \li First, now we have a flag that says the record is \e initially a next-layer.
   * When this is set, we do not put suffix, thus the price is gone.
   * \li Second, we also added a configuration to aggressively create next-layer to make most
   * high-layer records as initially next-layer. See MasstreeMetadata::min_layer_hint_.
   * \li Third, we also overhauled the way we expand records in page.
   *
   * Thanks to these improvements, we now allow \e more records, not less, in a page.
   * Previously it was 64, but now kBorderPageMaxSlots is 100 for 4kb pages.
   */
  xct::TrackMovedRecordResult track_moved_record(
    xct::LockableXctId* old_address,
    xct::WriteXctAccess* write_set);

  //// Masstree API

  // get_record() methods

  /**
   * @brief Retrieves an entire record of the given key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key that is lexicographically (big-endian) evaluated.
   * @param[in] key_length Byte size of key.
   * @param[out] payload Buffer to receive the payload of the record.
   * @param[in,out] payload_capacity [In] Byte size of the payload buffer, [Out] length of
   * the payload. This is set whether the payload capacity was too small or not.
   * @details
   * When payload_capacity is smaller than the actual payload, this method returns
   * kErrorCodeStrTooSmallPayloadBuffer and payload_capacity is set to be the required length.
   *
   * When the key is not found (kErrorCodeStrKeyNotFound), we also add an appropriate
   * record to \e range-lock read set because it is part of a transactional information.
   */
  ErrorCode   get_record(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    void* payload,
    PayloadLength* payload_capacity);

  /**
   * @brief Retrieves a part of the given key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key that is lexicographically (big-endian) evaluated.
   * @param[in] key_length Byte size of key.
   * @param[out] payload Buffer to receive the payload of the record.
   * @param[in] payload_offset We copy from this byte position of the record.
   * @param[in] payload_count How many bytes we copy.
   * @pre payload_offset + payload_count must be within the record's actual payload size
   * (returns kErrorCodeStrTooShortPayload if not)
   */
  ErrorCode   get_record_part(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    void* payload,
    PayloadLength payload_offset,
    PayloadLength payload_count);

  /**
   * @brief Retrieves a part of the given key in this Masstree as a primitive value.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key that is lexicographically (big-endian) evaluated.
   * @param[in] key_length Byte size of key.
   * @param[out] payload Receive the payload of the record.
   * @param[in] payload_offset We copy from this byte position of the record.
   * @pre payload_offset + sizeof(PAYLOAD) must be within the record's actual payload size
   * (returns kErrorCodeStrTooShortPayload if not)
   * @tparam PAYLOAD primitive type of the payload. all integers and floats are allowed.
   */
  template <typename PAYLOAD>
  ErrorCode   get_record_primitive(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    PAYLOAD* payload,
    PayloadLength payload_offset);

  /**
   * @brief Retrieves an entire record of the given primitive key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Primitive key that is evaluated in the primitive type's comparison rule.
   * @param[out] payload Buffer to receive the payload of the record.
   * @param[in,out] payload_capacity [In] Byte size of the payload buffer, [Out] length of
   * the payload. This is set whether the payload capacity was too small or not.
   */
  ErrorCode   get_record_normalized(
    thread::Thread* context,
    KeySlice key,
    void* payload,
    PayloadLength* payload_capacity);

  /**
   * @brief Retrieves a part of the given primitive key in this Masstree.
   * @see get_record_part()
   * @see get_record_normalized()
   */
  ErrorCode   get_record_part_normalized(
    thread::Thread* context,
    KeySlice key,
    void* payload,
    PayloadLength payload_offset,
    PayloadLength payload_count);

  /**
   * @brief Retrieves a part of the given primitive key in this Masstree as a primitive value.
   * @see get_record_normalized()
   * @see get_record_primitive()
   */
  template <typename PAYLOAD>
  ErrorCode   get_record_primitive_normalized(
    thread::Thread* context,
    KeySlice key,
    PAYLOAD* payload,
    PayloadLength payload_offset);

  // insert_record() methods

  /**
   * @brief Inserts a new record of the given key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key that is lexicographically (big-endian) evaluated.
   * @param[in] key_length Byte size of key.
   * @param[in] payload Value to insert.
   * @param[in] payload_count Length of payload.
   * @details
   * If the key already exists, it returns kErrorCodeStrKeyAlreadyExists and also adds the
   * found record to read set because it is part of transactional information.
   */
  inline ErrorCode insert_record(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    const void* payload,
    PayloadLength payload_count) ALWAYS_INLINE {
    return insert_record(context, key, key_length, payload, payload_count, payload_count);
  }

  /**
   * With this method, you can also specify \e physical_payload_hint, the
   * physical size of the record's payload part. If you will later expand
   * the payload of this record, giving a larger value will avoid record migration.
   * Default value (and minimal value) is same as the initial payload_count.
   */
  ErrorCode   insert_record(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    const void* payload,
    PayloadLength payload_count,
    PayloadLength physical_payload_hint);

  /**
   * @brief Inserts a new record without payload of the given key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key that is lexicographically (big-endian) evaluated.
   * @param[in] key_length Byte size of key.
   * @details
   * If the key already exists, it returns kErrorCodeStrKeyAlreadyExists and also adds the
   * found record to read set because it is part of transactional information.
   */
  ErrorCode   insert_record(
    thread::Thread* context,
    const void* key,
    KeyLength key_length) ALWAYS_INLINE {
      return insert_record(context, key, key_length, CXX11_NULLPTR, 0U);
  }

  /**
   * @brief Inserts a new record of the given primitive key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Primitive key that is evaluated in the primitive type's comparison rule.
   * @param[in] payload Value to insert.
   * @param[in] payload_count Length of payload.
   */
  inline ErrorCode insert_record_normalized(
    thread::Thread* context,
    KeySlice key,
    const void* payload,
    PayloadLength payload_count) ALWAYS_INLINE {
    return insert_record_normalized(context, key, payload, payload_count, payload_count);
  }

  /**
   * With this method, you can also specify \e physical_payload_hint.
   */
  ErrorCode   insert_record_normalized(
    thread::Thread* context,
    KeySlice key,
    const void* payload,
    PayloadLength payload_count,
    PayloadLength physical_payload_hint);

  /**
   * @brief Inserts a new record without payload of the given primitive key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Primitive key that is evaluated in the primitive type's comparison rule.
   */
  ErrorCode   insert_record_normalized(thread::Thread* context, KeySlice key) ALWAYS_INLINE {
    return insert_record_normalized(context, key, CXX11_NULLPTR, 0U);
  }

  // delete_record() methods

  /**
   * @brief Deletes a record of the given key from this Masstree.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key that is lexicographically (big-endian) evaluated.
   * @param[in] key_length Byte size of key.
   * @details
   * When the key does not exist, it returns kErrorCodeStrKeyNotFound and also adds an appropriate
   * record to \e range-lock read set because it is part of transactional information.
   */
  ErrorCode   delete_record(thread::Thread* context, const void* key, KeyLength key_length);

  /**
   * @brief Deletes a record of the given primitive key from this Masstree.
   * @param[in] context Thread context
   * @param[in] key Primitive key that is evaluated in the primitive type's comparison rule.
   */
  ErrorCode   delete_record_normalized(thread::Thread* context, KeySlice key);

  // upsert_record() methods

  /**
   * @brief Inserts a new record of the given key or replaces the existing one,
   * or so-called \e upsert.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key that is lexicographically (big-endian) evaluated.
   * @param[in] key_length Byte size of key.
   * @param[in] payload Value to upsert.
   * @param[in] payload_count Length of payload.
   * @details
   * This method puts the record whether the key already exists or not, which is handy
   * in many usecases. Internally, the Implementation comes with a bit of complexity.
   * If there is an existing record of the key (whether logically deleted or not)
   * that is spacious enough, then we simply replace the record.
   * If the key doesn't exist or the existing record is no big enough, we create/migrate
   * the record in a system transaction then install the record.
   */
  inline ErrorCode upsert_record(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    const void* payload,
    PayloadLength payload_count) ALWAYS_INLINE {
    return upsert_record(context, key, key_length, payload, payload_count, payload_count);
  }

  /**
   * With this method, you can also specify \e physical_payload_hint, the
   * physical size of the record's payload part. If you will later expand
   * the payload of this record, giving a larger value will avoid record migration.
   * Default value (and minimal value) is same as the initial payload_count.
   */
  ErrorCode   upsert_record(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    const void* payload,
    PayloadLength payload_count,
    PayloadLength physical_payload_hint);

  /**
   * @brief Upserts a new record without payload of the given key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key that is lexicographically (big-endian) evaluated.
   * @param[in] key_length Byte size of key.
   */
  ErrorCode   upsert_record(
    thread::Thread* context,
    const void* key,
    KeyLength key_length) ALWAYS_INLINE {
      return upsert_record(context, key, key_length, CXX11_NULLPTR, 0U);
  }

  /**
   * @brief Upserts a new record of the given primitive key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Primitive key that is evaluated in the primitive type's comparison rule.
   * @param[in] payload Value to upsert.
   * @param[in] payload_count Length of payload.
   */
  inline ErrorCode upsert_record_normalized(
    thread::Thread* context,
    KeySlice key,
    const void* payload,
    PayloadLength payload_count) ALWAYS_INLINE {
    return upsert_record_normalized(context, key, payload, payload_count, payload_count);
  }

  /**
   * With this method, you can also specify \e physical_payload_hint.
   */
  ErrorCode   upsert_record_normalized(
    thread::Thread* context,
    KeySlice key,
    const void* payload,
    PayloadLength payload_count,
    PayloadLength physical_payload_hint);

  /**
   * @brief Upserts a new record without payload of the given primitive key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Primitive key that is evaluated in the primitive type's comparison rule.
   */
  ErrorCode   upsert_record_normalized(thread::Thread* context, KeySlice key) ALWAYS_INLINE {
    return upsert_record_normalized(context, key, CXX11_NULLPTR, 0U);
  }

  // overwrite_record() methods

  /**
   * @brief Overwrites a part of one record of the given key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key that is lexicographically (big-endian) evaluated.
   * @param[in] key_length Byte size of key.
   * @param[in] payload We copy from this buffer. Must be at least payload_count.
   * @param[in] payload_offset We overwrite to this byte position of the record.
   * @param[in] payload_count How many bytes we overwrite.
   * @details
   * When payload_offset+payload_count is larger than the actual payload, this method returns
   * kErrorCodeStrTooShortPayload. Just like get_record(), this adds to range-lock read set
   * even when key is not found.
   */
  ErrorCode   overwrite_record(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    const void* payload,
    PayloadLength payload_offset,
    PayloadLength payload_count);

  /**
   * @brief Overwrites a part of one record of the given key in this Masstree as a primitive value.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key that is lexicographically (big-endian) evaluated.
   * @param[in] key_length Byte size of key.
   * @param[in] payload We copy this value.
   * @param[in] payload_offset We overwrite to this byte position of the record.
   * @pre payload_offset + sizeof(PAYLOAD) must be within the record's actual payload size
   * (returns kErrorCodeStrTooShortPayload if not)
   * @tparam PAYLOAD primitive type of the payload. all integers and floats are allowed.
   */
  template <typename PAYLOAD>
  ErrorCode   overwrite_record_primitive(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    PAYLOAD payload,
    PayloadLength payload_offset);

  /**
   * @brief Overwrites a part of one record of the given primitive key in this Masstree.
   * @param[in] context Thread context
   * @param[in] key Primitive key that is evaluated in the primitive type's comparison rule.
   * @param[in] payload We copy from this buffer. Must be at least payload_count.
   * @param[in] payload_offset We overwrite to this byte position of the record.
   * @param[in] payload_count How many bytes we overwrite.
   * @see get_record_normalized()
   */
  ErrorCode   overwrite_record_normalized(
    thread::Thread* context,
    KeySlice key,
    const void* payload,
    PayloadLength payload_offset,
    PayloadLength payload_count);

  /**
   * @brief Overwrites a part of one record of the given primitive key
   * in this Masstree as a primitive value.
   * @see overwrite_record_primitive()
   * @see overwrite_record_normalized()
   */
  template <typename PAYLOAD>
  ErrorCode   overwrite_record_primitive_normalized(
    thread::Thread* context,
    KeySlice key,
    PAYLOAD payload,
    PayloadLength payload_offset);


  // increment_record() methods

  /**
   * @brief This one further optimizes overwrite methods for the frequent use
   * case of incrementing some data in primitive type.
   * @param[in] context Thread context
   * @param[in] key Arbitrary length of key that is lexicographically (big-endian) evaluated.
   * @param[in] key_length Byte size of key.
   * @param[in,out] value (in) addendum, (out) value after addition.
   * @param[in] payload_offset We overwrite to this byte position of the record.
   * @pre payload_offset + sizeof(PAYLOAD) must be within the record's actual payload size
   * (returns kErrorCodeStrTooShortPayload if not)
   * @tparam PAYLOAD primitive type of the payload. all integers and floats are allowed.
   */
  template <typename PAYLOAD>
  ErrorCode   increment_record(
    thread::Thread* context,
    const void* key,
    KeyLength key_length,
    PAYLOAD* value,
    PayloadLength payload_offset);

  /**
   * @brief For primitive key.
   * @see increment_record()
   */
  template <typename PAYLOAD>
  ErrorCode   increment_record_normalized(
    thread::Thread* context,
    KeySlice key,
    PAYLOAD* value,
    PayloadLength payload_offset);

  // TODO(Hideaki): Extend/shrink/update methods for payload. A bit faster than delete + insert.

  ErrorStack  verify_single_thread(thread::Thread* context);

  /**
   * @brief A super-expensive and single-thread only debugging feature to write out
   * gigantic human-readable texts to describe the Masstree in details.
   * @details
   * Do not invoke this method for more than 100 pages, or in production use.
   * This is for debugging.
   */
  ErrorStack  debugout_single_thread(
    Engine* engine,
    bool volatile_only = false,
    uint32_t max_pages = 1024U);

  /** Arguments for peek_volatile_page_boundaries() */
  struct PeekBoundariesArguments {
    /** [IN] slices of higher layers that lead to the B-trie layer of interest. null if 1st layer */
    const KeySlice* prefix_slices_;
    /** [IN] size of prefix_slices_. 0 means we are interested in the first layer. */
    uint32_t prefix_slice_count_;
    /** [IN] capacity of found_boundaries_. */
    uint32_t found_boundary_capacity_;
    /** [IN] lists up page boundaries from this slice */
    KeySlice from_;
    /** [IN] lists up page boundaries up to this slice */
    KeySlice to_;
    /** [OUT] fence-slices of border pages between from-to */
    KeySlice* found_boundaries_;
    /** [OUT] number of found_boundaries_ entries returned */
    uint32_t* found_boundary_count_;
  };

  /**
   * @brief Checks the volatile pages of this storage to give hints to decide page boundary keys.
   * @details
   * This method is used from the composer to make page boundaries in snapshot pages better aligned
   * with those of volatile pages so that we can drop more volatile pages at the end.
   * This method is opportunistic, meaning the result is not guaranteed to be transactionally
   * correct, but that's fine because these are just hints. If the page boundary is not aligned
   * well with volatile pages, we just have to keep more volatile pages for a while.
   * In fact, the composer might ignore these hints in some cases (eg. a page has too few tuples).
   * Defined in masstree_storage_peek.cpp
   */
  ErrorCode   peek_volatile_page_boundaries(Engine* engine, const PeekBoundariesArguments& args);

  /**
   * @brief Deliberately causes splits under the volatile root of first layer, or "fatify" it.
   * @param[in] context Thread context
   * @param[in] desired_count Does nothing if the root of first layer already has this
   * many children.
   * @details
   * This is a special-purpose, physical-only method that does nothing logically.
   * It increases the number of direct children in first root, which is useful when we partition
   * based on children in first root. Fatifying beforehand shouldn't have any significant drawback,
   * but we so far restrict the use of this method to performance benchmarks.
   * @todo testcase for this method. TDD? shut up.
   */
  ErrorStack  fatify_first_root(thread::Thread* context, uint32_t desired_count);

  /**
   * @param[in] layer B-trie layer most border pages would be in.
   * @param[in] key_length estimated byte size of each key
   * @param[in] payload_length estimated byte size of each payload
   * @returns Estimated number of records that fit in one border page.
   */
  static SlotIndex estimate_records_per_page(
    Layer layer,
    KeyLength key_length,
    PayloadLength payload_length);
};
}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_HPP_
