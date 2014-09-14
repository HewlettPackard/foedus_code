/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
#include <stdint.h>

#include <string>
#include <vector>

#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/assorted/const_div.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_id.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_route.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/fwd.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace array {
/** Shared data of this storage type */
struct ArrayStorageControlBlock final {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  ArrayStorageControlBlock() = delete;
  ~ArrayStorageControlBlock() = delete;

  /** Status of the storage */
  StorageStatus       status_;
  /** Points to the root page (or something equivalent). */
  DualPagePointer     root_page_pointer_;
  /** metadata of this storage. */
  FixedArrayMetadata  meta_;

  // Do NOT reorder members up to here. The layout must be compatible with StorageControlBlock
  // Type-specific shared members below.

  LookupRouteFinder   route_finder_;
};

/**
 * @brief Pimpl object of ArrayStorage.
 * @ingroup ARRAY
 * @details
 * A private pimpl object for ArrayStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ArrayStoragePimpl final : public DefaultInitializable {
 public:
  enum Constants {
    /** If you want more than this, you should loop. ArrayStorage should take care of it. */
    kBatchMax = 16,
  };

  ArrayStoragePimpl() = delete;
  ArrayStoragePimpl(Engine* engine, ArrayStorage* holder, const ArrayMetadata &metadata,
            bool create);
  ~ArrayStoragePimpl() {}

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ErrorStack  create(thread::Thread* context);
  void        report_page_distribution();

  ErrorStack  verify_single_thread(thread::Thread* context, ArrayPage* page);

  /** defined in array_storage_prefetch.cpp */
  ErrorCode   prefetch_pages(thread::Thread* context, ArrayOffset from, ArrayOffset to);
  ErrorCode   prefetch_pages_recurse(
    thread::Thread* context,
    ArrayOffset from,
    ArrayOffset to,
    ArrayPage* page);

  // all per-record APIs are called so frequently, so returns ErrorCode rather than ErrorStack
  ErrorCode   locate_record_for_read(
    thread::Thread* context,
    ArrayOffset offset,
    Record** out,
    bool* snapshot_record) ALWAYS_INLINE;

  ErrorCode   locate_record_for_write(
    thread::Thread* context,
    ArrayOffset offset,
    Record** out) ALWAYS_INLINE;

  ErrorCode   get_record(
    thread::Thread* context,
    ArrayOffset offset,
    void *payload,
    uint16_t payload_offset,
    uint16_t payload_count) ALWAYS_INLINE;

  template <typename T>
  ErrorCode   get_record_primitive(
    thread::Thread* context,
    ArrayOffset offset,
    T *payload,
    uint16_t payload_offset);

  ErrorCode   get_record_payload(
    thread::Thread* context,
    ArrayOffset offset,
    const void **payload) ALWAYS_INLINE;
  ErrorCode   get_record_for_write(
    thread::Thread* context,
    ArrayOffset offset,
    Record** record) ALWAYS_INLINE;

  template <typename T>
  ErrorCode get_record_primitive_batch(
    thread::Thread* context,
    uint16_t payload_offset,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    T* payload_batch) ALWAYS_INLINE;
  ErrorCode get_record_payload_batch(
    thread::Thread* context,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    const void** payload_batch) ALWAYS_INLINE;
  ErrorCode get_record_for_write_batch(
    thread::Thread* context,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    Record** record_batch) ALWAYS_INLINE;

  ErrorCode   overwrite_record(thread::Thread* context, ArrayOffset offset,
      const void *payload, uint16_t payload_offset, uint16_t payload_count) ALWAYS_INLINE;
  template <typename T>
  ErrorCode   overwrite_record_primitive(thread::Thread* context, ArrayOffset offset,
            T payload, uint16_t payload_offset);

  ErrorCode   overwrite_record(
    thread::Thread* context,
    ArrayOffset offset,
    Record* record,
    const void *payload,
    uint16_t payload_offset,
    uint16_t payload_count) ALWAYS_INLINE;

  template <typename T>
  ErrorCode   overwrite_record_primitive(
    thread::Thread* context,
    ArrayOffset offset,
    Record* record,
    T payload,
    uint16_t payload_offset) ALWAYS_INLINE;

  template <typename T>
  ErrorCode   increment_record(thread::Thread* context, ArrayOffset offset,
            T* value, uint16_t payload_offset);
  template <typename T>
  ErrorCode  increment_record_oneshot(
    thread::Thread* context,
    ArrayOffset offset,
    T value,
    uint16_t payload_offset);

  ErrorCode   lookup_for_read(
    thread::Thread* context,
    ArrayOffset offset,
    ArrayPage** out,
    uint16_t* index,
    bool* snapshot_page) ALWAYS_INLINE;

  /**
   * This version always returns a volatile page, installing a new one if needed.
   */
  ErrorCode   lookup_for_write(
    thread::Thread* context,
    ArrayOffset offset,
    ArrayPage** out,
    uint16_t* index) ALWAYS_INLINE;

  ErrorCode locate_record_for_read_batch(
    thread::Thread* context,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    Record** out_batch,
    bool* snapshot_page_batch) ALWAYS_INLINE;
  ErrorCode lookup_for_read_batch(
    thread::Thread* context,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    ArrayPage** out_batch,
    uint16_t* index_batch,
    bool* snapshot_page_batch) ALWAYS_INLINE;
  ErrorCode lookup_for_write_batch(
    thread::Thread* context,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    Record** record_batch) ALWAYS_INLINE;

  /** Used only from uninitialize() */
  void        release_pages_recursive(
    memory::PageReleaseBatch* batch, ArrayPage* page, VolatilePagePointer volatile_page_id);

  /**
  * Calculate leaf/interior pages we need.
  * @return index=level.
  */
  static std::vector<uint64_t> calculate_required_pages(uint64_t array_size, uint16_t payload);
  /**
   * The offset interval a single page represents in each level. index=level.
   * So, offset_intervals[0] is the number of records in a leaf page.
   */
  static std::vector<uint64_t> calculate_offset_intervals(uint8_t levels, uint16_t payload);

  static ErrorCode follow_pointer_for_read(
    thread::Thread* context,
    xct::Xct* current_xct,
    const memory::GlobalVolatilePageResolver& page_resolver,
    DualPagePointer* pointer,
    bool* followed_snapshot_pointer,
    ArrayPage** out) ALWAYS_INLINE;
  static ErrorCode follow_pointer_for_write(
    thread::Thread* context,
    const memory::GlobalVolatilePageResolver& page_resolver,
    DualPagePointer* pointer,
    ArrayPage** out) ALWAYS_INLINE;

  Engine* const           engine_;
  ArrayStorage* const     holder_;
  ArrayMetadata           metadata_;

  /**
   * Points to the root page.
   */
  DualPagePointer         root_page_pointer_;

  /**
   * Root page is assured to be never evicted.
   * So, we can access the root_page_ without going through caching module.
   */
  ArrayPage*              root_page_;

  bool                    exist_;

  /** Number of levels. */
  const uint8_t           levels_;

  LookupRouteFinder       route_finder_;
};
static_assert(sizeof(ArrayStoragePimpl) <= kPageSize, "ArrayStoragePimpl is too large");
static_assert(
  sizeof(ArrayStorageControlBlock) <= soc::GlobalMemoryAnchors::kStorageMemorySize,
  "ArrayStorageControlBlock is too large.");

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_

