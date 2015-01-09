/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
#include <stdint.h>

#include <string>
#include <vector>

#include "foedus/attachable.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/const_div.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_id.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/storage/array/array_route.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/fwd.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace array {
/** Shared data of this storage type */
struct ArrayStorageControlBlock final {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  ArrayStorageControlBlock() = delete;
  ~ArrayStorageControlBlock() = delete;

  bool exists() const { return status_ == kExists || status_ == kMarkedForDeath; }

  soc::SharedMutex    status_mutex_;
  /** Status of the storage */
  StorageStatus       status_;
  /** Points to the root page (or something equivalent). */
  DualPagePointer     root_page_pointer_;
  /** metadata of this storage. */
  ArrayMetadata       meta_;

  // Do NOT reorder members up to here. The layout must be compatible with StorageControlBlock
  // Type-specific shared members below.

  /** Number of levels. */
  uint8_t             levels_;
  LookupRouteFinder   route_finder_;

  /**
   * intervals_[x] is the range of array offset in one page of level-x.
   * For example, [0] is the number of records in leaf (level-0),
   * [1] = [0] * kInteriorFanout, ....
   * When you use this information, be careful on right-most page.
   * If the page is right-most (eg root page), the end should be the array's size,
   * which might be smaller than the range it can physically contain.
   */
  uint64_t            intervals_[8];
};

/**
 * @brief Pimpl object of ArrayStorage.
 * @ingroup ARRAY
 * @details
 * A private pimpl object for ArrayStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ArrayStoragePimpl final {
 public:
  enum Constants {
    /** If you want more than this, you should loop. ArrayStorage should take care of it. */
    kBatchMax = 16,
  };

  ArrayStoragePimpl() = delete;
  explicit ArrayStoragePimpl(ArrayStorage* storage)
    : engine_(storage->get_engine()), control_block_(storage->get_control_block()) {}
  ArrayStoragePimpl(Engine* engine, ArrayStorageControlBlock* control_block)
    : engine_(engine), control_block_(control_block) {}

  ~ArrayStoragePimpl() {}

  ErrorStack  create(const Metadata& metadata);
  ErrorStack  load(const StorageControlBlock& snapshot_block);
  ErrorStack  load_empty();

  void        report_page_distribution();

  bool        exists() const { return control_block_->exists(); }
  const ArrayMetadata&    get_meta() const { return control_block_->meta_; }
  StorageId   get_id() const { return get_meta().id_; }
  uint16_t    get_levels() const { return control_block_->levels_; }
  uint16_t    get_snapshot_drop_volatile_pages_threshold() const {
    return get_meta().snapshot_drop_volatile_pages_threshold_;
  }
  uint16_t    get_payload_size() const { return get_meta().payload_size_; }
  ArrayOffset get_array_size() const { return get_meta().array_size_; }
  ArrayPage*  get_root_page();
  ErrorStack  verify_single_thread(thread::Thread* context);
  ErrorStack  verify_single_thread(thread::Thread* context, ArrayPage* page);

  /** defined in array_storage_prefetch.cpp */
  ErrorCode   prefetch_pages(
    thread::Thread* context,
    bool install_volatile,
    bool cache_snapshot,
    ArrayOffset from,
    ArrayOffset to);
  ErrorCode   prefetch_pages_recurse(
    thread::Thread* context,
    bool install_volatile,
    bool cache_snapshot,
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

  // the following methods are for batching. no point to do ALWAYS_INLINE
  ErrorCode locate_record_for_read_batch(
    thread::Thread* context,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    Record** out_batch,
    bool* snapshot_page_batch);
  ErrorCode lookup_for_read_batch(
    thread::Thread* context,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    ArrayPage** out_batch,
    uint16_t* index_batch,
    bool* snapshot_page_batch);
  ErrorCode lookup_for_write_batch(
    thread::Thread* context,
    uint16_t batch_size,
    const ArrayOffset* offset_batch,
    Record** record_batch);

  /** Used only from drop() */
  static void release_pages_recursive(
    const memory::GlobalVolatilePageResolver& resolver,
    memory::PageReleaseBatch* batch,
    VolatilePagePointer volatile_page_id);

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

  ErrorCode follow_pointer(
    thread::Thread* context,
    bool in_snapshot,
    bool for_write,
    DualPagePointer* pointer,
    ArrayPage** out,
    const ArrayPage* parent,
    uint16_t index_in_parent) ALWAYS_INLINE;

  Engine* const                   engine_;
  ArrayStorageControlBlock* const control_block_;
};

inline ErrorCode ArrayStoragePimpl::follow_pointer(
  thread::Thread* context,
  bool in_snapshot,
  bool for_write,
  DualPagePointer* pointer,
  ArrayPage** out,
  const ArrayPage* parent,
  uint16_t index_in_parent) {
  ASSERT_ND(!in_snapshot || !for_write);  // if we are modifying, we must be in volatile world
  ASSERT_ND(!parent->is_leaf());
  return context->follow_page_pointer(
    array_volatile_page_init,  // array might have null pointer. in that case create empty new page
    false,  // if both volatile/snapshot null, create a new volatile (logically all-zero)
    for_write,
    !in_snapshot,  // if we are already in snapshot world, no need to take more pointer set
    false,
    pointer,
    reinterpret_cast<Page**>(out),
    reinterpret_cast<const Page*>(parent),
    index_in_parent);
}


static_assert(sizeof(ArrayStoragePimpl) <= kPageSize, "ArrayStoragePimpl is too large");
static_assert(
  sizeof(ArrayStorageControlBlock) <= soc::GlobalMemoryAnchors::kStorageMemorySize,
  "ArrayStorageControlBlock is too large.");

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_

