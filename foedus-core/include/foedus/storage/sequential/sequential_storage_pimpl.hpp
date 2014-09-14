/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_PIMPL_HPP_
#include <stdint.h>

#include <string>
#include <vector>

#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_volatile_list_impl.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace sequential {
/** Shared data of this storage type */
struct SequentialStorageControlBlock final {
  enum Constants {
    kPointerPageCount = 1 << 6,
  };
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  SequentialStorageControlBlock() = delete;
  ~SequentialStorageControlBlock() = delete;

  /** Status of the storage */
  StorageStatus       status_;
  /** Points to the root page (or something equivalent). */
  DualPagePointer     root_page_pointer_;
  /** metadata of this storage. */
  FixedSequentialMetadata  meta_;

  // Do NOT reorder members up to here. The layout must be compatible with StorageControlBlock
  // Type-specific shared members below.

  /**
   * Points to pages that store thread-private head pages to store thread-private volatile pages.
   * Each page can contain 2^10 pointers (as the node is implicit, PagePoolOffset suffices)
   * and we can have at most 2^16 cores. Thus we have 2^6 pointers here.
   * This means we can waste 64*2=128 volatile pages (=512kb) per one sequential storage..
   * shouldn't be a big issue.
   */
  VolatilePagePointer   head_pointers_page_[kPointerPageCount];
  /** Same above, but for tail pointers. */
  VolatilePagePointer   tail_pointers_page_[kPointerPageCount];
};

/**
 * @brief Pimpl object of SequentialStorage.
 * @ingroup SEQUENTIAL
 * @details
 * A private pimpl object for SequentialStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class SequentialStoragePimpl final : public DefaultInitializable {
 public:
  SequentialStoragePimpl() = delete;
  SequentialStoragePimpl(Engine* engine,
                          SequentialStorage* holder,
                          const SequentialMetadata &metadata,
                          bool create);
  ~SequentialStoragePimpl() {}

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ErrorStack  create(thread::Thread* context);

  ErrorCode   append_record(thread::Thread* context, const void *payload, uint16_t payload_count);

  void        apply_append_record(
    thread::Thread* context,
    const SequentialAppendLogType* log_entry);

  Engine* const             engine_;
  SequentialStorage* const  holder_;
  SequentialMetadata        metadata_;

  /**
   * @brief A separate lock-free in-memory list of volatile records.
   * @details
   * This separate list maintains records in the sequential storage until they are
   * snapshotted. When the records are snapshotted, the snapshot thread scans this list
   * and drops snapshotted records (of course atomically with installing the snapshot versions).
   */
  SequentialVolatileList    volatile_list_;

  /** If this is true, initialize() reads it back from previous snapshot and logs. */
  bool                      exist_;
};
static_assert(sizeof(SequentialStoragePimpl) <= kPageSize, "SequentialStoragePimpl is too large");
static_assert(
  sizeof(SequentialStorageControlBlock) <= soc::GlobalMemoryAnchors::kStorageMemorySize,
  "SequentialStorageControlBlock is too large.");
}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_PIMPL_HPP_
