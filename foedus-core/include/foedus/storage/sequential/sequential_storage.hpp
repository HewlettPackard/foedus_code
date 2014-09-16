/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_HPP_

#include <iosfwd>
#include <string>

#include "foedus/attachable.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace sequential {
/**
 * @brief Represents an append/scan-only store.
 * @ingroup SEQUENTIAL
 */
class SequentialStorage CXX11_FINAL
  : public virtual Storage, public Attachable<SequentialStorageControlBlock> {
 public:
  SequentialStorage() : Attachable<SequentialStorageControlBlock>() {}
  /**
   * Constructs an sequential storage either from disk or newly create.
   */
  SequentialStorage(Engine* engine, SequentialStorageControlBlock* control_block)
    : Attachable<SequentialStorageControlBlock>(engine, control_block) {
      ASSERT_ND(get_type() == kSequentialStorage || !exists());
    }
  SequentialStorage(Engine* engine, StorageControlBlock* control_block)
    : Attachable<SequentialStorageControlBlock>(
      engine,
      reinterpret_cast<SequentialStorageControlBlock*>(control_block)) {
      ASSERT_ND(get_type() == kSequentialStorage || !exists());
  }

  // Storage interface
  StorageId           get_id()    const CXX11_OVERRIDE;
  StorageType         get_type()  const CXX11_OVERRIDE;
  const StorageName&  get_name()  const CXX11_OVERRIDE;
  const Metadata*     get_metadata()  const CXX11_OVERRIDE;
  const SequentialMetadata*  get_sequential_metadata()  const;
  bool                exists()    const CXX11_OVERRIDE;
  ErrorStack          create() CXX11_OVERRIDE;
  ErrorStack          drop() CXX11_OVERRIDE;

  // this storage type doesn't use moved bit
  bool track_moved_record(xct::WriteXctAccess* /*write*/) CXX11_OVERRIDE {
    ASSERT_ND(false);
    return false;
  }
  xct::LockableXctId* track_moved_record(xct::LockableXctId* /*address*/) CXX11_OVERRIDE {
    ASSERT_ND(false);
    return CXX11_NULLPTR;
  }

  /**
   * @brief Append one record to this sequential storage.
   * @param[in] context Thread context
   * @param[in] payload We copy from this buffer. Must be at least get_payload_size().
   * @param[in] payload_count Length of payload.
   * @pre payload_count > 0
   * @pre payload_count < kMaxPayload
   * @details
   * The strict ordering of the appended record is NOT guaranteed to be the commit serialization
   * order. \ref SEQUENTIAL storage essentially provides a set semantics, not a strictly
   * ordered LIFO queue, to be more scalable. However, the orders are at least loosely
   * ordered; the order largely represents when it was inserted.
   */
  ErrorCode  append_record(thread::Thread* context, const void *payload, uint16_t payload_count);

  /**
   * Used to apply the effect of appending to volatile list.
   */
  void       apply_append_record(thread::Thread* context, const SequentialAppendLogType* log_entry);

  // TODO(Hideaki) Scan-access methods

  void       describe(std::ostream* o) const CXX11_OVERRIDE;
};
}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_HPP_
