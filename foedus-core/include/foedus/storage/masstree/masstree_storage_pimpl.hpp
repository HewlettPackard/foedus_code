/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_PIMPL_HPP_
#include <stdint.h>

#include <string>
#include <vector>

#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/**
 * @brief Pimpl object of MasstreeStorage.
 * @ingroup MASSTREE
 * @details
 * A private pimpl object for MasstreeStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class MasstreeStoragePimpl final : public DefaultInitializable {
 public:
  MasstreeStoragePimpl() = delete;
  MasstreeStoragePimpl(Engine* engine,
                      MasstreeStorage* holder,
                      const MasstreeMetadata &metadata,
                      bool create);

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ErrorStack  create(thread::Thread* context);

  Engine* const           engine_;
  MasstreeStorage* const  holder_;
  MasstreeMetadata        metadata_;

  /** If this is true, initialize() reads it back from previous snapshot and logs. */
  bool                      exist_;

  /** @copydoc foedus::storage::masstree::MasstreeStorage::get_record() */
  ErrorCode get_record(
    thread::Thread* context,
    const char* key,
    uint16_t key_length,
    void* payload,
    uint16_t* payload_capacity);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::get_record_part() */
  ErrorCode get_record_part(
    thread::Thread* context,
    const char* key,
    uint16_t key_length,
    void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::get_record_primitive() */
  template <typename PAYLOAD>
  ErrorCode get_record_primitive(
    thread::Thread* context,
    const char* key,
    uint16_t key_length,
    PAYLOAD* payload,
    uint16_t payload_offset);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::get_record_normalized() */
  ErrorCode get_record_normalized(
    thread::Thread* context,
    NormalizedPrimitiveKey key,
    void* payload,
    uint16_t* payload_capacity);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::get_record_part_normalized() */
  ErrorCode get_record_part_normalized(
    thread::Thread* context,
    NormalizedPrimitiveKey key,
    void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /**
   * @copydoc foedus::storage::masstree::MasstreeStorage::get_record_primitive_normalized()
   */
  template <typename PAYLOAD>
  ErrorCode get_record_primitive_normalized(
    thread::Thread* context,
    NormalizedPrimitiveKey key,
    PAYLOAD* payload,
    uint16_t payload_offset);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::insert_record() */
  ErrorCode insert_record(
    thread::Thread* context,
    const char* key,
    uint16_t key_length,
    const void* payload,
    uint16_t payload_count);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::insert_record_normalized() */
  ErrorCode insert_record_normalized(
    thread::Thread* context,
    NormalizedPrimitiveKey key,
    const void* payload,
    uint16_t payload_count);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::delete_record() */
  ErrorCode delete_record(thread::Thread* context, const char* key, uint16_t key_length);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::delete_record_normalized() */
  ErrorCode delete_record_normalized(thread::Thread* context, NormalizedPrimitiveKey key);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::overwrite_record() */
  ErrorCode overwrite_record(
    thread::Thread* context,
    const char* key,
    uint16_t key_length,
    const void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::overwrite_record_primitive() */
  template <typename PAYLOAD>
  ErrorCode overwrite_record_primitive(
    thread::Thread* context,
    const char* key,
    uint16_t key_length,
    PAYLOAD payload,
    uint16_t payload_offset);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::overwrite_record_normalized() */
  ErrorCode overwrite_record_normalized(
    thread::Thread* context,
    NormalizedPrimitiveKey key,
    const void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /**
   * @copydoc foedus::storage::masstree::MasstreeStorage::overwrite_record_primitive_normalized()
   */
  template <typename PAYLOAD>
  ErrorCode overwrite_record_primitive_normalized(
    thread::Thread* context,
    NormalizedPrimitiveKey key,
    PAYLOAD payload,
    uint16_t payload_offset);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::increment_record() */
  template <typename PAYLOAD>
  ErrorCode increment_record(
    thread::Thread* context,
    const char* key,
    uint16_t key_length,
    PAYLOAD* value,
    uint16_t payload_offset);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::increment_record_normalized() */
  template <typename PAYLOAD>
  ErrorCode increment_record_normalized(
    thread::Thread* context,
    NormalizedPrimitiveKey key,
    PAYLOAD* value,
    uint16_t payload_offset);
};
}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_PIMPL_HPP_
