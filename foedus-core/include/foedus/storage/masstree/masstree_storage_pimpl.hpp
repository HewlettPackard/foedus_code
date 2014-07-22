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
#include "foedus/storage/masstree/masstree_page_version.hpp"
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

  /**
   * A always-existing volatile image of (probably-) root page of the first layer.
   * This might be MasstreeIntermediatePage or MasstreeBoundaryPage.
   * During root expansion, this variable tentatively points to a child of root, but
   * one can/should check that situation by reading the parent pointer as described in [YANDONG12].
   */
  MasstreePage*           first_root_;
  DualPagePointer         first_root_pointer_;

  /** If this is true, initialize() reads it back from previous snapshot and logs. */
  bool                    exist_;


  /** @copydoc foedus::storage::masstree::MasstreeStorage::insert_record() */
  ErrorCode insert_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    const void* payload,
    uint16_t payload_count);

  /** @copydoc foedus::storage::masstree::MasstreeStorage::insert_record_normalized() */
  ErrorCode insert_record_normalized(
    thread::Thread* context,
    KeySlice key,
    const void* payload,
    uint16_t payload_count);

  /**
   * Find a border node in the layer that corresponds to the given key slice.
   */
  ErrorCode find_border(
    thread::Thread* context,
    MasstreePage* layer_root,
    uint8_t   current_layer,
    bool      for_writes,
    KeySlice  slice,
    MasstreeBorderPage** border,
    MasstreePageVersion* border_version) ALWAYS_INLINE;
  ErrorCode find_border_descend(
    thread::Thread* context,
    MasstreeIntermediatePage* cur,
    MasstreePageVersion cur_stable,
    uint8_t   current_layer,
    bool      for_writes,
    KeySlice  slice,
    MasstreeBorderPage** out);
  ErrorCode locate_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    bool for_writes,
    MasstreeBorderPage** out_page,
    uint8_t* record_index);
  ErrorCode locate_record_normalized(
    thread::Thread* context,
    KeySlice key,
    bool for_writes,
    MasstreeBorderPage** out_page,
    uint8_t* record_index);

  ErrorCode retrieve_general(
    thread::Thread* context,
    MasstreeBorderPage* border,
    uint8_t index,
    void* payload,
    uint16_t* payload_capacity);
  ErrorCode retrieve_part_general(
    thread::Thread* context,
    MasstreeBorderPage* border,
    uint8_t index,
    void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  ErrorCode delete_general(
    thread::Thread* context,
    MasstreeBorderPage* border,
    uint8_t index,
    const void* be_key,
    uint16_t key_length);

  ErrorCode overwrite_general(
    thread::Thread* context,
    MasstreeBorderPage* border,
    uint8_t index,
    const void* be_key,
    uint16_t key_length,
    const void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  template <typename PAYLOAD>
  ErrorCode increment_general(
    thread::Thread* context,
    MasstreeBorderPage* border,
    uint8_t index,
    const void* be_key,
    uint16_t key_length,
    PAYLOAD* value,
    uint16_t payload_offset);
};
}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_PIMPL_HPP_
