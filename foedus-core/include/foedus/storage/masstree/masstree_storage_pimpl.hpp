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
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/thread/thread.hpp"

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
   * Root page of the first layer. Volatile pointer is always active.
   * This might be MasstreeIntermediatePage or MasstreeBoundaryPage.
   * When the first layer B-tree grows, this points to a new page. So, this is one of the few
   * page pointers that might be \e swapped. Transactions thus have to add this to a pointer
   * set even thought they are following a volatile pointer.
   *
   * Instead, this always points to a root. We don't need "is_root" check in [YANDONG12] and
   * thus doesn't need a parent pointer.
   */
  DualPagePointer         first_root_pointer_;

  /** If this is true, initialize() reads it back from previous snapshot and logs. */
  bool                    exist_;

  ErrorCode get_first_root(thread::Thread* context, MasstreePage** root, PageVersion* version);
  ErrorCode grow_root(
    thread::Thread* context,
    DualPagePointer* root_pointer,
    MasstreePage* root);

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
    PageVersion* border_version) ALWAYS_INLINE;
  /** descend subroutine of find_border() */
  ErrorCode find_border_descend(
    thread::Thread* context,
    MasstreeIntermediatePage* cur,
    PageVersion cur_stable,
    uint8_t   current_layer,
    bool      for_writes,
    KeySlice  slice,
    MasstreeBorderPage** out,
    PageVersion* out_version);
  /** similar to descend, but only for border page's foster child chain. */
  ErrorCode find_border_leaf(
    MasstreeBorderPage* cur,
    PageVersion cur_stable,
    uint8_t   current_layer,
    KeySlice  slice,
    MasstreeBorderPage** out,
    PageVersion* out_version) ALWAYS_INLINE;

  /** Identifies page and record for the key */
  ErrorCode locate_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    bool for_writes,
    MasstreeBorderPage** out_page,
    uint8_t* record_index);
  /** Identifies page and record for the normalized key */
  ErrorCode locate_record_normalized(
    thread::Thread* context,
    KeySlice key,
    bool for_writes,
    MasstreeBorderPage** out_page,
    uint8_t* record_index);

  ErrorCode reserve_record(
    thread::Thread* context,
    const void* key,
    uint16_t key_length,
    uint16_t payload_count,
    MasstreeBorderPage** out_page,
    uint8_t* record_index);
  ErrorCode reserve_record_normalized(
    thread::Thread* context,
    KeySlice key,
    uint16_t payload_count,
    MasstreeBorderPage** out_page,
    uint8_t* record_index);
  ErrorCode reserve_record_new_record(
    thread::Thread* context,
    MasstreeBorderPage* border,
    KeySlice key,
    uint8_t remaining,
    const void* suffix,
    uint16_t payload_count,
    MasstreeBorderPage** out_page,
    uint8_t* record_index);
  void      reserve_record_new_record_apply(
    thread::Thread* context,
    MasstreeBorderPage* target,
    uint8_t target_index,
    KeySlice slice,
    uint8_t remaining_key_length,
    const void* suffix,
    uint16_t payload_count);

  /** implementation of get_record family. use with locate_record() */
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

  /** implementation of insert_record family. use with \b reserve_record() */
  ErrorCode insert_general(
    thread::Thread* context,
    MasstreeBorderPage* border,
    uint8_t index,
    const void* be_key,
    uint16_t key_length,
    const void* payload,
    uint16_t payload_count);

  /** implementation of delete_record family. use with locate_record()  */
  ErrorCode delete_general(
    thread::Thread* context,
    MasstreeBorderPage* border,
    uint8_t index,
    const void* be_key,
    uint16_t key_length);

  /** implementation of overwrite_record family. use with locate_record()  */
  ErrorCode overwrite_general(
    thread::Thread* context,
    MasstreeBorderPage* border,
    uint8_t index,
    const void* be_key,
    uint16_t key_length,
    const void* payload,
    uint16_t payload_offset,
    uint16_t payload_count);

  /** implementation of increment_record family. use with locate_record()  */
  template <typename PAYLOAD>
  ErrorCode increment_general(
    thread::Thread* context,
    MasstreeBorderPage* border,
    uint8_t index,
    const void* be_key,
    uint16_t key_length,
    PAYLOAD* value,
    uint16_t payload_offset);

  ErrorStack verify_single_thread(thread::Thread* context);
  ErrorStack verify_single_thread_layer(
    thread::Thread* context,
    uint8_t layer,
    MasstreePage* layer_root);
  ErrorStack verify_single_thread_intermediate(
    thread::Thread* context,
    KeySlice low_fence,
    HighFence high_fence,
    MasstreeIntermediatePage* page);
  ErrorStack verify_single_thread_border(
    thread::Thread* context,
    KeySlice low_fence,
    HighFence high_fence,
    MasstreeBorderPage* page);


  /** Thread::follow_page_pointer() for masstree */
  ErrorCode follow_page(
    thread::Thread* context,
    bool for_writes,
    bool root_in_layer,
    storage::DualPagePointer* pointer,
    MasstreePage** page);
  /** Follows to next layer's root page. */
  ErrorCode follow_layer(
    thread::Thread* context,
    bool for_writes,
    MasstreeBorderPage* parent,
    uint8_t record_index,
    MasstreePage** page) ALWAYS_INLINE;

  /** Reserve a next layer as one system transaction. */
  ErrorCode create_next_layer(
    thread::Thread* context,
    MasstreeBorderPage* parent,
    uint8_t parent_index);
};
}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_PIMPL_HPP_
