/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"

#include <glog/logging.h>

#include <string>

#include "foedus/engine.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_inl.hpp"

namespace foedus {
namespace storage {
namespace masstree {

// Defines MasstreeStorage methods so that we can inline implementation calls
bool        MasstreeStorage::is_initialized()   const  { return pimpl_->is_initialized(); }
bool        MasstreeStorage::exists()           const  { return pimpl_->exist_; }
StorageId   MasstreeStorage::get_id()           const  { return pimpl_->metadata_.id_; }
const std::string& MasstreeStorage::get_name()  const  { return pimpl_->metadata_.name_; }
const Metadata* MasstreeStorage::get_metadata() const  { return &pimpl_->metadata_; }
const MasstreeMetadata* MasstreeStorage::get_masstree_metadata() const  {
  return &pimpl_->metadata_;
}

MasstreeStoragePimpl::MasstreeStoragePimpl(
  Engine* engine,
  MasstreeStorage* holder,
  const MasstreeMetadata &metadata,
  bool create)
  :
    engine_(engine),
    holder_(holder),
    metadata_(metadata),
    first_root_(nullptr),
    exist_(!create) {
  ASSERT_ND(create || metadata.id_ > 0);
  ASSERT_ND(metadata.name_.size() > 0);
  first_root_pointer_.snapshot_pointer_ = 0;
  first_root_pointer_.volatile_pointer_.word = 0;
}

ErrorStack MasstreeStoragePimpl::initialize_once() {
  LOG(INFO) << "Initializing an masstree-storage " << *holder_ << " exists=" << exist_;
  first_root_ = nullptr;
  first_root_pointer_.snapshot_pointer_ = 0;
  first_root_pointer_.volatile_pointer_.word = 0;

  if (exist_) {
    // TODO(Hideaki): initialize head_root_page_id_
  }
  return kRetOk;
}

ErrorStack MasstreeStoragePimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing an masstree-storage " << *holder_;
  // TODO(Hideaki): release volatile pages
  if (first_root_) {
    memory::PageReleaseBatch release_batch(engine_);
    release_batch.release(first_root_pointer_.volatile_pointer_);
    release_batch.release_all();
    first_root_ = nullptr;
    first_root_pointer_.volatile_pointer_.word = 0;
  }
  return kRetOk;
}

ErrorStack MasstreeStoragePimpl::create(thread::Thread* context) {
  if (exist_) {
    LOG(ERROR) << "This masstree-storage already exists: " << *holder_;
    return ERROR_STACK(kErrorCodeStrAlreadyExists);
  }

  LOG(INFO) << "Newly created an masstree-storage " << *holder_;
  memory::NumaCoreMemory* memory = context->get_thread_memory();
  const memory::LocalPageResolver &local_resolver = context->get_local_volatile_page_resolver();

  // just allocate an empty root page for the first layer
  memory::PagePoolOffset root_offset = memory->grab_free_volatile_page();
  ASSERT_ND(root_offset);
  first_root_ = reinterpret_cast<MasstreePage*>(local_resolver.resolve_offset(root_offset));
  MasstreeBorderPage* root_page = reinterpret_cast<MasstreeBorderPage*>(first_root_);
  first_root_pointer_.snapshot_pointer_ = 0;
  first_root_pointer_.volatile_pointer_ = combine_volatile_page_pointer(
    context->get_numa_node(),
    0,
    0,
    root_offset);
  root_page->initialize_volatile_page(
    metadata_.id_,
    first_root_pointer_.volatile_pointer_,
    0,
    nullptr);

  exist_ = true;
  engine_->get_storage_manager().get_pimpl()->register_storage(holder_);
  return kRetOk;
}

inline ErrorCode MasstreeStoragePimpl::find_border(
  thread::Thread* context,
  MasstreePage* layer_root,
  uint8_t   current_layer,
  bool      for_writes,
  KeySlice  slice,
  MasstreeBorderPage** border,
  MasstreePageVersion* border_version) {
  while (true) {  // for retry
    ASSERT_ND(layer_root->get_layer() == current_layer);
    layer_root->prefetch_general();
    MasstreePageVersion stable(layer_root->get_stable_version());
    while (!stable.is_root()) {
      layer_root = layer_root->get_in_layer_parent();
      layer_root->prefetch_general();
      stable = layer_root->get_stable_version();
    }

    if (stable.is_border()) {
      *border = reinterpret_cast<MasstreeBorderPage*>(layer_root);
      *border_version = stable;
      return kErrorCodeOk;
    } else {
      MasstreeIntermediatePage* cur = reinterpret_cast<MasstreeIntermediatePage*>(layer_root);
      ErrorCode error_code = find_border_descend(
        context,
        cur,
        stable,
        current_layer,
        for_writes,
        slice,
        border);
      if (error_code == kErrorCodeStrMasstreeRetry) {
        DVLOG(0) << "Masstree retry find_border";
        continue;
      } else {
        *border_version = stable;
        return error_code;
      }
    }
  }
}

inline ErrorCode MasstreeStoragePimpl::find_border_descend(
  thread::Thread* context,
  MasstreeIntermediatePage* cur,
  MasstreePageVersion cur_stable,
  uint8_t   current_layer,
  bool      for_writes,
  KeySlice  slice,
  MasstreeBorderPage** out) {
  ASSERT_ND(cur->get_layer() == current_layer);
  while (true) {  // retry loop
    uint8_t minipage_index = cur->find_minipage(cur_stable, slice);
    MasstreeIntermediatePage::MiniPage& minipage = cur->get_minipage(minipage_index);

    minipage.prefetch();
    MasstreePageVersion mini_stable(minipage.get_stable_version());
    uint8_t pointer_index = minipage.find_pointer(mini_stable, slice);
    DualPagePointer& pointer = minipage.pointers_[pointer_index];

    MasstreePage* next;
    CHECK_ERROR_CODE(context->follow_page_pointer(
      &kDummyPageInitializer,  // masstree doesn't create a new page except splits.
      false,  // so, there is no null page possible
      for_writes,  // always get volatile pages for writes
      true,
      false,
      &pointer,
      reinterpret_cast<Page**>(&next)));

    next->prefetch_general();
    MasstreePageVersion next_stable(next->get_stable_version());

    // check cur's version again for hand-over-hand verification
    assorted::memory_fence_acquire();
    uint64_t diff = (cur->get_version().data_ ^ cur_stable.data_);
    uint64_t diff_mini = (minipage.mini_version_.data_ ^ mini_stable.data_);
    if (diff <= kPageVersionLockedBit && diff_mini <= kPageVersionLockedBit) {
      // this means nothing important has changed.
      if (next_stable.is_border()) {
        *out = reinterpret_cast<MasstreeBorderPage*>(next);
        return kErrorCodeOk;
      } else {
        return find_border_descend(
          context,
          reinterpret_cast<MasstreeIntermediatePage*>(next),
          next_stable,
          current_layer,
          for_writes,
          slice,
          out);
      }
    } else {
      DVLOG(0) << "find_border encountered a changed version. retry";
      MasstreePageVersion cur_new_stable(cur->get_stable_version());
      if (cur_new_stable.get_split_counter() != cur_stable.get_split_counter()) {
        // we have to retry from root in this case
        return kErrorCodeStrMasstreeRetry;
      }
      // otherwise retry locally
      cur_stable = cur_new_stable;
      continue;
    }
  }
}

ErrorCode MasstreeStoragePimpl::locate_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  bool for_writes,
  MasstreeBorderPage** out_page,
  uint8_t* record_index) {
  ASSERT_ND(key_length <= kMaxKeyLength);
  MasstreePage* layer_root = first_root_;
  for (uint16_t current_layer = 0;; ++current_layer) {
    uint8_t remaining_length = key_length - current_layer * 8;
    KeySlice slice;
    if (remaining_length >= 8) {
      slice = normalize_be_bytes_full(reinterpret_cast<const char*>(key) + current_layer * 8);
    } else {
      slice = normalize_be_bytes_fragment(
        reinterpret_cast<const char*>(key) + current_layer * 8,
        remaining_length);
    }
    const void* suffix = reinterpret_cast<const char*>(key) + (current_layer + 1) * 8;
    MasstreeBorderPage* border;
    MasstreePageVersion border_version;
    CHECK_ERROR_CODE(find_border(
      context,
      layer_root,
      0,
      for_writes,
      slice,
      &border,
      &border_version));
    uint8_t index = border->find_key(border_version, slice, suffix, remaining_length);

    if (index == MasstreeBorderPage::kMaxKeys) {
      // this means not found
      // TODO(Hideaki) range lock
      return kErrorCodeStrKeyNotFound;
    }
    const MasstreeBorderPage::Slot& slot = border->get_slot(index);
    if (slot.does_point_to_layer()) {
      DualPagePointer& pointer = border->layer_record(slot.offset_);
      MasstreePage* next_root;
      CHECK_ERROR_CODE(context->follow_page_pointer(
        &kDummyPageInitializer,
        false,
        for_writes,
        true,
        false,
        &pointer,
        reinterpret_cast<Page**>(&next_root)));
      ASSERT_ND(next_root);
      layer_root = next_root;
      continue;
    } else {
      *out_page = border;
      *record_index = index;
      return kErrorCodeOk;
    }
  }
}

ErrorCode MasstreeStoragePimpl::locate_record_normalized(
  thread::Thread* context,
  KeySlice key,
  bool for_writes,
  MasstreeBorderPage** out_page,
  uint8_t* record_index) {
  MasstreeBorderPage* border;
  MasstreePageVersion border_version;
  CHECK_ERROR_CODE(find_border(context, first_root_, 0, for_writes, key, &border, &border_version));
  uint8_t index = border->find_key(border_version, key, nullptr, 0);

  if (index == MasstreeBorderPage::kMaxKeys) {
    // this means not found
    return kErrorCodeStrKeyNotFound;
  }
  const MasstreeBorderPage::Slot& slot = border->get_slot(index);
  if (slot.does_point_to_layer()) {
    // it is possible that we go on to second layer (the entire 8 bytes are prefix)
    DualPagePointer& pointer = border->layer_record(slot.offset_);
    MasstreePage* next_root;
    CHECK_ERROR_CODE(context->follow_page_pointer(
      &kDummyPageInitializer,
      false,
      for_writes,
      true,
      false,
      &pointer,
      reinterpret_cast<Page**>(&next_root)));
    ASSERT_ND(next_root);
    // in that case, we search for key '0'.
    CHECK_ERROR_CODE(find_border(context, next_root, 1, for_writes, 0, &border, &border_version));
    index = border->find_key(border_version, 0, nullptr, 0);
    if (index == MasstreeBorderPage::kMaxKeys) {
      return kErrorCodeStrKeyNotFound;
    }
  }

  *out_page = border;
  *record_index = index;
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::retrieve_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  void* payload,
  uint16_t* payload_capacity) {
  const MasstreeBorderPage::Slot& slot = border->get_slot(index);
  if (slot.payload_length_ > *payload_capacity) {
    // buffer too small
    DVLOG(0) << "buffer too small??" << slot.payload_length_ << ":" << *payload_capacity;
    *payload_capacity = slot.payload_length_;
    return kErrorCodeStrTooSmallPayloadBuffer;
  }

  Record* record = border->body_record(slot.offset_);
  if (!border->header().snapshot_) {
    CHECK_ERROR_CODE(context->get_current_xct().add_to_read_set(holder_, record));
  }
  *payload_capacity = slot.payload_length_;
  std::memcpy(payload, record->payload_ + slot.remaining_key_length_, slot.payload_length_);
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::retrieve_part_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  const MasstreeBorderPage::Slot& slot = border->get_slot(index);
  if (slot.payload_length_ < payload_offset + payload_count) {
    LOG(WARNING) << "short record";  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

  Record* record = border->body_record(slot.offset_);
  if (!border->header().snapshot_) {
    CHECK_ERROR_CODE(context->get_current_xct().add_to_read_set(holder_, record));
  }
  std::memcpy(
    payload,
    record->payload_ + slot.remaining_key_length_ + payload_offset,
    payload_count);
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::insert_record(
  thread::Thread* /* context */,
  const void* /* key */,
  uint16_t /* key_length */,
  const void* /* payload */,
  uint16_t /* payload_count */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}


ErrorCode MasstreeStoragePimpl::insert_record_normalized(
  thread::Thread* /* context */,
  KeySlice /* key */,
  const void* /* payload */,
  uint16_t /* payload_count */) {
  return kErrorCodeOk;  // TODO(Hideaki) Implement
}

ErrorCode MasstreeStoragePimpl::delete_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  const void* be_key,
  uint16_t key_length) {
  const MasstreeBorderPage::Slot& slot = border->get_slot(index);
  Record* record = border->body_record(slot.offset_);
  ASSERT_ND(!record->owner_id_.is_deleted());
  uint16_t log_length = MasstreeDeleteLogType::calculate_log_length(key_length);
  MasstreeDeleteLogType* log_entry = reinterpret_cast<MasstreeDeleteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(metadata_.id_, be_key, key_length, border->get_layer());

  return context->get_current_xct().add_to_write_set(holder_, record, log_entry);
}

ErrorCode MasstreeStoragePimpl::overwrite_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  const void* be_key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  const MasstreeBorderPage::Slot& slot = border->get_slot(index);
  Record* record = border->body_record(slot.offset_);
  ASSERT_ND(!record->owner_id_.is_deleted());
  if (slot.payload_length_ < payload_offset + payload_count) {
    LOG(WARNING) << "short record ";  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

  uint16_t log_length = MasstreeOverwriteLogType::calculate_log_length(key_length, payload_count);
  MasstreeOverwriteLogType* log_entry = reinterpret_cast<MasstreeOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    metadata_.id_,
    be_key,
    key_length,
    payload,
    payload_offset,
    payload_count,
    border->get_layer());

  return context->get_current_xct().add_to_write_set(holder_, record, log_entry);
}

template <typename PAYLOAD>
ErrorCode MasstreeStoragePimpl::increment_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  const void* be_key,
  uint16_t key_length,
  PAYLOAD* value,
  uint16_t payload_offset) {
  const MasstreeBorderPage::Slot& slot = border->get_slot(index);
  Record* record = border->body_record(slot.offset_);
  ASSERT_ND(!record->owner_id_.is_deleted());
  if (slot.payload_length_ < payload_offset + sizeof(PAYLOAD)) {
    LOG(WARNING) << "short record ";  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

  uint8_t layer = border->get_layer();
  uint16_t skipped = calculate_skipped_key_length(key_length, layer);
  char* ptr = record->payload_ + key_length - skipped + payload_offset;
  PAYLOAD old_value = *reinterpret_cast<const PAYLOAD*>(ptr);
  *value += old_value;

  uint16_t log_length = MasstreeOverwriteLogType::calculate_log_length(key_length, sizeof(PAYLOAD));
  MasstreeOverwriteLogType* log_entry = reinterpret_cast<MasstreeOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    metadata_.id_,
    be_key,
    key_length,
    value,
    payload_offset,
    sizeof(PAYLOAD),
    border->get_layer());

  return context->get_current_xct().add_to_write_set(holder_, record, log_entry);
}

// Explicit instantiations for each payload type
// @cond DOXYGEN_IGNORE
#define EXPIN_5(x) template ErrorCode MasstreeStoragePimpl::increment_general< x > \
  (thread::Thread* context, MasstreeBorderPage* border, uint8_t index, const void* be_key, \
  uint16_t key_length, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5);
// @endcond

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
