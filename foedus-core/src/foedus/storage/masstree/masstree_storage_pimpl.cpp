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
#include "foedus/xct/xct_optimistic_read_impl.hpp"

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
  if (first_root_) {
    // release volatile pages
    const memory::GlobalVolatilePageResolver& page_resolver
      = engine_->get_memory_manager().get_global_volatile_page_resolver();
    memory::PageReleaseBatch release_batch(engine_);
    first_root_->release_pages_recursive_common(page_resolver, &release_batch);
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
    CHECK_ERROR_CODE(follow_page(context, for_writes, &pointer, &next));

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
    KeySlice slice = slice_layer(key, key_length, current_layer);
    const void* suffix = reinterpret_cast<const char*>(key) + (current_layer + 1) * 8;
    MasstreeBorderPage* border;
    MasstreePageVersion border_version;
    CHECK_ERROR_CODE(find_border(
      context,
      layer_root,
      current_layer,
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
      CHECK_ERROR_CODE(follow_layer(context, for_writes, border, index, &layer_root));
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
  uint8_t index = border->find_key_normalized(0, border_version.get_key_count(), key);
  if (index == MasstreeBorderPage::kMaxKeys) {
    // this means not found
    // TODO(Hideaki) range lock
    return kErrorCodeStrKeyNotFound;
  }
  // because this is just one slice, we never go to second layer
  ASSERT_ND(!border->get_slot(index).does_point_to_layer());
  *out_page = border;
  *record_index = index;
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::create_next_layer(
  thread::Thread* context,
  MasstreeBorderPage* parent,
  uint8_t parent_index) {
  memory::NumaCoreMemory* memory = context->get_thread_memory();
  memory::PagePoolOffset offset = memory->grab_free_volatile_page();
  if (offset == 0) {
    return kErrorCodeMemoryNoFreePages;
  }

  const memory::LocalPageResolver &resolver = context->get_local_volatile_page_resolver();
  MasstreeBorderPage* root = reinterpret_cast<MasstreeBorderPage*>(resolver.resolve_offset(offset));
  DualPagePointer pointer;
  pointer.snapshot_pointer_ = 0;
  pointer.volatile_pointer_ = combine_volatile_page_pointer(context->get_numa_node(), 0, 0, offset);

  MasstreeBorderPage::Slot& slot = parent->get_slot(parent_index);
  Record* parent_record = parent->body_record(slot.offset_);

  // as an independent system transaction, here we do an optimistic version check.
  parent_record->owner_id_.keylock_unconditional();
  if (slot.does_point_to_layer()) {
    // someone else has also made this to a next layer!
    // our effort was a waste, but anyway the goal was achieved.
    LOG(INFO) << "interesting. a concurrent thread has already made a next layer";
    memory->release_free_volatile_page(offset);
    parent_record->owner_id_.release_keylock();
  } else {
    // initialize the root page by copying the recor
    root->initialize_volatile_page(
      metadata_.id_,
      pointer.volatile_pointer_,
      parent->get_layer() + 1,
      parent);
    root->copy_initial_record(parent, parent_index);

    // point to the new page
    slot.flags_ |= MasstreeBorderPage::kSlotFlagLayer;
    parent->layer_record(slot.offset_)->pointer_ = pointer;

    xct::XctId unlocked_id = parent_record->owner_id_;
    unlocked_id.release_keylock();
    // set one next. we don't have to make the new xct id really in serialization order because
    // this is a system transaction that doesn't change anything logically.
    // this is just to make sure other threads get aware of this change at commit time.
    uint16_t ordinal = unlocked_id.get_ordinal();
    if (ordinal != 0xFFFFU) {
      ++ordinal;
    } else {
      unlocked_id.set_epoch(unlocked_id.get_epoch().one_more());
      ordinal = 0;
    }
    unlocked_id.set_ordinal(ordinal);
    if (unlocked_id.is_deleted()) {
      // if the original record was deleted, we inherited it in the new record too.
      // again, we didn't do anything logically.
      ASSERT_ND(root->body_record(root->get_slot(0).offset_)->owner_id_.is_deleted());
      // as a pointer, now it should be an active pointer.
      unlocked_id.set_notdeleted();
    }
    parent_record->owner_id_ = unlocked_id;  // now unlock and set the new version
  }
  return kErrorCodeOk;
}

inline ErrorCode MasstreeStoragePimpl::follow_page(
  thread::Thread* context,
  bool for_writes,
  storage::DualPagePointer* pointer,
  MasstreePage** page) {
  return context->follow_page_pointer(
    &kDummyPageInitializer,  // masstree doesn't create a new page except splits.
    false,  // so, there is no null page possible
    for_writes,  // always get volatile pages for writes
    true,
    false,
    pointer,
    reinterpret_cast<Page**>(page));
}

inline ErrorCode MasstreeStoragePimpl::follow_layer(
  thread::Thread* context,
  bool for_writes,
  MasstreeBorderPage* parent,
  uint8_t record_index,
  MasstreePage** page) {
  ASSERT_ND(record_index < MasstreeBorderPage::kMaxKeys);
  const MasstreeBorderPage::Slot& slot = parent->get_slot(record_index);
  ASSERT_ND(slot.does_point_to_layer());
  DualPagePointer& pointer = parent->layer_record(slot.offset_)->pointer_;
  MasstreePage* next_root;
  CHECK_ERROR_CODE(follow_page(context, for_writes, &pointer, &next_root));
  ASSERT_ND(next_root);
  *page = next_root;
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::reserve_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  uint16_t payload_count,
  MasstreeBorderPage** out_page,
  uint8_t* record_index) {
  ASSERT_ND(key_length <= kMaxKeyLength);
  MasstreePage* layer_root = first_root_;
  for (uint16_t layer = 0;; ++layer) {
    uint8_t remaining_length = key_length - layer * 8;
    KeySlice slice = slice_layer(key, key_length, layer);
    const void* suffix = reinterpret_cast<const char*>(key) + (layer + 1) * 8;
    MasstreeBorderPage* border;
    MasstreePageVersion stable;
    CHECK_ERROR_CODE(find_border(context, layer_root, layer, true, slice, &border, &stable));
    uint8_t key_count = stable.get_key_count();
    MasstreeBorderPage::FindKeyForReserveResult match = border->find_key_for_reserve(
      0,
      key_count,
      slice,
      suffix,
      remaining_length);

    if (match.match_type_ == MasstreeBorderPage::kExactMatchLayerPointer) {
      ASSERT_ND(match.index_ < MasstreeBorderPage::kMaxKeys);
      CHECK_ERROR_CODE(follow_layer(context, true, border, match.index_, &layer_root));
      continue;
    } else if (match.match_type_ == MasstreeBorderPage::kExactMatchLocalRecord) {
      *out_page = border;
      *record_index = match.index_;
      return kErrorCodeOk;
    }

    // no matching or conflicting keys. so we will create a brand new record.
    // this is a system transaction to just create a deleted record.
    if (match.match_type_ == MasstreeBorderPage::kNotFound) {
      // this is the only case we are NOT sure yet.
      // someone else might be now inserting a conflicting key or the exact key.
      // we thus have to take a lock only in this case.
      border->lock();
      MasstreePageVersion& locked_version = border->get_version();
      uint8_t updated_key_count = locked_version.get_key_count();
      ASSERT_ND(updated_key_count >= key_count);
      if (updated_key_count > key_count) {
        // someone else has inserted a new record. Is it conflicting?
        // search again, but only for newly inserted record(s)
        match = border->find_key_for_reserve(
          key_count,
          updated_key_count,
          slice,
          suffix,
          remaining_length);
        key_count = updated_key_count;
      }

      if (match.match_type_ == MasstreeBorderPage::kNotFound) {
        // okay, surely new record
        uint8_t new_index = key_count;
        if (border->can_accomodate(new_index, key_length, payload_count)) {
          locked_version.set_inserting_and_increment_key_count();
          xct::XctId initial_id;
          // initial ID doesn't matter as it logically doesn't exist yet
          initial_id.set_clean(
            Epoch::kEpochInitialCurrent,  // TODO(Hideaki) this should be something else
            0,
            context->get_thread_id());
          initial_id.set_deleted();
          border->reserve_record_space(
            new_index,
            initial_id,
            slice,
            suffix,
            remaining_length,
            payload_count);
        } else {
          ASSERT_ND(false);  // TODO(Hideaki) split
        }
        border->unlock();
        *out_page = border;
        *record_index = new_index;
        return kErrorCodeOk;
      } else {
        border->unlock();
        // someone has inserted conflicting or exact record. let the following code take care
      }
    }

    if (match.match_type_ == MasstreeBorderPage::kExactMatchLayerPointer) {
      CHECK_ERROR_CODE(follow_layer(context, true, border, match.index_, &layer_root));
      continue;
    } else if (match.match_type_ == MasstreeBorderPage::kExactMatchLocalRecord) {
      *out_page = border;
      *record_index = match.index_;
      return kErrorCodeOk;
    } else {
      ASSERT_ND(match.match_type_ == MasstreeBorderPage::kConflictingLocalRecord);
      ASSERT_ND(match.index_ < MasstreeBorderPage::kMaxKeys);
      // this means now we have to create a next layer.
      // this is also one system transaction.
      CHECK_ERROR_CODE(create_next_layer(context, border, match.index_));
      CHECK_ERROR_CODE(follow_layer(context, true, border, match.index_, &layer_root));
      continue;
    }
  }
}

ErrorCode MasstreeStoragePimpl::reserve_record_normalized(
  thread::Thread* context,
  KeySlice key,
  uint16_t payload_count,
  MasstreeBorderPage** out_page,
  uint8_t* record_index) {
  MasstreeBorderPage* border;
  MasstreePageVersion border_version;
  CHECK_ERROR_CODE(find_border(
    context,
    first_root_,
    0,
    true,
    key,
    &border,
    &border_version));
  // because we never go on to second layer in this case, it's either a full match or not-found
  uint8_t key_count = border_version.get_key_count();
  uint8_t index = border->find_key_normalized(0, key_count, key);

  if (index != MasstreeBorderPage::kMaxKeys) {
    *out_page = border;
    *record_index = index;
    return kErrorCodeOk;
  }

  ASSERT_ND(index == MasstreeBorderPage::kMaxKeys);
  // same flow as reserve_record(), but much simpler
  border->lock();
  MasstreePageVersion& locked_version = border->get_version();
  uint8_t updated_key_count = locked_version.get_key_count();
  ASSERT_ND(updated_key_count >= key_count);
  if (updated_key_count > key_count) {
    index = border->find_key_normalized(key_count, updated_key_count, key);
    key_count = updated_key_count;
  }

  if (index == MasstreeBorderPage::kMaxKeys) {
    // okay, surely new record
    uint8_t new_index = key_count;
    if (border->can_accomodate(new_index, sizeof(KeySlice), payload_count)) {
      locked_version.set_inserting_and_increment_key_count();
      xct::XctId initial_id;
      initial_id.set_clean(
        Epoch::kEpochInitialCurrent,  // TODO(Hideaki) this should be something else
        0,
        context->get_thread_id());
      initial_id.set_deleted();
      border->reserve_record_space(
        new_index,
        initial_id,
        key,
        nullptr,
        sizeof(KeySlice),
        payload_count);
    } else {
      ASSERT_ND(false);  // TODO(Hideaki) split
    }
    border->unlock();
    *out_page = border;
    *record_index = new_index;
  } else {
    border->unlock();
    // someone has inserted the exact record. this is also good.
    *out_page = border;
    *record_index = index;
  }
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::retrieve_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  void* payload,
  uint16_t* payload_capacity) {
  const MasstreeBorderPage::Slot& slot = border->get_slot(index);
  ASSERT_ND(!slot.does_point_to_layer());
  Record* record = border->body_record(slot.offset_);

  CHECK_ERROR_CODE(xct::optimistic_read_protocol(
    &context->get_current_xct(),
    holder_,
    &record->owner_id_,
    border->header().snapshot_,
    [record, payload, payload_capacity, slot](xct::XctId observed){
      if (observed.is_deleted()) {
        // in this case, we don't need a range lock. the physical record is surely there.
        return kErrorCodeStrKeyNotFound;
      } else if (slot.payload_length_ > *payload_capacity) {
        // buffer too small
        DVLOG(0) << "buffer too small??" << slot.payload_length_ << ":" << *payload_capacity;
        *payload_capacity = slot.payload_length_;
        return kErrorCodeStrTooSmallPayloadBuffer;
      }
      *payload_capacity = slot.payload_length_;
      uint16_t suffix_length = slot.get_suffix_length();
      std::memcpy(payload, record->payload_ + suffix_length, slot.payload_length_);
      return kErrorCodeOk;
    }));
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
  ASSERT_ND(!slot.does_point_to_layer());
  Record* record = border->body_record(slot.offset_);
  CHECK_ERROR_CODE(xct::optimistic_read_protocol(
    &context->get_current_xct(),
    holder_,
    &record->owner_id_,
    border->header().snapshot_,
    [record, payload, payload_offset, payload_count, slot](xct::XctId /*observed*/){
      if (record->owner_id_.is_deleted()) {
        // in this case, we don't need a range lock. the physical record is surely there.
        return kErrorCodeStrKeyNotFound;
      } else if (slot.payload_length_ < payload_offset + payload_count) {
        LOG(WARNING) << "short record";  // probably this is a rare error. so warn.
        return kErrorCodeStrTooShortPayload;
      }
      uint16_t suffix_length = slot.get_suffix_length();
      std::memcpy(payload, record->payload_ + suffix_length + payload_offset, payload_count);
      return kErrorCodeOk;
    }));
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::insert_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  const void* be_key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_count) {
  const MasstreeBorderPage::Slot& slot = border->get_slot(index);
  Record* record = border->body_record(slot.offset_);
  ASSERT_ND(record->owner_id_.is_deleted());
  ASSERT_ND(slot.payload_length_ == payload_count);

  uint16_t log_length = MasstreeInsertLogType::calculate_log_length(key_length, payload_count);
  MasstreeInsertLogType* log_entry = reinterpret_cast<MasstreeInsertLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    metadata_.id_,
    be_key,
    key_length,
    payload,
    payload_count,
    border->get_layer());

  return context->get_current_xct().add_to_write_set(
    holder_,
    &record->owner_id_,
    record->payload_,
    log_entry);
}

ErrorCode MasstreeStoragePimpl::delete_general(
  thread::Thread* context,
  MasstreeBorderPage* border,
  uint8_t index,
  const void* be_key,
  uint16_t key_length) {
  const MasstreeBorderPage::Slot& slot = border->get_slot(index);
  // TODO(Hideaki) these methods have to do another optimistic check on whether the slot
  // is now changed to next-layer pointer. only when remaining_length>8, though.
  Record* record = border->body_record(slot.offset_);
  CHECK_ERROR_CODE(xct::optimistic_read_protocol(
    &context->get_current_xct(),
    holder_,
    &record->owner_id_,
    false,
    [](xct::XctId observed){
      if (observed.is_deleted()) {
        // in this case, we don't need a range lock. the physical record is surely there.
        return kErrorCodeStrKeyNotFound;
      } else {
        return kErrorCodeOk;
      }
    }));
  uint16_t log_length = MasstreeDeleteLogType::calculate_log_length(key_length);
  MasstreeDeleteLogType* log_entry = reinterpret_cast<MasstreeDeleteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(metadata_.id_, be_key, key_length, border->get_layer());

  return context->get_current_xct().add_to_write_set(
    holder_,
    &record->owner_id_,
    record->payload_,
    log_entry);
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
  CHECK_ERROR_CODE(xct::optimistic_read_protocol(
    &context->get_current_xct(),
    holder_,
    &record->owner_id_,
    false,
    [record, slot, payload_offset, payload_count](xct::XctId observed){
      if (observed.is_deleted()) {
        // in this case, we don't need a range lock. the physical record is surely there.
        return kErrorCodeStrKeyNotFound;
      } else if (slot.payload_length_ < payload_offset + payload_count) {
        LOG(WARNING) << "short record ";  // probably this is a rare error. so warn.
        return kErrorCodeStrTooShortPayload;
      } else {
        return kErrorCodeOk;
      }
    }));
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

  return context->get_current_xct().add_to_write_set(
    holder_,
    &record->owner_id_,
    record->payload_,
    log_entry);
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

  // NOTE if we directly pass value and increment there, we might do it multiple times!
  // optimistic_read_protocol() retries if there are version mismatch.
  // so it must be idempotent. be careful!
  PAYLOAD tmp;
  PAYLOAD* tmp_address = &tmp;
  CHECK_ERROR_CODE(xct::optimistic_read_protocol(
    &context->get_current_xct(),
    holder_,
    &record->owner_id_,
    false,
    [record, slot, tmp_address, payload_offset](xct::XctId observed){
      if (observed.is_deleted()) {
        // in this case, we don't need a range lock. the physical record is surely there.
        return kErrorCodeStrKeyNotFound;
      } else if (slot.payload_length_ < payload_offset + sizeof(PAYLOAD)) {
        LOG(WARNING) << "short record ";  // probably this is a rare error. so warn.
        return kErrorCodeStrTooShortPayload;
      }

      uint16_t suffix_length = slot.get_suffix_length();
      char* ptr = record->payload_ + suffix_length + payload_offset;
      *tmp_address = *reinterpret_cast<const PAYLOAD*>(ptr);
      return kErrorCodeOk;
    }));
  *value += tmp;

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

  return context->get_current_xct().add_to_write_set(
    holder_,
    &record->owner_id_,
    record->payload_,
    log_entry);
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
