/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/hash/hash_storage_pimpl.hpp"

#include <glog/logging.h>

#include <cstring>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/hash/hash_cuckoo.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace hash {

// Defines HashStorage methods so that we can inline implementation calls
bool        HashStorage::is_initialized()   const  { return pimpl_->is_initialized(); }
bool        HashStorage::exists()           const  { return pimpl_->exist_; }
StorageId   HashStorage::get_id()           const  { return pimpl_->metadata_.id_; }
const std::string& HashStorage::get_name()  const  { return pimpl_->metadata_.name_; }
const Metadata* HashStorage::get_metadata() const  { return &pimpl_->metadata_; }
const HashMetadata* HashStorage::get_hash_metadata() const  { return &pimpl_->metadata_; }

ErrorCode HashStorage::get_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  void* payload,
  uint16_t* payload_capacity) {
  return pimpl_->get_record(context, key, key_length, payload, payload_capacity);
}

ErrorCode HashStorage::get_record_part(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  return pimpl_->get_record_part(context, key, key_length, payload, payload_offset, payload_count);
}

template <typename PAYLOAD>
ErrorCode HashStorage::get_record_primitive(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD* payload,
  uint16_t payload_offset) {
  return pimpl_->get_record_primitive(context, key, key_length, payload, payload_offset);
}

ErrorCode HashStorage::insert_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_count) {
  return pimpl_->insert_record(context, key, key_length, payload, payload_count);
}

ErrorCode HashStorage::delete_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length) {
  return pimpl_->delete_record(context, key, key_length);
}

ErrorCode HashStorage::overwrite_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  return pimpl_->overwrite_record(context, key, key_length, payload, payload_offset, payload_count);
}

template <typename PAYLOAD>
ErrorCode HashStorage::overwrite_record_primitive(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD payload,
  uint16_t payload_offset) {
  return pimpl_->overwrite_record_primitive(
    context,
    key,
    key_length,
    payload,
    payload_offset);
}

template <typename PAYLOAD>
ErrorCode HashStorage::increment_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD* value,
  uint16_t payload_offset) {
  return pimpl_->increment_record(context, key, key_length, value, payload_offset);
}

void HashStorage::apply_delete_record(
  thread::Thread* context,
  const HashDeleteLogType* log_entry,
  xct::LockableXctId* owner_id,
  char* payload) {
  pimpl_->apply_delete_record(context, log_entry, owner_id, payload);
}

void HashStorage::apply_insert_record(
  thread::Thread* context,
  const HashInsertLogType* log_entry,
  xct::LockableXctId* owner_id,
  char* payload) {
  pimpl_->apply_insert_record(context, log_entry, owner_id, payload);
}

HashStoragePimpl::HashStoragePimpl(Engine* engine, HashStorage* holder,
                   const HashMetadata &metadata, bool create)
  : engine_(engine),
  holder_(holder),
  metadata_(metadata),
  root_page_(nullptr),
  exist_(!create) {
  ASSERT_ND(create || metadata.id_ > 0);
  ASSERT_ND(metadata.name_.size() > 0);
  root_page_pointer_.snapshot_pointer_ = metadata.root_snapshot_page_id_;
  root_page_pointer_.volatile_pointer_.word = 0;
}

ErrorStack HashStoragePimpl::initialize_once() {
  LOG(INFO) << "Initializing an hash-storage " << *holder_ << " exists=" << exist_;
  bin_count_ = 1ULL << metadata_.bin_bits_;
  bin_pages_ = assorted::int_div_ceil(bin_count_, kBinsPerPage);
  root_pages_ = assorted::int_div_ceil(bin_pages_, kHashRootPageFanout);
  ASSERT_ND(root_pages_ >= 1);
  LOG(INFO) << "bin_count=" << bin_count_ << ", bin_pages=" << bin_pages_
    << ", root_pages=" << root_pages_;
  if (root_pages_ > kHashRootPageFanout) {
    // we don't assume this case so far. kHashRootPageFanout^2 * 4kb just for bin pages...
    LOG(FATAL) << "more than 2 levels root page in hash?? that's too big!" << *holder_;
  }
  if (exist_) {
    // initialize root_page_
  }
  return kRetOk;
}

void HashStoragePimpl::release_pages_recursive_root(
  memory::PageReleaseBatch* batch,
  HashRootPage* page,
  VolatilePagePointer volatile_page_id) {
  // child is bin page
  const memory::GlobalVolatilePageResolver& page_resolver
    = engine_->get_memory_manager().get_global_volatile_page_resolver();
  for (uint32_t i = 0; i < kHashRootPageFanout; ++i) {
    DualPagePointer &child_pointer = page->pointer(i);
    VolatilePagePointer child_page_id = child_pointer.volatile_pointer_;
    if (child_page_id.components.offset != 0) {
      HashBinPage* child_page = reinterpret_cast<HashBinPage*>(
        page_resolver.resolve_offset(child_page_id));
      release_pages_recursive_bin(batch, child_page, child_page_id);
      child_pointer.volatile_pointer_.word = 0;
    }
  }
  batch->release(volatile_page_id);
}

void HashStoragePimpl::release_pages_recursive_bin(
  memory::PageReleaseBatch* batch,
  HashBinPage* page,
  VolatilePagePointer volatile_page_id) {
  ASSERT_ND(volatile_page_id.components.offset != 0);
  const memory::GlobalVolatilePageResolver& page_resolver
    = engine_->get_memory_manager().get_global_volatile_page_resolver();
  for (uint16_t i = 0; i < kBinsPerPage; ++i) {
    DualPagePointer &child_pointer = page->bin(i).data_pointer_;
    VolatilePagePointer child_page_id = child_pointer.volatile_pointer_;
    if (child_page_id.components.offset != 0) {
      // then recurse
      HashDataPage* child_page = reinterpret_cast<HashDataPage*>(
        page_resolver.resolve_offset(child_page_id));
      release_pages_recursive_data(batch, child_page, child_page_id);
      child_pointer.volatile_pointer_.word = 0;
    }
  }
  batch->release(volatile_page_id);
}
void HashStoragePimpl::release_pages_recursive_data(
  memory::PageReleaseBatch* batch,
  HashDataPage* page,
  VolatilePagePointer volatile_page_id) {
  ASSERT_ND(volatile_page_id.components.offset != 0);
  const memory::GlobalVolatilePageResolver& page_resolver
    = engine_->get_memory_manager().get_global_volatile_page_resolver();

  DualPagePointer &next_pointer = page->next_page();
  VolatilePagePointer next_page_id = next_pointer.volatile_pointer_;
  if (next_page_id.components.offset != 0) {
    // then recurse
    HashDataPage* child_page = reinterpret_cast<HashDataPage*>(
      page_resolver.resolve_offset(next_page_id));
    release_pages_recursive_data(batch, child_page, next_page_id);
    next_pointer.volatile_pointer_.word = 0;
  }
  batch->release(volatile_page_id);
}

ErrorStack HashStoragePimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing an hash-storage " << *holder_;
  if (root_page_) {
    LOG(INFO) << "Releasing all in-memory pages...";
    const memory::GlobalVolatilePageResolver& page_resolver
      = engine_->get_memory_manager().get_global_volatile_page_resolver();
    memory::PageReleaseBatch release_batch(engine_);
    VolatilePagePointer root_id = root_page_pointer_.volatile_pointer_;
    if (root_pages_ == 1) {
      release_pages_recursive_root(&release_batch, root_page_, root_id);
    } else {
      // child is still a root page, which should be always in-memory
      for (uint32_t i = 0; i < kHashRootPageFanout; ++i) {
        DualPagePointer &child_pointer = root_page_->pointer(i);
        VolatilePagePointer child_page_id = child_pointer.volatile_pointer_;
        if (child_page_id.components.offset != 0) {
          HashRootPage* child_page = reinterpret_cast<HashRootPage*>(
            page_resolver.resolve_offset(child_page_id));
          release_pages_recursive_root(&release_batch, child_page, child_page_id);
        }
      }
      release_batch.release(root_id);
    }
    release_batch.release_all();
    root_page_ = nullptr;
    root_page_pointer_.volatile_pointer_.word = 0;
  }
  return kRetOk;
}


ErrorStack HashStoragePimpl::create(thread::Thread* context) {
  if (exist_) {
    LOG(ERROR) << "This hash-storage already exists: " << *holder_;
    return ERROR_STACK(kErrorCodeStrAlreadyExists);
  }

  LOG(INFO) << "Newly creating an hash-storage " << *holder_;

  // small number of root pages. we should at least have that many free pages.
  // so far grab all of them from this context. no round robbin
  memory::NumaCoreMemory* memory = context->get_thread_memory();
  const memory::LocalPageResolver &local_resolver = context->get_local_volatile_page_resolver();

  // root of root
  memory::PagePoolOffset root_offset = memory->grab_free_volatile_page();
  ASSERT_ND(root_offset);
  root_page_ = reinterpret_cast<HashRootPage*>(local_resolver.resolve_offset_newpage(root_offset));
  root_page_pointer_.snapshot_pointer_ = 0;
  root_page_pointer_.volatile_pointer_ = combine_volatile_page_pointer(
    context->get_numa_node(),
    0,
    0,
    root_offset);
  root_page_->initialize_volatile_page(
    metadata_.id_,
    root_page_pointer_.volatile_pointer_,
    nullptr);
  if (root_pages_ > 1) {
    for (uint16_t i = 0; i < root_pages_; ++i) {
      memory::PagePoolOffset offset = memory->grab_free_volatile_page();
      ASSERT_ND(offset);
      HashRootPage* page = reinterpret_cast<HashRootPage*>(
        local_resolver.resolve_offset_newpage(offset));
      VolatilePagePointer pointer = combine_volatile_page_pointer(
        context->get_numa_node(),
        0,
        0,
        offset);
      page->initialize_volatile_page(metadata_.id_, pointer, root_page_);
      root_page_->pointer(i).volatile_pointer_ = pointer;
    }
  }

  LOG(INFO) << "Newly created an hash-storage " << *holder_;
  exist_ = true;
  engine_->get_storage_manager().get_pimpl()->register_storage(holder_);
  return kRetOk;
}

ErrorCode HashStoragePimpl::get_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  void* payload,
  uint16_t* payload_capacity) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, false, &combo));
  DVLOG(3) << "get_hash: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_) {
    // we already added to read set, so the only remaining thing is to read payload
    if (combo.payload_length_ > *payload_capacity) {
      // buffer too small
      DVLOG(0) << "buffer too small??" << combo;
      *payload_capacity = combo.payload_length_;
      return kErrorCodeStrTooSmallPayloadBuffer;
    }
    *payload_capacity = combo.payload_length_;
    std::memcpy(payload, combo.record_->payload_ + key_length, combo.payload_length_);
    return kErrorCodeOk;
  } else {
    // not found
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }
}

template <typename PAYLOAD>
ErrorCode HashStoragePimpl::get_record_primitive(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD* payload,
  uint16_t payload_offset) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, false, &combo));
  DVLOG(3) << "get_hash_primitive: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_) {
    // we already added to read set, so the only remaining thing is to read payload
    if (combo.payload_length_ < payload_offset + sizeof(PAYLOAD)) {
      // payload too small
      LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
      return kErrorCodeStrTooShortPayload;
    }
    std::memcpy(payload, combo.record_->payload_ + key_length + payload_offset, sizeof(PAYLOAD));
    return kErrorCodeOk;
  } else {
    // not found
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }
}

ErrorCode HashStoragePimpl::get_record_part(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, false, &combo));
  DVLOG(3) << "get_hash_part: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_) {
    if (combo.payload_length_ < payload_offset + payload_count) {
      LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
      return kErrorCodeStrTooShortPayload;
    }
    std::memcpy(payload, combo.record_->payload_ + key_length + payload_offset, payload_count);
    return kErrorCodeOk;
  } else {
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }
}



ErrorCode HashStoragePimpl::insert_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_count) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, true, &combo));
  DVLOG(3) << "insert_hash: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_) {
    return kErrorCodeStrKeyAlreadyExists;
  }

  // okay, the key doesn't exist in either bin.
  // which one to install?
  bool bin1;
  if (combo.data_pages_[0] == nullptr) {
    // bin1 page doesnt exist, so empty. perfect for balancing.
    bin1 = true;
  } else if (combo.data_pages_[1] == nullptr) {
    bin1 = false;  // for same reason
  } else {
    // if both exist, compare the record count. add to less full bin for balancing
    bin1 = (combo.data_pages_[0]->get_record_count() <= combo.data_pages_[1]->get_record_count());
  }
  uint8_t choice = bin1 ? 0 : 1;

  // lookup_bin should have already created volatile pages for both cases
  HashBinPage* bin_page = combo.bin_pages_[choice];
  ASSERT_ND(bin_page && !bin_page->header().snapshot_);
  ASSERT_ND(bin_page->header().get_page_type() == kHashBinPageType);
  HashDataPage* data_page = combo.data_pages_[choice];
  ASSERT_ND(data_page && !data_page->header().snapshot_);
  ASSERT_ND(data_page->header().get_page_type() == kHashDataPageType);

  uint16_t log_length = HashInsertLogType::calculate_log_length(key_length, payload_count);
  HashInsertLogType* log_entry = reinterpret_cast<HashInsertLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(metadata_.id_, key, key_length, bin1, combo.tag_, payload, payload_count);
  // NOTE tentative hack! currently "payload address" for hash insert write set points to bin page
  // so that we don't have to calculate it again.
  return context->get_current_xct().add_to_write_set(
    holder_,
    &(data_page->page_owner()),
    reinterpret_cast<char*>(bin_page),
    log_entry);
}

ErrorCode HashStoragePimpl::delete_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, true, &combo));
  DVLOG(3) << "delete_hash: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_ == nullptr) {
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }

  HashBinPage* bin_page = combo.bin_pages_[combo.record_bin1_ ? 0 : 1];
#ifndef NDEBUG
  ASSERT_ND(bin_page && !bin_page->header().snapshot_);
  HashDataPage* data_page = combo.data_pages_[combo.record_bin1_ ? 0 : 1];
  ASSERT_ND(data_page && !data_page->header().snapshot_);
#endif  // NDEBUG

  uint16_t log_length = HashDeleteLogType::calculate_log_length(key_length);
  HashDeleteLogType* log_entry = reinterpret_cast<HashDeleteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(metadata_.id_, key, key_length, combo.record_bin1_, combo.record_slot_);
  // NOTE tentative hack! currently "payload address" for hash insert write set points to bin page
  // so that we don't have to calculate it again.
  return context->get_current_xct().add_to_write_set(
    holder_,
    &combo.record_->owner_id_,
    reinterpret_cast<char*>(bin_page),
    log_entry);
}

ErrorCode HashStoragePimpl::overwrite_record(
  thread::Thread* context ,
  const void* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, true, &combo));
  DVLOG(3) << "overwrite_hash: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_ == nullptr) {
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }

  if (combo.payload_length_ < payload_offset + payload_count) {
    LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

#ifndef NDEBUG
  HashBinPage* bin_page = combo.bin_pages_[combo.record_bin1_ ? 0 : 1];
  ASSERT_ND(bin_page && !bin_page->header().snapshot_);
  HashDataPage* data_page = combo.data_pages_[combo.record_bin1_ ? 0 : 1];
  ASSERT_ND(data_page && !data_page->header().snapshot_);
#endif  // NDEBUG

  uint16_t log_length = HashOverwriteLogType::calculate_log_length(key_length, payload_count);
  HashOverwriteLogType* log_entry = reinterpret_cast<HashOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    metadata_.id_,
    key,
    key_length,
    combo.record_bin1_,
    combo.record_slot_,
    payload,
    payload_offset,
    payload_count);
  return context->get_current_xct().add_to_write_set(holder_, combo.record_, log_entry);
}

template <typename PAYLOAD>
ErrorCode HashStoragePimpl::overwrite_record_primitive(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD payload,
  uint16_t payload_offset) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, true, &combo));
  DVLOG(3) << "overwrite_hash_primitive: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_ == nullptr) {
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }

  if (combo.payload_length_ < payload_offset + sizeof(PAYLOAD)) {
    LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

  uint16_t log_length = HashOverwriteLogType::calculate_log_length(key_length, sizeof(PAYLOAD));
  HashOverwriteLogType* log_entry = reinterpret_cast<HashOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    metadata_.id_,
    key,
    key_length,
    combo.record_bin1_,
    combo.record_slot_,
    &payload,
    payload_offset,
    sizeof(PAYLOAD));
  return context->get_current_xct().add_to_write_set(holder_, combo.record_, log_entry);
}

template <typename PAYLOAD>
ErrorCode HashStoragePimpl::increment_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  PAYLOAD* value,
  uint16_t payload_offset) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, true, &combo));
  DVLOG(3) << "increment_hash: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_ == nullptr) {
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }

  if (combo.payload_length_ < payload_offset + sizeof(PAYLOAD)) {
    LOG(WARNING) << "short record " << combo;  // probably this is a rare error. so warn.
    return kErrorCodeStrTooShortPayload;
  }

  char* ptr = combo.record_->payload_ + key_length + payload_offset;
  PAYLOAD old_value = *reinterpret_cast<const PAYLOAD*>(ptr);
  *value += old_value;

  uint16_t log_length = HashOverwriteLogType::calculate_log_length(key_length, sizeof(PAYLOAD));
  HashOverwriteLogType* log_entry = reinterpret_cast<HashOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    metadata_.id_,
    key,
    key_length,
    combo.record_bin1_,
    combo.record_slot_,
    value,
    payload_offset,
    sizeof(PAYLOAD));
  return context->get_current_xct().add_to_write_set(holder_, combo.record_, log_entry);
}

inline HashDataPage* to_page(xct::LockableXctId* owner_id) {
  // super-dirty way to obtain Page the record belongs to.
  // because all pages are 4kb aligned, we can just divide and multiply.
  uintptr_t int_address = reinterpret_cast<uintptr_t>(reinterpret_cast<void*>(owner_id));
  uint64_t aligned_address = static_cast<uint64_t>(int_address) / kPageSize * kPageSize;
  return reinterpret_cast<HashDataPage*>(
    reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(aligned_address)));
}

void HashStoragePimpl::apply_insert_record(
  thread::Thread* /*context*/,
  const HashInsertLogType* log_entry,
  xct::LockableXctId* owner_id,
  char* payload) {
  // NOTE tentative hack! currently "payload address" for hash insert write set points to bin page
  // so that we don't have to calculate it again.
  HashDataPage* data_page = to_page(owner_id);
  ASSERT_ND(!data_page->header().snapshot_);
  ASSERT_ND(data_page->header().get_page_type() == kHashDataPageType);

  uint64_t bin = data_page->get_bin();
  HashBinPage* bin_page = reinterpret_cast<HashBinPage*>(payload);
  ASSERT_ND(bin_page);
  ASSERT_ND(bin_page->header().get_page_type() == kHashBinPageType);
  ASSERT_ND(!bin_page->header().snapshot_);
  HashBinPage::Bin& the_bin = bin_page->bin(bin % kBinsPerPage);

#ifndef NDEBUG
  HashCombo combo(log_entry->data_, log_entry->key_length_, metadata_.bin_bits_);
  ASSERT_ND(bin == combo.bins_[log_entry->bin1_ ? 0 : 1]);
  ASSERT_ND(log_entry->hashtag_ == combo.tag_);
#endif  // NDEBUG

  // install the tag and increment the bin page's mod counter first.
  // we do this first, so there might be false positives. but there is no serializability violation.
  uint16_t slot = data_page->find_empty_slot(log_entry->key_length_, log_entry->payload_count_);
  the_bin.tags_[slot] = log_entry->hashtag_;
  assorted::raw_atomic_fetch_add<uint16_t>(&the_bin.mod_counter_, 1U);
  data_page->add_record(
    log_entry->header_.xct_id_,
    slot,
    log_entry->key_length_,
    log_entry->payload_count_,
    log_entry->data_);
}

void HashStoragePimpl::apply_delete_record(
  thread::Thread* /*context*/,
  const HashDeleteLogType* log_entry,
  xct::LockableXctId* owner_id,
  char* payload) {
  // NOTE tentative hack! currently "payload address" for hash insert write set points to bin page
  // so that we don't have to calculate it again.
  HashDataPage* data_page = to_page(owner_id);
  ASSERT_ND(!data_page->header().snapshot_);
  ASSERT_ND(data_page->header().get_page_type() == kHashDataPageType);

  uint64_t bin = data_page->get_bin();
  HashBinPage* bin_page = reinterpret_cast<HashBinPage*>(payload);
  ASSERT_ND(bin_page);
  ASSERT_ND(bin_page->header().get_page_type() == kHashBinPageType);
  ASSERT_ND(!bin_page->header().snapshot_);
  HashBinPage::Bin& the_bin = bin_page->bin(bin % kBinsPerPage);

#ifndef NDEBUG
  HashCombo combo(log_entry->data_, log_entry->key_length_, metadata_.bin_bits_);
  ASSERT_ND(bin == combo.bins_[log_entry->bin1_ ? 0 : 1]);
#endif  // NDEBUG

  uint16_t slot = log_entry->slot_;
  // We are deleting this record, so it should be locked
  ASSERT_ND(data_page->get_record_count() > slot);
  ASSERT_ND(data_page->interpret_record(slot)->owner_id_.is_keylocked());
  ASSERT_ND((data_page->slot(slot).flags_ & HashDataPage::kFlagDeleted) == 0);
  data_page->slot(slot).flags_ |= HashDataPage::kFlagDeleted;

  // we also remove tag from bin page. this happens AFTER physically deleting it with fence.
  // this protocol makes sure it's safe, although there might be false positive.
  assorted::memory_fence_release();
  the_bin.tags_[slot] = 0;
}


inline HashRootPage* HashStoragePimpl::lookup_boundary_root(
  thread::Thread* context,
  uint64_t bin,
  uint16_t* pointer_index) {
  uint64_t bin_page = bin / kBinsPerPage;
  if (root_pages_ == 1) {
    ASSERT_ND(bin_page <= kHashRootPageFanout);
    *pointer_index = bin_page;
    return root_page_;
  } else {
    uint64_t child_root = bin_page / kHashRootPageFanout;
    VolatilePagePointer pointer = root_page_->pointer(child_root).volatile_pointer_;
    ASSERT_ND(pointer.components.offset != 0);
    *pointer_index = bin_page % kHashRootPageFanout;
    return reinterpret_cast<HashRootPage*>(
      context->get_global_volatile_page_resolver().resolve_offset(pointer));
  }
}

inline uint32_t iterate_over_tags(const HashBinPage::Bin& bin, HashTag tag) {
  uint32_t hit_bitmap = 0;  // kMaxEntriesPerBin bits < 32bit
  /* TODO(Hideaki) let's do SIMD and 8-byte packed operation here. Currently 7% cost here.
  uint64_t packed =
    static_cast<uint64_t>(tag) << 48
    | static_cast<uint64_t>(tag) << 32
    | static_cast<uint64_t>(tag) << 16
    | static_cast<uint64_t>(tag);
  if (bin.tags_[0] == tag) {
    hit_bitmap |= (1U << 0);
  }
  if (bin.tags_[1] == tag) {
    hit_bitmap |= (1U << 1);
  }
  if (bin.tags_[2] == tag) {
    hit_bitmap |= (1U << 2);
  }
  const uint64_t *batched = static_cast<const uint64_t*>(bin.tags_ + 3);
  for (int i = 0; i < 5; ++i) {
    uint64_t anded = batched[i] & packed;
  }*/

  for (uint16_t tag_pos = 0; tag_pos < kMaxEntriesPerBin; ++tag_pos) {
    if (bin.tags_[tag_pos] == tag) {
      hit_bitmap |= (1U << tag_pos);
    }
  }
  return hit_bitmap;
}

ErrorCode HashStoragePimpl::lookup_bin(thread::Thread* context, bool for_write, HashCombo* combo) {
  // TODO(Hideaki) to speed up the following, we should prefetch more aggressively.
  // the code will be a bit uglier, though.

  // For writes, we take care of node-set later separately.
  // TODO(Hideaki) let's blindly load volatile pages for all writes.
  // it simplifies this method a lot.

  // find bin page
  for (uint8_t i = 0; i < 2; ++i) {
    uint16_t pointer_index;
    HashRootPage* boundary = lookup_boundary_root(context, combo->bins_[i], &pointer_index);
    HashBinPage::Initializer initializer(
      metadata_.id_,
      combo->bins_[i] / kBinsPerPage * kBinsPerPage);
    CHECK_ERROR_CODE(context->follow_page_pointer(
      &initializer,
      !for_write,  // tolerate null page for read. if that happens, we get nullptr on the bin page
      for_write,  // always get volatile pages for writes
      true,
      false,
      &(boundary->pointer(pointer_index)),
      reinterpret_cast<Page**>(&(combo->bin_pages_[i]))));
  }

  // read bin pages
  for (uint8_t i = 0; i < 2; ++i) {
    uint16_t bin_pos = combo->bins_[i] % kBinsPerPage;
    HashBinPage* bin_page = combo->bin_pages_[i];
    if (bin_page) {
      bool snapshot = bin_page->header().snapshot_;
      if (i == 0 && combo->bin_pages_[1]) {
        // when we are reading from both of them we prefetch the two 64 bytes.
        // if we are reading from only one of them, no need.
        assorted::prefetch_cacheline(&(combo->bin_pages_[0]->bin(bin_pos)));
        uint16_t another_pos = combo->bins_[1] % kBinsPerPage;
        assorted::prefetch_cacheline(&(combo->bin_pages_[1]->bin(another_pos)));
      }

      HashBinPage::Bin& bin = combo->bin_pages_[i]->bin(bin_pos);
      // add the mod counter to read set. we must do it BEFORE reading tags
      // and then take fence (consume is enough).
      if (!snapshot) {  // if we are reading a snapshot page, doesn't matter
        combo->observed_mod_count_[i] = bin.mod_counter_;
        assorted::memory_fence_consume();
      }
      combo->hit_bitmap_[i] = iterate_over_tags(bin, combo->tag_);

      // obtain data pages, but not read it yet. it's done in locate_record()
      if (combo->hit_bitmap_[i] || for_write) {
        // becauase it's hitting. either page must be non-null
        HashDataPage::Initializer initializer(metadata_.id_, combo->bins_[i]);
        CHECK_ERROR_CODE(context->follow_page_pointer(
          &initializer,
          !for_write,  // tolerate null page for read
          for_write,  // always get volatile pages for writes
          !snapshot && !for_write,  // if bin page is snapshot, data page is stable
          false,
          &(bin.data_pointer_),
          reinterpret_cast<Page**>(&(combo->data_pages_[i]))));
      } else {
        combo->data_pages_[i] = nullptr;
      }
    }
  }
  return kErrorCodeOk;
}

inline ErrorCode HashStoragePimpl::locate_record(
  thread::Thread* context,
  const void* key,
  uint16_t key_length,
  HashCombo* combo) {
  // TODO(Hideaki) prefetch.
  // TODO(Hideaki) some of the read set below is not mandatory depending on operation type.
  xct::Xct& current_xct = context->get_current_xct();
  combo->record_ = nullptr;
  for (uint8_t i = 0; i < 2; ++i) {
    if (combo->bin_pages_[i] == nullptr || combo->data_pages_[i] == nullptr) {
      continue;
    }
    HashDataPage* data_page = combo->data_pages_[i];

    // add page lock for read set. before accessing records
    current_xct.add_to_read_set(
      holder_,
      data_page->page_owner().xct_id_,
      &data_page->page_owner());

    uint32_t hit_bitmap = combo->hit_bitmap_[i];
    for (uint8_t rec = 0; rec < data_page->get_record_count(); ++rec) {
      if ((hit_bitmap & (1U << rec)) == 0) {
        continue;
      }
      // okay, this record has the same tag. let's check this by comparing the whole key.
      const HashDataPage::Slot& slot = data_page->slot(rec);
      if (slot.flags_ & HashDataPage::kFlagDeleted) {
        continue;
      } else if (slot.key_length_ != key_length) {
        continue;
      }
      // TODO(Hideaki) handle kFlagStoredInNextPages.
      Record* record = data_page->interpret_record(slot.offset_);
      current_xct.add_to_read_set(
        holder_,
        record->owner_id_.xct_id_,
        &record->owner_id_);
      if (std::memcmp(record->payload_, key, key_length) == 0) {
        // okay, matched!!
        // once we exactly locate the record, no point to check other records/bin. exit
        combo->record_ = record;
        combo->record_bin1_ = (i == 0);
        combo->record_slot_ = rec;
        combo->payload_length_ = slot.record_length_ - key_length - kRecordOverhead;
        return kErrorCodeOk;
      }
    }
  }
  return kErrorCodeOk;
}

/*
ErrorCode get_cuckoo_path(
  thread::Thread* context,
  std::vector<uint16_t>* nodes,
  std::vector<uint16_t>* adjacentnodes,
  uint16_t depth,
  uint64_t *place_tracker) {
    if(depth > 4) return kErrorCodeStrCuckooTooDeep; //need to define the error code //4 is max depth
       // give place_tracker more bits
    for(uint16_t a = 0; a < nodes -> size(); a++){
      for(uint8_t x = 0; x < 4; x++){
        uint16_t newbin = get_other_bin(context, (*nodes)[a], x);
        adjacentnodes -> push_back(newbin); //stick adjacent nodes in new bins
        for(uint8_t y=0; y < 4; y++){
          if(get_tag(context, newbin, y) == 0){ //If we find an end position in the path
            (*place_tracker) *= 4;
            (*place_tracker) += y; //add on the information for the position used in the final bucket in path
            return kErrorCodeOk;
          }
        }
        (*place_tracker) ++;
      }
    }
    nodes -> resize(0);
    return get_cuckoo_path(context, adjacentnodes, nodes, depth+1, place_tracker);
}

ErrorCode execute_path(thread::Thread* context, uint16_t bin, std::vector<uint16_t> path){ //bin is starting bin in path
//   uint8_t bin_pos = path.back(); // is a number from 0 to 3
//   path.pop_back();
//   uint8_t new_bin_pos = path[path.size()-1]; // is a number from 0 to 3
//   uint16_t newbin = get_other_bin(context, bin, bin_pos);
//   if(path.size() > 1) CHECK_ERROR_CODE(execute_path(context, newbin, place_tracker / 4));
//   (*place_tracker) ++;
//   // TODO(Bill): need to figure out how to actually get payload and payload_count (this basically has to do with page layout I think
//   // If I wanted to make it look good write now, I could just pretend I had functions for them (just like I did for some other things)
//   // But at this point I'm too lazy to do that...
//   // ALSO, DON'T I NEED THE LENGTH OF THE KEY? How could get_key function otherwise if we are using variable length keys...
//   CHECK_ERROR_CODE(write_new_record(context, new_bin, new_bin_pos,
//                                     get_key(context, bin, bin_pos),
//                                     get_tag(context, bin, bin_pos), payload, payload_count));
//   delete_record(context, bin, bin_pos); //function not written yet
  return kErrorCodeOk;
}

ErrorCode HashStoragePimpl::insert_record(thread::Thread* context, const void* key,
                                          uint16_t key_length, const void* payload, uint16_t payload_count) {
  //Do I actually need to add anything to the read set in this function?
  uint64_t bin = compute_hash(key, key_length);
  uint8_t tag = compute_tag(key, key_length);
  for(uint8_t x=0; x<4; x++){
    if(get_tag(context, bin, x) == 0) { //special value of tag that we need to make sure never occurs
      return write_new_record(context, bin, x, key, tag, payload, payload_count); //Needs to be written still
    }
  }
  bin = bin ^ tag;
  for(uint8_t x=0; x<4; x++){
    if(get_tag(context, bin, x) == 0) { //special value of tag that we need to make sure never occurs
      return write_new_record(context, bin, x, key, tag, payload, payload_count); //Needs to be written still
    }
  }
  //In this case we need to build a Cuckoo Chain we should use a bfs that way we can have as few guys in the write set as possible
  //Do we even need to add to the read set guys who we don't use in the chain?
  //For now we'll go with the second bin, even though we should go for the emptier bin in practice
  uint64_t place_tracker=0; //keeps track of how many nodes we've visited -- we can use that to reverse engineer the path to the node
  std::vector<uint16_t> nodes;
  std::vector<uint16_t> adjacentnodes;
  nodes.push_back(bin);
  CHECK_ERROR_CODE(get_cuckoo_path(context, &nodes, &adjacentnodes, 0, &place_tracker));
  //First we want to reverse place_tracker in base 4 (base 4 because we're using 4-way associativity)
  std::vector <uint16_t> path;
  while(place_tracker > 0){
    path.push_back(place_tracker % 4);
    place_tracker /= 4;
  }
  //Now we're ready to execute the path
  CHECK_ERROR_CODE(execute_path(context, bin, path));
  uint16_t positioninbin=0; //TODO
  return write_new_record(context, bin, positioninbin, key, tag, payload, payload_count);
  //TODO(Bill): Keep track of read list in this function (do we need to?) (write list is taken care of already in write_new_record function)
}
*/

// Explicit instantiations for each payload type
// @cond DOXYGEN_IGNORE
#define EXPIN_2(x) template ErrorCode HashStorage::get_record_primitive< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x* payload, \
  uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_2);

#define EXPIN_2I(x) template ErrorCode HashStoragePimpl::get_record_primitive< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x* payload, \
  uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_2I);

#define EXPIN_3(x) template ErrorCode HashStorage::overwrite_record_primitive< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x payload, \
  uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_3);

#define EXPIN_3I(x) template ErrorCode HashStoragePimpl::overwrite_record_primitive< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x payload, \
  uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_3I);

#define EXPIN_5(x) template ErrorCode HashStorage::increment_record< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5);

#define EXPIN_5I(x) template ErrorCode HashStoragePimpl::increment_record< x > \
  (thread::Thread* context, const void* key, uint16_t key_length, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPIN_5I);

// @endcond

}  // namespace hash
}  // namespace storage
}  // namespace foedus
