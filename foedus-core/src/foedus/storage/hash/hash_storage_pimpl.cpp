/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/hash/hash_storage_pimpl.hpp"

#include <xmmintrin.h>
#include <glog/logging.h>

#include <cstring>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/assorted/assorted_func.hpp"
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
#include "foedus/xct/xct_inl.hpp"
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
  const char* key,
  uint16_t key_length,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  return pimpl_->get_record_part(context, key, key_length, payload, payload_offset, payload_count);
}

ErrorCode HashStorage::insert_record(
  thread::Thread* context,
  const char* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_count) {
  return pimpl_->insert_record(context, key, key_length, payload, payload_count);
}

ErrorCode HashStorage::delete_record(
  thread::Thread* context,
  const char* key,
  uint16_t key_length) {
  return pimpl_->delete_record(context, key, key_length);
}

ErrorCode HashStorage::overwrite_record(
  thread::Thread* context,
  const char* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  return pimpl_->overwrite_record(context, key, key_length, payload, payload_offset, payload_count);
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
        ASSERT_ND(child_page_id.components.offset != 0);
        HashRootPage* child_page = reinterpret_cast<HashRootPage*>(
          page_resolver.resolve_offset(child_page_id));
        release_pages_recursive_root(&release_batch, child_page, child_page_id);
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
  root_page_ = reinterpret_cast<HashRootPage*>(local_resolver.resolve_offset(root_offset));
  root_page_pointer_.snapshot_pointer_ = 0;
  root_page_pointer_.volatile_pointer_ = combine_volatile_page_pointer(
    context->get_numa_node(),
    0,
    0,
    root_offset);
  root_page_->initialize_page(metadata_.id_, root_page_pointer_.volatile_pointer_.word);
  if (root_pages_ > 1) {
    for (uint16_t i = 0; i < root_pages_; ++i) {
      memory::PagePoolOffset offset = memory->grab_free_volatile_page();
      ASSERT_ND(offset);
      HashRootPage* page = reinterpret_cast<HashRootPage*>(local_resolver.resolve_offset(offset));
      VolatilePagePointer pointer = combine_volatile_page_pointer(
        context->get_numa_node(),
        0,
        0,
        offset);
      page->initialize_page(metadata_.id_, pointer.word);
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
  CHECK_ERROR_CODE(lookup_bin(context, &combo));
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

ErrorCode HashStoragePimpl::get_record_part(
  thread::Thread* context,
  const char* key,
  uint16_t key_length,
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, &combo));
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

ErrorCode HashStoragePimpl::make_room(
// TODO(Bill) Will go horribly wrong if we come back to a position at which we have already been
// TODO(Bill) Add case where the path is considered too long.
  thread::Thread* context,
  HashDataPage* data_page) {
  if (data_page -> get_record_count() == kMaxEntriesPerBin) {
    uint16_t pick = rand() % kMaxEntriesPerBin;  // TODO(Bill) Need to initialize seed
    uint16_t key_length = data_page ->slot(pick).key_length_;
    uint32_t offset = data_page -> slot(pick).offset_;
    Record* kickrec = data_page -> interpret_record(offset);
    char* key = kickrec -> payload_;
    HashCombo combo(key, key_length, metadata_.bin_bits_);
    CHECK_ERROR_CODE(lookup_bin(context, &combo));
    HashDataPage* other_page = combo.data_pages_[0];
    if (other_page == data_page) {
      other_page = combo.data_pages_[1];
    }
    make_room(context, other_page);
    delete_record(context, key, key_length);
    uint8_t choice = (combo.data_pages_[0] == other_page) ? 0 : 1;
    insert_record_chosen_bin(context, key, key_length,
                             kickrec -> payload_,
                             combo.payload_length_ ,
                             choice, combo);
  }
  return kErrorCodeOk;
}


ErrorCode HashStoragePimpl::insert_record_chosen_bin(
  thread::Thread* context,
  const char* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_count,
  uint8_t choice,
  HashCombo combo) {
  // TODO(Hideaki) if the bin/data page is a snapshot or does not exist, we have to install
  // a new volatile page. also, we need to physically create a record.
  HashDataPage* data_page = combo.data_pages_[choice];
  HashBinPage* bin_page = combo.bin_pages_[choice];
  if (data_page == nullptr || data_page->header().snapshot_) {
    if (bin_page == nullptr || bin_page->header().snapshot_) {
      // bin page too
      uint16_t pointer_index;
      HashRootPage* boundary = lookup_boundary_root(context, combo.bins_[choice], &pointer_index);
      DualPagePointer& pointer = boundary->pointer(pointer_index);
      storage::VolatilePagePointer cur_pointer = pointer.volatile_pointer_;
      // TODO(Hideaki) the following must be refactored to a method
      if (pointer.snapshot_pointer_ != 0) {
        CHECK_ERROR_CODE(context->install_a_volatile_page(
          &pointer,
          reinterpret_cast<Page*>(boundary),
          reinterpret_cast<Page**>(&bin_page)));
      } else {
        memory::PagePoolOffset offset = context->get_thread_memory()->grab_free_volatile_page();
        if (UNLIKELY(offset == 0)) {
          return kErrorCodeMemoryNoFreePages;
        }
        bin_page = reinterpret_cast<HashBinPage*>(
          context->get_local_volatile_page_resolver().resolve_offset(offset));
        VolatilePagePointer new_pointer
          = combine_volatile_page_pointer(context->get_numa_node(), 0, 1, offset);
        bin_page->initialize_page(
          metadata_.id_,
          new_pointer.word,
          combo.bins_[choice] / kBinsPerPage * kBinsPerPage,
          combo.bins_[choice] / kBinsPerPage * kBinsPerPage + kBinsPerPage);
        if (assorted::raw_atomic_compare_exchange_strong<uint64_t>(
          &(pointer.volatile_pointer_.word),
          &(cur_pointer.word),
          new_pointer.word)) {
          // good
        } else {
          // Then there must be a new page installed by someone else.
          context->get_thread_memory()->release_free_volatile_page(offset);
          ASSERT_ND(pointer.volatile_pointer_.components.offset != 0);
          bin_page = reinterpret_cast<HashBinPage*>(
            context->get_global_volatile_page_resolver().resolve_offset(cur_pointer));
        }
      }
    }
    // TODO(Hideaki) again, refactor
    DualPagePointer& pointer = bin_page->bin(combo.bins_[choice] % kBinsPerPage).data_pointer_;
    storage::VolatilePagePointer cur_pointer = pointer.volatile_pointer_;
    if (pointer.snapshot_pointer_ != 0) {
      CHECK_ERROR_CODE(context->install_a_volatile_page(
        &pointer,
        reinterpret_cast<Page*>(bin_page),
        reinterpret_cast<Page**>(&data_page)));
    } else {
      memory::PagePoolOffset offset = context->get_thread_memory()->grab_free_volatile_page();
      if (UNLIKELY(offset == 0)) {
        return kErrorCodeMemoryNoFreePages;
      }
      data_page = reinterpret_cast<HashDataPage*>(
        context->get_local_volatile_page_resolver().resolve_offset(offset));
      VolatilePagePointer new_pointer
        = combine_volatile_page_pointer(context->get_numa_node(), 0, 1, offset);
      data_page->initialize_page(metadata_.id_, new_pointer.word, combo.bins_[choice]);
      if (assorted::raw_atomic_compare_exchange_strong<uint64_t>(
        &(pointer.volatile_pointer_.word),
        &(cur_pointer.word),
        new_pointer.word)) {
        // good
      } else {
        // Then there must be a new page installed by someone else.
        context->get_thread_memory()->release_free_volatile_page(offset);
        ASSERT_ND(pointer.volatile_pointer_.components.offset != 0);
        data_page = reinterpret_cast<HashDataPage*>(
          context->get_global_volatile_page_resolver().resolve_offset(cur_pointer));
      }
    }
  }

  ASSERT_ND(bin_page && !bin_page->header().snapshot_);
  ASSERT_ND(data_page && !data_page->header().snapshot_);

  uint16_t log_length = HashInsertLogType::calculate_log_length(key_length, payload_count);
  HashInsertLogType* log_entry = reinterpret_cast<HashInsertLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(metadata_.id_, key, key_length, (choice == 1), payload, payload_count);
  Record* page_lock_record = reinterpret_cast<Record*>(&(data_page->page_owner()));
  return context->get_current_xct().add_to_write_set(holder_, page_lock_record, log_entry);
}


ErrorCode HashStoragePimpl::insert_record(
  thread::Thread* context,
  const char* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_count) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, &combo));
  DVLOG(3) << "insert_hash: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo))

  if (combo.record_) {
    // TODO(Hideaki) Add the mod counter to a special read set
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

  // TODO(Hideaki) if the bin/data page is a snapshot or does not exist, we have to install
  // a new volatile page. also, we need to physically create a record.
  HashDataPage* data_page = combo.data_pages_[choice];
  if (data_page -> get_record_count() == kMaxEntriesPerBin) make_room(context, data_page);
  insert_record_chosen_bin(context, key, key_length, payload, payload_count, choice, combo);

//   HashBinPage* bin_page = combo.bin_pages_[choice];
//   if (data_page == nullptr || data_page->header().snapshot_) {
//     if (bin_page == nullptr || bin_page->header().snapshot_) {
//       // bin page too
//       uint16_t pointer_index;
//       HashRootPage* boundary = lookup_boundary_root(context, combo.bins_[choice], &pointer_index);
//       DualPagePointer& pointer = boundary->pointer(pointer_index);
//       storage::VolatilePagePointer cur_pointer = pointer.volatile_pointer_;
//       // TODO(Hideaki) the following must be refactored to a method
//       if (pointer.snapshot_pointer_ != 0) {
//         CHECK_ERROR_CODE(context->install_a_volatile_page(
//           &pointer,
//           reinterpret_cast<Page*>(boundary),
//           reinterpret_cast<Page**>(&bin_page)));
//       } else {
//         memory::PagePoolOffset offset = context->get_thread_memory()->grab_free_volatile_page();
//         if (UNLIKELY(offset == 0)) {
//           return kErrorCodeMemoryNoFreePages;
//         }
//         bin_page = reinterpret_cast<HashBinPage*>(
//           context->get_local_volatile_page_resolver().resolve_offset(offset));
//         VolatilePagePointer new_pointer
//           = combine_volatile_page_pointer(context->get_numa_node(), 0, 1, offset);
//         bin_page->initialize_page(
//           metadata_.id_,
//           new_pointer.word,
//           combo.bins_[choice] / kBinsPerPage * kBinsPerPage,
//           combo.bins_[choice] / kBinsPerPage * kBinsPerPage + kBinsPerPage);
//         if (assorted::raw_atomic_compare_exchange_strong<uint64_t>(
//           &(pointer.volatile_pointer_.word),
//           &(cur_pointer.word),
//           new_pointer.word)) {
//           // good
//         } else {
//           // Then there must be a new page installed by someone else.
//           context->get_thread_memory()->release_free_volatile_page(offset);
//           ASSERT_ND(pointer.volatile_pointer_.components.offset != 0);
//           bin_page = reinterpret_cast<HashBinPage*>(
//             context->get_global_volatile_page_resolver().resolve_offset(cur_pointer));
//         }
//       }
//     }
//     // TODO(Hideaki) again, refactor
//     DualPagePointer& pointer = bin_page->bin(combo.bins_[choice] % kBinsPerPage).data_pointer_;
//     storage::VolatilePagePointer cur_pointer = pointer.volatile_pointer_;
//     if (pointer.snapshot_pointer_ != 0) {
//       CHECK_ERROR_CODE(context->install_a_volatile_page(
//         &pointer,
//         reinterpret_cast<Page*>(bin_page),
//         reinterpret_cast<Page**>(&data_page)));
//     } else {
//       memory::PagePoolOffset offset = context->get_thread_memory()->grab_free_volatile_page();
//       if (UNLIKELY(offset == 0)) {
//         return kErrorCodeMemoryNoFreePages;
//       }
//       data_page = reinterpret_cast<HashDataPage*>(
//         context->get_local_volatile_page_resolver().resolve_offset(offset));
//       VolatilePagePointer new_pointer
//         = combine_volatile_page_pointer(context->get_numa_node(), 0, 1, offset);
//       data_page->initialize_page(metadata_.id_, new_pointer.word, combo.bins_[choice]);
//       if (assorted::raw_atomic_compare_exchange_strong<uint64_t>(
//         &(pointer.volatile_pointer_.word),
//         &(cur_pointer.word),
//         new_pointer.word)) {
//         // good
//       } else {
//         // Then there must be a new page installed by someone else.
//         context->get_thread_memory()->release_free_volatile_page(offset);
//         ASSERT_ND(pointer.volatile_pointer_.components.offset != 0);
//         data_page = reinterpret_cast<HashDataPage*>(
//           context->get_global_volatile_page_resolver().resolve_offset(cur_pointer));
//       }
//     }
//   }
//
//   ASSERT_ND(bin_page && !bin_page->header().snapshot_);
//   ASSERT_ND(data_page && !data_page->header().snapshot_);
//
//   uint16_t log_length = HashInsertLogType::calculate_log_length(key_length, payload_count);
//   HashInsertLogType* log_entry = reinterpret_cast<HashInsertLogType*>(
//     context->get_thread_log_buffer().reserve_new_log(log_length));
//   log_entry->populate(metadata_.id_, key, key_length, bin1, payload, payload_count);
//   Record* page_lock_record = reinterpret_cast<Record*>(&(data_page->page_owner()));
//   return context->get_current_xct().add_to_write_set(holder_, page_lock_record, log_entry);
}

ErrorCode HashStoragePimpl::delete_record(
  thread::Thread* context,
  const char* key,
  uint16_t key_length) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, &combo));
  DVLOG(3) << "delete_hash: hash=" << combo;
  CHECK_ERROR_CODE(locate_record(context, key, key_length, &combo));

  if (combo.record_ == nullptr) {
    // TODO(Hideaki) Add the mod counter to a special read set
    return kErrorCodeStrKeyNotFound;
  }

  if (combo.data_pages_[combo.record_bin1_ ? 0 : 1]->header().snapshot_) {
    // TODO(Hideaki) the data page is a snapshot page. we have to install a new volatile page
  }

  uint16_t log_length = HashDeleteLogType::calculate_log_length(key_length);
  HashDeleteLogType* log_entry = reinterpret_cast<HashDeleteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(metadata_.id_, key, key_length, combo.record_bin1_);
  return context->get_current_xct().add_to_write_set(holder_, combo.record_, log_entry);
}

ErrorCode HashStoragePimpl::overwrite_record(
  thread::Thread* context ,
  const char* key,
  uint16_t key_length,
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  HashCombo combo(key, key_length, metadata_.bin_bits_);
  CHECK_ERROR_CODE(lookup_bin(context, &combo));
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

  if (combo.data_pages_[combo.record_bin1_ ? 0 : 1]->header().snapshot_) {
    // TODO(Hideaki) the data page is a snapshot page. we have to install a new volatile page
  }

  uint16_t log_length = HashOverwriteLogType::calculate_log_length(key_length, payload_count);
  HashOverwriteLogType* log_entry = reinterpret_cast<HashOverwriteLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(
    metadata_.id_,
    key,
    key_length,
    combo.record_bin1_,
    payload,
    payload_offset,
    payload_count);
  return context->get_current_xct().add_to_write_set(holder_, combo.record_, log_entry);
}

inline HashRootPage* HashStoragePimpl::lookup_boundary_root(
  thread::Thread* context,
  uint64_t bin,
  uint16_t* pointer_index) {
  uint64_t bin_page = bin / kBinsPerPage;
  ASSERT_ND(bin_page <= kHashRootPageFanout);
  if (root_pages_ == 1) {
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

ErrorCode HashStoragePimpl::lookup_bin(thread::Thread* context, HashCombo* combo) {
  const memory::GlobalVolatilePageResolver& page_resolver
    = context->get_global_volatile_page_resolver();
  xct::Xct& current_xct = context->get_current_xct();

  // TODO(Hideaki) to speed up the following, we should prefetch more aggressively.
  // the code will be a bit uglier, though.

  // find bin page
  for (uint8_t i = 0; i < 2; ++i) {
    uint16_t pointer_index;
    HashRootPage* boundary = lookup_boundary_root(context, combo->bins_[i], &pointer_index);
    // TODO(Hideaki) the follwoing logic is appearing in a few places. should become a method.
    const DualPagePointer& pointer = boundary->pointer(pointer_index);
    storage::VolatilePagePointer volatile_pointer = pointer.volatile_pointer_;
    if (pointer.snapshot_pointer_ == 0) {
      if (volatile_pointer.components.offset == 0) {
        // oh, both null, so the page is not created yet. definitely no such record.
        // just add the pointer to read set.
        current_xct.add_to_node_set(&pointer.volatile_pointer_, volatile_pointer);
        combo->bin_pages_[i] = nullptr;
      } else {
        // then we have to follow it anyway
        combo->bin_pages_[i] = reinterpret_cast<HashBinPage*>(
          page_resolver.resolve_offset(volatile_pointer));
      }
    } else {
      // if there is a snapshot page, we have a few more choices.
      if (volatile_pointer.components.offset == 0 ||
        current_xct.get_isolation_level() == xct::kDirtyReadPreferSnapshot) {
        // then read from snapshot page.
        current_xct.add_to_node_set(&pointer.volatile_pointer_, volatile_pointer);
        CHECK_ERROR_CODE(context->find_or_read_a_snapshot_page(
          pointer.snapshot_pointer_,
          reinterpret_cast<Page**>(&combo->bin_pages_[i])));
      } else {
        combo->bin_pages_[i] = reinterpret_cast<HashBinPage*>(
          page_resolver.resolve_offset(volatile_pointer));
      }
    }
  }

  // read bin pages
  for (uint8_t i = 0; i < 2; ++i) {
    uint16_t bin_pos = combo->bins_[i] % kBinsPerPage;
    if (combo->bin_pages_[i]) {
      bool snapshot = combo->bin_pages_[i]->header().snapshot_;
      if (i == 0 && combo->bin_pages_[1]) {
        // when we are reading from both of them we prefetch the two 64 bytes.
        // if we are reading from only one of them, no need.
        ::_mm_prefetch(&(combo->bin_pages_[0]->bin(bin_pos)), ::_MM_HINT_T0);
        uint16_t another_pos = combo->bins_[1] % kBinsPerPage;
        ::_mm_prefetch(&(combo->bin_pages_[1]->bin(another_pos)), ::_MM_HINT_T0);
      }

      const HashBinPage::Bin& bin = combo->bin_pages_[i]->bin(bin_pos);
      // add the mod counter to read set. we must do it BEFORE reading tags
      // and then take fence (consume is enough).
      if (!snapshot) {  // if we are reading a snapshot page, doesn't matter
        combo->observed_mod_count_[i] = bin.mod_counter_;
        assorted::memory_fence_consume();
      }
      uint32_t hit_bitmap = 0;  // kMaxEntriesPerBin bits < 32bit
      HashTag tag = combo->tag_;
      for (uint16_t tag_pos = 0; tag_pos < kMaxEntriesPerBin; ++tag_pos) {
        if (bin.tags_[tag_pos] == tag) {
          hit_bitmap |= (1U << tag_pos);
        }
      }
      combo->hit_bitmap_[i] = hit_bitmap;

      // obtain data pages, but not read it yet. it's done in locate_record()
      if (hit_bitmap) {
        // becauase it's hitting. either page must be non-null
        const DualPagePointer& pointer = bin.data_pointer_;
        storage::VolatilePagePointer volatile_pointer = pointer.volatile_pointer_;
        if (pointer.snapshot_pointer_ == 0 ||
            (volatile_pointer.components.offset != 0 &&
              current_xct.get_isolation_level() != xct::kDirtyReadPreferSnapshot)) {
          combo->data_pages_[i] = reinterpret_cast<HashDataPage*>(
            page_resolver.resolve_offset(volatile_pointer));
        } else {
          if (!snapshot) {
            // if bin page is snapshot, data page is stable
            current_xct.add_to_node_set(&pointer.volatile_pointer_, volatile_pointer);
          }
          CHECK_ERROR_CODE(context->find_or_read_a_snapshot_page(
            pointer.snapshot_pointer_,
            reinterpret_cast<Page**>(&combo->data_pages_[i])));
        }
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
    current_xct.add_to_read_set(holder_, reinterpret_cast<Record*>(&data_page->page_owner()));

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
      current_xct.add_to_read_set(holder_, record);
      if (std::memcmp(record->payload_, key, key_length) == 0) {
        // okay, matched!!
        // once we exactly locate the record, no point to check other records/bin. exit
        combo->record_ = record;
        combo->record_bin1_ = (i == 0);
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
       // give place_tracker more bits
    for(uint16_t a = 0; a < nodes -> size(); a++){
      for(uint8_t x = 0; x < bin_size; x++){
        uint16_t newbin = get_other_bin(context, (*nodes)[a], x);
        adjacentnodes -> push_back(newbin); //stick adjacent nodes in new bins
        for(uint8_t y=0; y < bin_size; y++){
          if(get_tag(context, newbin, y) == 0){ //If we find an end position in the path
            (*place_tracker) *= bin_size;
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
//   if(path.size() > 1) CHECK_ERROR_CODE(execute_path(context, newbin, place_tracker / bin_size));
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


bool is_empty(int bin, int position){
  return true;
}

void transfer(int bin, int pos, int bin2, int pos2){
}

/*
ErrorCode bfschain(Thread::thread* context, std::vector<int> nodes, int &freebin, int &freepos){
    std::vector<node> adjacent(0);
    for(int x=0; x<nodes.size(); x++){ //for each bin in node set
      for(int y = 0; y < bin_size; y++){ //for each position in bin
        int other_bin = get_other_bin(context, nodes[x], y); //get the adjacent bin
        for(int z=0; z<bin_size; z++){ //for each position in adjacent bin
          if(is_empty(other_bin, z)) { //if that position is empty //NEED TO DEFINE
            transfer(nodes[x], y, other_bin, z); //transfer into that position from the bin in the node set
            freebin = x;
            freepos = y;
            return kErrorCodeOk;
          }
        }
        adjacent.push_back(other_bin);
      }
    }
    CHECK_ERROR_CODE(bfschain(adjacent, freebin, freepos));
    transfer(nodes[freebin / bin_size], freebin % bin_size, adjacent[freebin], freepos);
    freepos = freebin % bin_size;
    freebin = freebin / bin_size;
    return kErrorCodeOk;
}
*/
/*

ErrorCode HashStoragePimpl::insert_record(thread::Thread* context, const void* key,
                                          uint16_t key_length, const void* payload, uint16_t payload_count) {
  //Do I actually need to add anything to the read set in this function?
  uint64_t bin = compute_hash(key, key_length);
  uint8_t tag = compute_tag(key, key_length);
  for(uint8_t x=0; x<bin_size; x++){
    if(get_tag(context, bin, x) == 0) { //special value of tag that we need to make sure never occurs
      return write_new_record(context, bin, x, key, tag, payload, payload_count); //Needs to be written still
    }
  }
  bin = bin ^ tag;
  for(uint8_t x=0; x<bin_size; x++){
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
  //First we want to reverse place_tracker in base bin_size (base bin_size because we're using bin_size-way associativity)
  std::vector <uint16_t> path;
  while(place_tracker > 0){
    path.push_back(place_tracker % bin_size);
    place_tracker /= bin_size;
  }
  //Now we're ready to execute the path
  CHECK_ERROR_CODE(execute_path(context, bin, path));
  uint16_t positioninbin=0; //TODO
  return write_new_record(context, bin, positioninbin, key, tag, payload, payload_count);
  //TODO(Bill): Keep track of read list in this function (do we need to?) (write list is taken care of already in write_new_record function)
}
*/


}  // namespace hash
}  // namespace storage
}  // namespace foedus
