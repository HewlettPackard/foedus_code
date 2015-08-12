/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"

#include <glog/logging.h>

#include <cstring>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/meta_log_buffer.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/storage_manager_pimpl.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/storage/sequential/sequential_log_types.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace sequential {

// Defines SequentialStorage methods so that we can inline implementation calls

ErrorCode SequentialStorage::append_record(
  thread::Thread* context,
  const void *payload,
  uint16_t payload_count) {
  if (payload_count >= kMaxPayload) {
    return kErrorCodeStrTooLongPayload;
  }

  // Sequential storage doesn't need to check its current state for appends.
  // we are sure we can append it anyways, so we just create a log record.
  uint16_t log_length = SequentialAppendLogType::calculate_log_length(payload_count);
  SequentialAppendLogType* log_entry = reinterpret_cast<SequentialAppendLogType*>(
    context->get_thread_log_buffer().reserve_new_log(log_length));
  log_entry->populate(get_id(), payload, payload_count);

  // also, we don't have to take a lock while commit because our SequentialVolatileList is
  // lock-free. So, we maintain a special lock-free write-set for sequential storage.
  return context->get_current_xct().add_to_lock_free_write_set(get_id(), log_entry);
}

void SequentialStorage::apply_append_record(
  thread::Thread* context,
  const SequentialAppendLogType* log_entry) {
  SequentialStoragePimpl(this).append_record(
    context,
    log_entry->header_.xct_id_,
    log_entry->payload_,
    log_entry->payload_count_);
}

ErrorStack SequentialStoragePimpl::drop() {
  LOG(INFO) << "Uninitializing an sequential-storage " << get_name();
  // release all pages in this list.
  uint16_t nodes = engine_->get_options().thread_.group_count_;
  uint16_t threads_per_node = engine_->get_options().thread_.thread_count_per_group_;
  for (uint16_t node = 0; node < nodes; ++node) {
    // we are sure these pages are from only one NUMA node, so we can easily batch-return.
    memory::NumaNodeMemoryRef* memory = engine_->get_memory_manager()->get_node_memory(node);
    memory::PagePool* pool = memory->get_volatile_pool();
    const memory::LocalPageResolver& resolver = pool->get_resolver();
    memory::PagePoolOffsetChunk chunk;
    for (uint16_t local_ordinal = 0; local_ordinal < threads_per_node; ++local_ordinal) {
      thread::ThreadId thread_id = thread::compose_thread_id(node, local_ordinal);
      for (SequentialPage* page = get_head(resolver, thread_id); page;) {
        ASSERT_ND(page->header().page_id_);
        VolatilePagePointer cur_pointer;
        cur_pointer.word = page->header().page_id_;
        ASSERT_ND(page == reinterpret_cast<SequentialPage*>(resolver.resolve_offset(
          cur_pointer.components.offset)));
        ASSERT_ND(node == cur_pointer.components.numa_node);
        VolatilePagePointer next_pointer = page->next_page().volatile_pointer_;
        if (chunk.full()) {
          pool->release(chunk.size(), &chunk);
        }
        ASSERT_ND(!chunk.full());
        chunk.push_back(cur_pointer.components.offset);

        if (next_pointer.components.offset != 0) {
          ASSERT_ND(node == next_pointer.components.numa_node);
          page = reinterpret_cast<SequentialPage*>(resolver.resolve_offset(
            next_pointer.components.offset));
        } else {
          page = nullptr;
        }
      }
    }
    if (chunk.size() > 0) {
      pool->release(chunk.size(), &chunk);
    }
    ASSERT_ND(chunk.size() == 0);
  }

  // release pointer pages
  for (uint16_t p = 0; p * 4 < nodes; ++p) {
    uint16_t node = p * 4;
    memory::NumaNodeMemoryRef* memory = engine_->get_memory_manager()->get_node_memory(node);
    memory::PagePool* pool = memory->get_volatile_pool();
    if (control_block_->head_pointer_pages_[p].components.offset) {
      ASSERT_ND(control_block_->head_pointer_pages_[p].components.numa_node == node);
      pool->release_one(control_block_->head_pointer_pages_[p].components.offset);
    }
    if (control_block_->tail_pointer_pages_[p].components.offset) {
      ASSERT_ND(control_block_->tail_pointer_pages_[p].components.numa_node == node);
      pool->release_one(control_block_->tail_pointer_pages_[p].components.offset);
    }
  }
  std::memset(control_block_->head_pointer_pages_, 0, sizeof(control_block_->head_pointer_pages_));
  std::memset(control_block_->tail_pointer_pages_, 0, sizeof(control_block_->tail_pointer_pages_));
  return kRetOk;
}

ErrorStack SequentialStoragePimpl::create(const SequentialMetadata& metadata) {
  if (exists()) {
    LOG(ERROR) << "This sequential-storage already exists: " << get_name();
    return ERROR_STACK(kErrorCodeStrAlreadyExists);
  }

  control_block_->meta_ = metadata;
  const Epoch initial_truncate_epoch = engine_->get_earliest_epoch();
  control_block_->meta_.truncate_epoch_ = initial_truncate_epoch.value();
  control_block_->cur_truncate_epoch_.store(initial_truncate_epoch.value());
  control_block_->cur_truncate_epoch_tid_.reset();
  control_block_->cur_truncate_epoch_tid_.xct_id_.set_epoch(initial_truncate_epoch);
  control_block_->cur_truncate_epoch_tid_.xct_id_.set_ordinal(1);

  CHECK_ERROR(initialize_head_tail_pages());
  control_block_->status_ = kExists;
  LOG(INFO) << "Newly created a sequential-storage " << get_name();
  return kRetOk;
}
ErrorStack SequentialStoragePimpl::load(const StorageControlBlock& snapshot_block) {
  // for sequential storage, whether the snapshot root pointer is null or not doesn't matter.
  // essentially load==create, except that it just sets the snapshot root pointer.
  control_block_->meta_ = static_cast<const SequentialMetadata&>(snapshot_block.meta_);
  Epoch initial_truncate_epoch(control_block_->meta_.truncate_epoch_);
  if (!initial_truncate_epoch.is_valid()) {
    initial_truncate_epoch = engine_->get_earliest_epoch();
  }
  ASSERT_ND(initial_truncate_epoch.is_valid());
  control_block_->meta_.truncate_epoch_ = initial_truncate_epoch.value();
  control_block_->cur_truncate_epoch_.store(initial_truncate_epoch.value());
  control_block_->cur_truncate_epoch_tid_.reset();
  control_block_->cur_truncate_epoch_tid_.xct_id_.set_epoch(initial_truncate_epoch);
  control_block_->cur_truncate_epoch_tid_.xct_id_.set_ordinal(1);

  CHECK_ERROR(initialize_head_tail_pages());
  control_block_->root_page_pointer_.snapshot_pointer_
    = control_block_->meta_.root_snapshot_page_id_;
  control_block_->status_ = kExists;
  LOG(INFO) << "Loaded a sequential-storage " << get_name();
  return kRetOk;
}
ErrorStack SequentialStoragePimpl::initialize_head_tail_pages() {
  std::memset(control_block_->head_pointer_pages_, 0, sizeof(control_block_->head_pointer_pages_));
  std::memset(control_block_->tail_pointer_pages_, 0, sizeof(control_block_->tail_pointer_pages_));
  // we pre-allocate pointer pages for all required nodes.
  // 2^10 pointers (threads) per page : 4 nodes per page

  uint32_t nodes = engine_->get_options().thread_.group_count_;
  for (uint16_t p = 0; p * 4 < nodes; ++p) {
    uint16_t node = p * 4;
    memory::NumaNodeMemoryRef* memory = engine_->get_memory_manager()->get_node_memory(node);
    memory::PagePool* pool = memory->get_volatile_pool();
    memory::PagePoolOffset head_offset, tail_offset;
    // minor todo: gracefully fail in case of out of memory
    WRAP_ERROR_CODE(pool->grab_one(&head_offset));
    WRAP_ERROR_CODE(pool->grab_one(&tail_offset));
    control_block_->head_pointer_pages_[p] = combine_volatile_page_pointer(node, 0, 0, head_offset);
    control_block_->tail_pointer_pages_[p] = combine_volatile_page_pointer(node, 0, 0, tail_offset);
    void* head_page =  pool->get_resolver().resolve_offset_newpage(head_offset);
    void* tail_page =  pool->get_resolver().resolve_offset_newpage(tail_offset);
    std::memset(head_page, 0, kPageSize);
    std::memset(tail_page, 0, kPageSize);
  }
  return kRetOk;
}

ErrorCode SequentialStorageControlBlock::optimistic_read_truncate_epoch(
  thread::Thread* context,
  Epoch* out) const {
  xct::Xct& cur_xct = context->get_current_xct();
  if (!cur_xct.is_active()) {
    return kErrorCodeXctNoXct;
  }

  auto* address = &cur_truncate_epoch_tid_;
  xct::XctId observed = address->xct_id_;
  while (UNLIKELY(observed.is_being_written())) {
    assorted::memory_fence_acquire();
    observed = address->xct_id_;
  }

  *out = Epoch(cur_truncate_epoch_.load());  // atomic!
  CHECK_ERROR_CODE(cur_xct.add_to_read_set(
    meta_.id_,
    observed,
    const_cast< xct::LockableXctId* >(address)));  // why it doesn't receive const? I forgot..
  return kErrorCodeOk;
}

ErrorStack SequentialStoragePimpl::truncate(Epoch new_truncate_epoch, Epoch* commit_epoch) {
  LOG(INFO) << "Truncating " << get_name() << " upto Epoch " << new_truncate_epoch
    << ". old value=" << control_block_->cur_truncate_epoch_;
  if (!new_truncate_epoch.is_valid()) {
    LOG(ERROR) << "truncate() was called with an invalid epoch";
    return ERROR_STACK(kErrorCodeInvalidParameter);
  } else if (new_truncate_epoch < engine_->get_earliest_epoch()) {
    LOG(ERROR) << "too-old epoch for this system. " << new_truncate_epoch;
    return ERROR_STACK(kErrorCodeInvalidParameter);
  }

  // will check this again after locking.
  if (Epoch(control_block_->cur_truncate_epoch_) >= new_truncate_epoch) {
    LOG(INFO) << "Already truncated up to " << Epoch(control_block_->cur_truncate_epoch_)
      << ". Requested = " << new_truncate_epoch;
    return kRetOk;
  }

  if (new_truncate_epoch > engine_->get_current_global_epoch()) {
    LOG(WARNING) << "Ohh? we don't prohibit it, but are you sure? Truncating up to a future"
      << " epoch-" << new_truncate_epoch << ". cur_global=" << engine_->get_current_global_epoch();
  }


  // We lock it first so that there are no concurrent truncate.
  {
    // TODO(Hideaki) Ownerless-lock here
    // xct::McsOwnerlessLockScope scope(&control_block_->cur_truncate_epoch_tid_);

    if (Epoch(control_block_->cur_truncate_epoch_) >= new_truncate_epoch) {
      LOG(INFO) << "Already truncated up to " << Epoch(control_block_->cur_truncate_epoch_)
        << ". Requested = " << new_truncate_epoch;
      return kRetOk;
    }

    // First, let scanner know that something is happening.
    // 1. Scanners that didn't observe this and commit before us: fine.
    // 2. Scanners that didn't observe this and commit after us:
    //      will see this flag and abort in precommit, fine.
    // 3. Scanners that observe this:
    //      spin until we are done, fine (see optimistic_read_truncate_epoch()).
    // NOTE: Below, we must NOT have any error-return path. Otherwise being_written state is left.
    control_block_->cur_truncate_epoch_tid_.xct_id_.set_being_written();
    assorted::memory_fence_acq_rel();

    // Log this operation as a metadata operation. We get a commit_epoch here.
    {
      char log_buffer[sizeof(SequentialTruncateLogType)];
      std::memset(log_buffer, 0, sizeof(log_buffer));
      SequentialTruncateLogType* the_log = reinterpret_cast<SequentialTruncateLogType*>(log_buffer);
      the_log->header_.storage_id_ = get_id();
      the_log->header_.log_type_code_ = log::get_log_code<SequentialTruncateLogType>();
      the_log->header_.log_length_ = sizeof(SequentialTruncateLogType);
      the_log->new_truncate_epoch_ = new_truncate_epoch;
      engine_->get_log_manager()->get_meta_buffer()->commit(the_log, commit_epoch);
    }

    // Then, apply it. This also clears the being_written flag
    {
      xct::XctId xct_id;
      xct_id.set(commit_epoch->value(), 1);  // no dependency, so minimal ordinal is always correct
      control_block_->cur_truncate_epoch_.store(new_truncate_epoch.value());  // atomic!
      control_block_->cur_truncate_epoch_tid_.xct_id_ = xct_id;
      assorted::memory_fence_release();

      // Also set to the metadata to make this permanent.
      // The metadata will be written out in next snapshot.
      // Until that, REDO-operation below will re-apply that after crash.
      control_block_->meta_.truncate_epoch_ = new_truncate_epoch.value();
      assorted::memory_fence_release();
    }
  }

  LOG(INFO) << "Truncated";
  return kRetOk;
}

void SequentialStoragePimpl::apply_truncate(const SequentialTruncateLogType& the_log) {
  // this method is called only during restart, so no race.
  ASSERT_ND(control_block_->exists());
  control_block_->cur_truncate_epoch_tid_.xct_id_ = the_log.header_.xct_id_;
  control_block_->cur_truncate_epoch_.store(the_log.new_truncate_epoch_.value());
  control_block_->meta_.truncate_epoch_ = the_log.new_truncate_epoch_.value();
  LOG(INFO) << "Applied redo-log of truncation on sequential storage- " << get_name()
    << " epoch=" << control_block_->cur_truncate_epoch_;
}

void SequentialStoragePimpl::append_record(
  thread::Thread* context,
  xct::XctId owner_id,
  const void* payload,
  uint16_t payload_count) {
  thread::ThreadId thread_id = context->get_thread_id();
  thread::ThreadGroupId node = context->get_numa_node();

  // the list is local to this core, so no race possible EXCEPT scanning thread
  // and snapshot thread, but they are read-only or only dropping pages.
  memory::PagePoolOffset* tail_pointer = get_tail_pointer(thread_id);
  ASSERT_ND(tail_pointer);
  SequentialPage* tail = nullptr;
  if (*tail_pointer != 0) {
    tail = reinterpret_cast<SequentialPage*>(
      context->get_local_volatile_page_resolver().resolve_offset(*tail_pointer));
  }
  if (tail == nullptr ||
      !tail->can_insert_record(payload_count) ||
      // note: we make sure no volatile page has records from two epochs.
      // this makes us easy to drop volatile pages after snapshotting.
      (tail->get_record_count() > 0 && tail->get_first_record_epoch() != owner_id.get_epoch())) {
    memory::PagePoolOffset new_page_offset
      = context->get_thread_memory()->grab_free_volatile_page();
    if (UNLIKELY(new_page_offset == 0)) {
      LOG(FATAL) << " Unexpected error. we ran out of free page while inserting to sequential"
        " storage after commit.";
    }
    VolatilePagePointer new_page_pointer;
    new_page_pointer = combine_volatile_page_pointer(node, 0, 0, new_page_offset);
    SequentialPage* new_page = reinterpret_cast<SequentialPage*>(
      context->get_local_volatile_page_resolver().resolve_offset_newpage(new_page_offset));
    new_page->initialize_volatile_page(get_id(), new_page_pointer);

    if (tail == nullptr) {
      // this is the first access to this head pointer. Let's install the first page.
      ASSERT_ND(*tail_pointer == 0);
      memory::PagePoolOffset* head_pointer = get_head_pointer(thread_id);
      ASSERT_ND(*head_pointer == 0);
      *head_pointer = new_page_offset;
      *tail_pointer = new_page_offset;
    } else {
      ASSERT_ND(*get_head_pointer(thread_id) != 0);
      *tail_pointer = new_page_offset;
      tail->next_page().volatile_pointer_ = new_page_pointer;
    }
    tail = new_page;
  }

  ASSERT_ND(tail &&
    tail->can_insert_record(payload_count) &&
    (tail->get_record_count() == 0 || tail->get_first_record_epoch() == owner_id.get_epoch()));
  tail->append_record_nosync(owner_id, payload_count, payload);
}

memory::PagePoolOffset* SequentialStoragePimpl::get_head_pointer(thread::ThreadId thread_id) const {
  ASSERT_ND(thread::decompose_numa_node(thread_id) < engine_->get_options().thread_.group_count_);
  ASSERT_ND(thread::decompose_numa_local_ordinal(thread_id)
    < engine_->get_options().thread_.thread_count_per_group_);
  uint16_t page;
  uint16_t index;
  get_pointer_page_and_index(thread_id, &page, &index);
  const memory::GlobalVolatilePageResolver& resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  PointerPage* head_page
    = reinterpret_cast<PointerPage*>(resolver.resolve_offset_newpage(
      control_block_->head_pointer_pages_[page]));
  return head_page->pointers_ + index;
}
memory::PagePoolOffset* SequentialStoragePimpl::get_tail_pointer(thread::ThreadId thread_id) const {
  ASSERT_ND(thread::decompose_numa_node(thread_id) < engine_->get_options().thread_.group_count_);
  ASSERT_ND(thread::decompose_numa_local_ordinal(thread_id)
    < engine_->get_options().thread_.thread_count_per_group_);
  uint16_t page;
  uint16_t index;
  get_pointer_page_and_index(thread_id, &page, &index);
  const memory::GlobalVolatilePageResolver& resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  PointerPage* tail_page
    = reinterpret_cast<PointerPage*>(resolver.resolve_offset_newpage(
      control_block_->tail_pointer_pages_[page]));
  return tail_page->pointers_ + index;
}

SequentialPage* SequentialStoragePimpl::get_head(
  const memory::LocalPageResolver& resolver,
  thread::ThreadId thread_id) const {
  memory::PagePoolOffset offset = *get_head_pointer(thread_id);
  if (offset == 0) {
    return nullptr;
  }
  return reinterpret_cast<SequentialPage*>(resolver.resolve_offset(offset));
}

SequentialPage* SequentialStoragePimpl::get_tail(
  const memory::LocalPageResolver& resolver,
  thread::ThreadId thread_id) const {
  memory::PagePoolOffset offset = *get_tail_pointer(thread_id);
  if (offset == 0) {
    return nullptr;
  }
  return reinterpret_cast<SequentialPage*>(resolver.resolve_offset(offset));
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
