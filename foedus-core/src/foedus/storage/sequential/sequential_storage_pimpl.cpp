/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"

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
#include "foedus/memory/page_pool.hpp"
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

  control_block_->status_ = kExists;
  LOG(INFO) << "Newly created an sequential-storage " << get_name();
  return kRetOk;
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

ErrorStack SequentialStoragePimpl::replace_pointers(
  const Composer::ReplacePointersArguments& args) {
  // In sequential, there is only one snapshot pointer to install, the root page.
  control_block_->root_page_pointer_.snapshot_pointer_ = args.new_root_page_pointer_;
  ++(*args.installed_count_);

  // other than that, it's just about dropping volatile pages. easy.
  // no need to determine what volatile pages to keep, or install snapshot pages to them.
  uint16_t nodes = engine_->get_options().thread_.group_count_;
  uint16_t threads_per_node = engine_->get_options().thread_.thread_count_per_group_;
  for (uint16_t node = 0; node < nodes; ++node) {
    const memory::LocalPageResolver& resolver
      = engine_->get_memory_manager()->get_node_memory(node)->get_volatile_pool()->get_resolver();
    for (uint16_t local_ordinal = 0; local_ordinal < threads_per_node; ++local_ordinal) {
      thread::ThreadId thread_id = thread::compose_thread_id(node, local_ordinal);
      memory::PagePoolOffset* head_ptr = get_head_pointer(thread_id);
      memory::PagePoolOffset* tail_ptr = get_tail_pointer(thread_id);
      memory::PagePoolOffset tail_offset = *tail_ptr;
      if ((*head_ptr) == 0) {
        ASSERT_ND(tail_offset == 0);
        VLOG(0) << "No volatile pages for thread-" << thread_id << " in sequential-" << get_id();
        continue;
      }

      ASSERT_ND(tail_offset != 0);
      while (true) {
        memory::PagePoolOffset offset = *head_ptr;
        ASSERT_ND(offset != 0);

        // if the page is newer than the snapshot, keep them.
        // all volatile pages/records are appended in epoch order, so no need to check further.
        SequentialPage* head = reinterpret_cast<SequentialPage*>(resolver.resolve_offset(offset));
        ASSERT_ND(head->get_record_count() > 0);
        if (head->get_record_count() > 0 && head->get_first_record_epoch() > args.until_epoch_) {
          VLOG(0) << "Thread-" << thread_id << " in sequential-" << get_id() << " keeps volatile"
            << " pages at and after epoch-" << head->get_first_record_epoch();
          break;
        }

        // okay, drop this
        memory::PagePoolOffset next = head->next_page().volatile_pointer_.components.offset;
        ASSERT_ND(next != offset);
        ASSERT_ND(head->next_page().volatile_pointer_.components.numa_node == node);
        args.drop_volatile_page(combine_volatile_page_pointer(node, 0, 0, offset));
        if (next == 0) {
          // it was the tail
          ASSERT_ND(tail_offset == offset);
          VLOG(0) << "Thread-" << thread_id << " in sequential-" << get_id() << " dropped all"
            << " volatile pages";
          *head_ptr = 0;
          *tail_ptr = 0;
          break;
        } else {
          // move head
          *head_ptr = next;
          DVLOG(1) << "Thread-" << thread_id << " in sequential-" << get_id() << " dropped a"
            << " page.";
        }
      }
    }
  }
  return kRetOk;
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
