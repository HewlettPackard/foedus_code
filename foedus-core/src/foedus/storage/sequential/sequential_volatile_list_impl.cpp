/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_volatile_list_impl.hpp"

#include <glog/logging.h>

#include <cstring>
#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"

namespace foedus {
namespace storage {
namespace sequential {

SequentialVolatileList::SequentialVolatileList(Engine* engine, StorageId storage_id)
  : engine_(engine),
    storage_id_(storage_id) {
  std::memset(head_pointer_pages_, 0, sizeof(head_pointer_pages_));
  std::memset(tail_pointer_pages_, 0, sizeof(tail_pointer_pages_));
  std::memset(head_pointer_pages_cache_, 0, sizeof(head_pointer_pages_cache_));
  std::memset(tail_pointer_pages_cache_, 0, sizeof(tail_pointer_pages_cache_));
}

ErrorStack SequentialVolatileList::initialize_once() {
  // we pre-allocate pointer pages for all required nodes.
  // 2^10 pointers (threads) per page : 4 nodes per page
  std::memset(head_pointer_pages_, 0, sizeof(head_pointer_pages_));
  std::memset(tail_pointer_pages_, 0, sizeof(tail_pointer_pages_));
  std::memset(head_pointer_pages_cache_, 0, sizeof(head_pointer_pages_cache_));
  std::memset(tail_pointer_pages_cache_, 0, sizeof(tail_pointer_pages_cache_));
  uint32_t nodes = engine_->get_options().thread_.group_count_;
  for (uint16_t p = 0; p * 4 < nodes; ++p) {
    uint16_t node = p * 4;
    memory::NumaNodeMemory* memory = engine_->get_memory_manager().get_node_memory(node);
    memory::PagePool& pool = memory->get_volatile_pool();
    memory::PagePoolOffset head_offset, tail_offset;
    // minor todo: gracefully fail in case of out of memory
    WRAP_ERROR_CODE(pool.grab_one(&head_offset));
    WRAP_ERROR_CODE(pool.grab_one(&tail_offset));
    head_pointer_pages_[p] = combine_volatile_page_pointer(node, 0, 0, head_offset);
    tail_pointer_pages_[p] = combine_volatile_page_pointer(node, 0, 0, tail_offset);
    void* head_page =  pool.get_resolver().resolve_offset_newpage(head_offset);
    void* tail_page =  pool.get_resolver().resolve_offset_newpage(tail_offset);
    std::memset(head_page, 0, kPageSize);
    std::memset(tail_page, 0, kPageSize);
    head_pointer_pages_cache_[p] = reinterpret_cast<PointerPage*>(head_page);
    tail_pointer_pages_cache_[p] = reinterpret_cast<PointerPage*>(tail_page);
  }
  return kRetOk;
}

ErrorStack SequentialVolatileList::uninitialize_once() {
  // release all pages in this list.
  uint16_t nodes = engine_->get_options().thread_.group_count_;
  uint16_t threads_per_node = engine_->get_options().thread_.thread_count_per_group_;
  for (uint16_t node = 0; node < nodes; ++node) {
    // we are sure these pages are from only one NUMA node, so we can easily batch-return.
    memory::NumaNodeMemory* memory = engine_->get_memory_manager().get_node_memory(node);
    memory::PagePool& pool = memory->get_volatile_pool();
    memory::LocalPageResolver& resolver = pool.get_resolver();
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
          pool.release(chunk.size(), &chunk);
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
      pool.release(chunk.size(), &chunk);
    }
    ASSERT_ND(chunk.size() == 0);
  }

  // release pointer pages
  for (uint16_t p = 0; p * 4 < nodes; ++p) {
    uint16_t node = p * 4;
    memory::NumaNodeMemory* memory = engine_->get_memory_manager().get_node_memory(node);
    memory::PagePool& pool = memory->get_volatile_pool();
    if (head_pointer_pages_[p].components.offset) {
      ASSERT_ND(head_pointer_pages_[p].components.numa_node == node);
      pool.release_one(head_pointer_pages_[p].components.offset);
    }
    if (tail_pointer_pages_[p].components.offset) {
      ASSERT_ND(tail_pointer_pages_[p].components.numa_node == node);
      pool.release_one(tail_pointer_pages_[p].components.offset);
    }
  }
  std::memset(head_pointer_pages_, 0, sizeof(head_pointer_pages_));
  std::memset(tail_pointer_pages_, 0, sizeof(tail_pointer_pages_));
  std::memset(head_pointer_pages_cache_, 0, sizeof(head_pointer_pages_cache_));
  std::memset(tail_pointer_pages_cache_, 0, sizeof(tail_pointer_pages_cache_));
  return kRetOk;
}

void SequentialVolatileList::append_record(
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
    new_page->initialize_volatile_page(storage_id_, new_page_pointer);

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
std::ostream& operator<<(std::ostream& o, const SequentialVolatileList& v) {
  o << "<SequentialVolatileList>";
    o << "<storage_id_>" << v.storage_id_ << "</storage_id_>";

  uint64_t page_count = 0;
  uint64_t record_count = 0;
  v.for_every_page([&page_count, &record_count](SequentialPage* page){
    ++page_count;
    record_count += page->get_record_count();
    return kErrorCodeOk;
  });
  o << "<page_count>" << page_count << "</page_count>";
  o << "<record_count>" << record_count << "</record_count>";
  o << "</SequentialVolatileList>";
  return o;
}

memory::PagePoolOffset* SequentialVolatileList::get_head_pointer(thread::ThreadId thread_id) const {
  ASSERT_ND(thread::decompose_numa_node(thread_id) < engine_->get_options().thread_.group_count_);
  ASSERT_ND(thread::decompose_numa_local_ordinal(thread_id)
    < engine_->get_options().thread_.thread_count_per_group_);
  uint16_t page;
  uint16_t index;
  get_pointer_page_and_index(thread_id, &page, &index);
  return head_pointer_pages_cache_[page]->pointers_ + index;
}
memory::PagePoolOffset* SequentialVolatileList::get_tail_pointer(thread::ThreadId thread_id) const {
  ASSERT_ND(thread::decompose_numa_node(thread_id) < engine_->get_options().thread_.group_count_);
  ASSERT_ND(thread::decompose_numa_local_ordinal(thread_id)
    < engine_->get_options().thread_.thread_count_per_group_);
  uint16_t page;
  uint16_t index;
  get_pointer_page_and_index(thread_id, &page, &index);
  return tail_pointer_pages_cache_[page]->pointers_ + index;
}

SequentialPage* SequentialVolatileList::get_head(
  const memory::LocalPageResolver& resolver,
  thread::ThreadId thread_id) const {
  memory::PagePoolOffset offset = *get_head_pointer(thread_id);
  if (offset == 0) {
    return nullptr;
  }
  return reinterpret_cast<SequentialPage*>(resolver.resolve_offset(offset));
}

SequentialPage* SequentialVolatileList::get_tail(
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
