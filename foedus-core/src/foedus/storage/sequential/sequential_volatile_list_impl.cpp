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
    storage_id_(storage_id),
    thread_count_(engine_->get_options().thread_.get_total_thread_count()),
    head_pointers_(nullptr),
    tail_pointers_(nullptr) {
}

ErrorStack SequentialVolatileList::initialize_once() {
  ASSERT_ND(head_pointers_ == nullptr);
  ASSERT_ND(tail_pointers_ == nullptr);
  head_pointers_ = new SequentialPage*[thread_count_];
  tail_pointers_ = new SequentialPage*[thread_count_];
  std::memset(head_pointers_, 0, sizeof(SequentialPage*) * thread_count_);
  std::memset(tail_pointers_, 0, sizeof(SequentialPage*) * thread_count_);
  return kRetOk;
}

ErrorStack SequentialVolatileList::uninitialize_once() {
  ASSERT_ND(head_pointers_);
  ASSERT_ND(tail_pointers_);
  // release all pages in this list.
  uint16_t threads_per_node = engine_->get_options().thread_.thread_count_per_group_;
  memory::PagePoolOffsetChunk chunk;
  for (thread::ThreadGlobalOrdinal i = 0; i < thread_count_; ++i) {
    thread::ThreadGroupId node = i / threads_per_node;
    // we are sure these pages are from only one NUMA node, so we can easily batch-return.
    memory::NumaNodeMemory* memory = engine_->get_memory_manager().get_node_memory(node);
    memory::PagePool& pool = memory->get_volatile_pool();
    memory::LocalPageResolver& resolver = pool.get_resolver();
    for (SequentialPage* page = head_pointers_[i]; page;) {
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
    if (chunk.size() > 0) {
      pool.release(chunk.size(), &chunk);
    }
  }
  ASSERT_ND(chunk.size() == 0);

  delete[] head_pointers_;
  delete[] tail_pointers_;
  head_pointers_ = nullptr;
  tail_pointers_ = nullptr;
  return kRetOk;
}

void SequentialVolatileList::append_record(
  thread::Thread* context,
  xct::XctId owner_id,
  const void* payload,
  uint16_t payload_count) {
  thread::ThreadGroupId node = context->get_numa_node();
  thread::ThreadGlobalOrdinal ordinal = context->get_thread_global_ordinal();

  // the list is local to this core, so no race possible EXCEPT scanning thread
  // and snapshot thread, but they are read-only or only dropping pages.
  SequentialPage* tail = tail_pointers_[ordinal];
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
      context->get_global_volatile_page_resolver().resolve_offset_newpage(node, new_page_offset));
    new_page->initialize_volatile_page(storage_id_, new_page_pointer);

    if (tail == nullptr) {
      // this is the first access to this head pointer. Let's install the first page.
      ASSERT_ND(head_pointers_[ordinal] == nullptr);
      head_pointers_[ordinal] = new_page;
      tail_pointers_[ordinal] = new_page;
    } else {
      tail_pointers_[ordinal] = new_page;
      tail->next_page().volatile_pointer_ = new_page_pointer;
    }
    tail = tail_pointers_[ordinal];
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
  memory::EngineMemory& memory = v.engine_->get_memory_manager();
  const memory::GlobalVolatilePageResolver& resolver = memory.get_global_volatile_page_resolver();
  for (thread::ThreadGlobalOrdinal i = 0; i < v.thread_count_; ++i) {
    if (v.head_pointers_ && v.head_pointers_[i]) {
      for (SequentialPage* page = v.head_pointers_[i]; page;) {
        ASSERT_ND(page->header().page_id_);
        ++page_count;
        record_count += page->get_record_count();

        VolatilePagePointer next_pointer = page->next_page().volatile_pointer_;
        if (next_pointer.components.offset != 0) {
          page = reinterpret_cast<SequentialPage*>(resolver.resolve_offset(next_pointer));
        } else {
          page = nullptr;
        }
      }
    }
  }
  o << "<page_count>" << page_count << "</page_count>";
  o << "<record_count>" << record_count << "</record_count>";
  o << "</SequentialVolatileList>";
  return o;
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
