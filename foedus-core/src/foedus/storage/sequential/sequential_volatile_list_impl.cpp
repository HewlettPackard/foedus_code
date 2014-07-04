/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_volatile_list_impl.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace sequential {
ErrorStack SequentialVolatileList::initialize_once() {
  ASSERT_ND(head_ == nullptr);
  ASSERT_ND(tail_ == nullptr);
  // the first page is simply retrieved from first page pool.
  // alternatively, the first appender could set it, but this is simpler as
  // head_/tail_ are always non-null.
  const uint8_t kNode = 0;  // simply assume the first node for initial page
  memory::PagePool& pool = engine_->get_memory_manager().get_node_memory(kNode)->get_page_pool();
  memory::PagePoolOffset initial_page_offset;
  WRAP_ERROR_CODE(pool.grab_one(&initial_page_offset));  // this is slower, but just once
  VolatilePagePointer initial_page_pointer = combine_volatile_page_pointer(
    kNode,
    0,
    0,
    initial_page_offset);
  SequentialPage* initial_page = reinterpret_cast<SequentialPage*>(
    pool.get_resolver().resolve_offset(initial_page_offset));
  ASSERT_ND(initial_page);
  initial_page->initialize_data_page(storage_id_, initial_page_pointer.word);
  head_ = initial_page;
  tail_ = initial_page;
  return kRetOk;
}

ErrorStack SequentialVolatileList::uninitialize_once() {
  ASSERT_ND(head_);
  ASSERT_ND(tail_);
  // release all pages in this list. return one by one.
  // TODO(Hideaki): optimize this by allocating PageOffsetChunk for each NUMA node and
  // batch-return them. But, again, this is just once at the end. Low priority...
  memory::EngineMemory& memory = engine_->get_memory_manager();
  const memory::GlobalPageResolver& resolver = memory.get_global_page_resolver();
  for (SequentialPage* page = head_; page;) {
    ASSERT_ND(page->header().page_id_);
    VolatilePagePointer cur_pointer;
    cur_pointer.word = page->header().page_id_;
    ASSERT_ND(page == reinterpret_cast<SequentialPage*>(resolver.resolve_offset(cur_pointer)));

    uint8_t node = cur_pointer.components.numa_node;
    VolatilePagePointer next_pointer = page->next_page().volatile_pointer_;

    memory::PagePool& pool = memory.get_node_memory(node)->get_page_pool();
    ASSERT_ND(page == reinterpret_cast<SequentialPage*>(
      pool.get_resolver().resolve_offset(cur_pointer.components.offset)));
    pool.release_one(cur_pointer.components.offset);

    if (next_pointer.components.offset != 0) {
      page = reinterpret_cast<SequentialPage*>(resolver.resolve_offset(next_pointer));
    } else {
      page = nullptr;
    }
  }

  head_ = nullptr;
  tail_ = nullptr;
  return kRetOk;
}

void SequentialVolatileList::append_record(
  thread::Thread* context, xct::XctId owner_id, const void* payload, uint16_t payload_count) {
  uint8_t node = context->get_numa_node();
  Epoch epoch = owner_id.get_epoch();
  while (true) {
    // note: we make sure no volatile page has records from two epochs.
    // this makes us easy to drop volatile pages after snapshotting.
    if (tail_->can_insert_record(payload_count) && tail_->get_first_record_epoch() == epoch) {
      bool succeeded = tail_->append_record(owner_id, payload_count, payload);
      if (succeeded) {
        break;
      }
    }

    // we need to insert a new page. 1) close the page, 2) install next page.
    bool this_thread_closed_it = tail_->try_close_page();
    if (this_thread_closed_it) {
      // this thread closed it, so it is responsible for installing next page.
      memory::PagePoolOffset new_page_offset = context->get_thread_memory()->grab_free_page();
      if (UNLIKELY(new_page_offset == 0)) {
        LOG(FATAL) << " Unexpected error. we ran out of free page while inserting to sequential"
          " storage after commit.";
      }

      VolatilePagePointer new_page_pointer;
      new_page_pointer = combine_volatile_page_pointer(node, 0, 0, new_page_offset);

      SequentialPage* new_page = reinterpret_cast<SequentialPage*>(
        context->get_global_page_resolver().resolve_offset(node, new_page_offset));
      new_page->initialize_data_page(storage_id_, new_page_pointer.word);
      new_page->append_record_nosync(owner_id, payload_count, payload);
      // change tail pointer BEFORE (with barrier) setting next pointer in ex-tail so that
      // no one else can change tail_ even if this thread gets stalled right now.
      SequentialPage* tmp_tail = tail_;
      tail_ = new_page;  // change the tail to the new page
      assorted::memory_fence_release();
      tmp_tail->next_page().volatile_pointer_ = new_page_pointer;
      return;  // we are done!
    } else {
      // other thread closed it. let's wait for the thread installing a next page.
      SPINLOCK_WHILE(tail_->next_page().volatile_pointer_.components.offset == 0) {
      }
      continue;  // retry
    }
  }
}
std::ostream& operator<<(std::ostream& o, const SequentialVolatileList& v) {
  o << "<SequentialVolatileList>";
    o << "<storage_id_>" << v.storage_id_ << "</storage_id_>";
  if (v.head_) {
    uint64_t page_count = 0;
    uint64_t record_count = 0;
    uint64_t last_page_status = 0;
    memory::EngineMemory& memory = v.engine_->get_memory_manager();
    const memory::GlobalPageResolver& resolver = memory.get_global_page_resolver();
    for (SequentialPage* page = v.head_; page;) {
      ASSERT_ND(page->header().page_id_);
      ++page_count;
      record_count += page->get_record_count();

      VolatilePagePointer next_pointer = page->next_page().volatile_pointer_;
      if (next_pointer.components.offset != 0) {
        page = reinterpret_cast<SequentialPage*>(resolver.resolve_offset(next_pointer));
      } else {
        last_page_status = page->peek_status();
        page = nullptr;
      }
    }
    o << "<page_count>" << page_count << "</page_count>";
    o << "<record_count>" << record_count << "</record_count>";
    o << "<last_page_status>" << assorted::Hex(last_page_status) << "</last_page_status>";
  }
  o << "</SequentialVolatileList>";
  return o;
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
