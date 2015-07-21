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
#include "foedus/storage/sequential/sequential_composer_impl.hpp"

#include <glog/logging.h>

#include <ostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/snapshot/log_gleaner_resource.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/composer.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/sequential/sequential_log_types.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/storage/sequential/sequential_partitioner_impl.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace sequential {

SequentialComposer::SequentialComposer(Composer *parent)
  : engine_(parent->get_engine()), storage_id_(parent->get_storage_id()) {
}

/**
 * Unlike other composers, this one doesn't need merge sort. It's dumb simple.
 */
struct StreamStatus {
  ErrorCode init(snapshot::SortedBuffer* stream) {
    stream_ = stream;
    stream_->assert_checks();
    buffer_ = stream->get_buffer();
    buffer_size_ = stream->get_buffer_size();
    cur_absolute_pos_ = stream->get_cur_block_abosulte_begin();
    ASSERT_ND(cur_absolute_pos_ >= stream->get_offset());
    cur_relative_pos_ = cur_absolute_pos_ - stream->get_offset();
    end_absolute_pos_ = stream->get_cur_block_abosulte_end();
    ended_ = false;
    read_entry();
    if (cur_absolute_pos_ >= end_absolute_pos_) {
      ended_ = true;
    } else if (cur_relative_pos_ + cur_length_ > buffer_size_) {
      LOG(INFO) << "interesting, super unlucky case. wind immediately";
      CHECK_ERROR_CODE(wind_stream());
      read_entry();
      ASSERT_ND(cur_relative_pos_ + cur_length_ <= buffer_size_);
    }
    return kErrorCodeOk;
  }
  ErrorCode next() {
    ASSERT_ND(!ended_);
    ASSERT_ND(cur_relative_pos_ + cur_length_ <= buffer_size_);
    cur_absolute_pos_ += cur_length_;
    cur_relative_pos_ += cur_length_;
    if (UNLIKELY(cur_absolute_pos_ >= end_absolute_pos_)) {
      ended_ = true;
      return kErrorCodeOk;
    } else if (UNLIKELY(cur_relative_pos_ >= buffer_size_)) {
      CHECK_ERROR_CODE(wind_stream());
    }
    read_entry();
    if (UNLIKELY(cur_relative_pos_ + cur_length_ > buffer_size_)) {
      CHECK_ERROR_CODE(wind_stream());
      read_entry();
    }

    ASSERT_ND(cur_relative_pos_ + cur_length_ <= buffer_size_);
    return kErrorCodeOk;
  }
  ErrorCode wind_stream() {
    CHECK_ERROR_CODE(stream_->wind(cur_absolute_pos_));
    cur_relative_pos_ = stream_->to_relative_pos(cur_absolute_pos_);
    return kErrorCodeOk;
  }
  void read_entry() {
    const SequentialAppendLogType* entry = get_entry();
    ASSERT_ND(entry->header_.get_type() == log::kLogCodeSequentialAppend);
    ASSERT_ND(entry->header_.log_length_ > 0);
    cur_length_ = entry->header_.log_length_;
    cur_payload_ = entry->payload_;
    cur_owner_id_ = entry->header_.xct_id_;
  }
  const SequentialAppendLogType* get_entry() const {
    return reinterpret_cast<const SequentialAppendLogType*>(buffer_ + cur_relative_pos_);
  }

  snapshot::SortedBuffer* stream_;
  const char*     buffer_;
  uint64_t        buffer_size_;
  uint64_t        cur_absolute_pos_;
  uint64_t        cur_relative_pos_;
  uint64_t        end_absolute_pos_;
  uint32_t        cur_length_;
  const void*     cur_payload_;
  xct::XctId      cur_owner_id_;
  bool            ended_;
};

SequentialPage* SequentialComposer::compose_new_head(snapshot::SnapshotWriter* snapshot_writer) {
  SnapshotPagePointer head_page_id = snapshot_writer->get_next_page_id();
  SequentialPage* page = reinterpret_cast<SequentialPage*>(snapshot_writer->get_page_base());
  page->initialize_snapshot_page(storage_id_, head_page_id);
  return page;
}

ErrorStack SequentialComposer::dump_pages(
  snapshot::SnapshotWriter* snapshot_writer,
  bool last_dump,
  uint32_t allocated_pages,
  uint64_t* total_pages) {
  SequentialPage* base = reinterpret_cast<SequentialPage*>(snapshot_writer->get_page_base());
  ASSERT_ND(snapshot_writer->get_next_page_id() == base->header().page_id_);

  SequentialPage* tail_page = base + allocated_pages - 1;
  SnapshotPagePointer next_head_page_id = tail_page->header().page_id_ + 1ULL;
  ASSERT_ND(tail_page->next_page().snapshot_pointer_ == 0);
  if (!last_dump) {
    // a bit of trick here. Usually, we can't point to a page that is not written yet,
    // but in this case we are sure that we are writing out a page contiguously,
    // so we can set the next-page pointer at this point.
    tail_page->next_page().snapshot_pointer_ = next_head_page_id;
  }

  WRAP_ERROR_CODE(snapshot_writer->dump_pages(0, allocated_pages));
  *total_pages += allocated_pages;
  ASSERT_ND(snapshot_writer->get_next_page_id() == next_head_page_id);
  return kRetOk;
}

ErrorStack SequentialComposer::compose(const Composer::ComposeArguments& args) {
  debugging::StopWatch stop_watch;

  // Everytime it's full, we write out all pages. much simpler than other storage types.
  // No intermediate pages to track any information.
  snapshot::SnapshotWriter* snapshot_writer = args.snapshot_writer_;
  SequentialPage* base = reinterpret_cast<SequentialPage*>(snapshot_writer->get_page_base());

  SequentialPage* cur_page = compose_new_head(snapshot_writer);
  SnapshotPagePointer head_page_id = cur_page->header().page_id_;
  uint32_t allocated_pages = 1;
  uint64_t total_pages = 0;
  const uint32_t max_pages = snapshot_writer->get_page_size();
  VLOG(0) << to_string() << " composing with " << args.log_streams_count_ << " streams.";
  Epoch min_epoch;
  Epoch max_epoch;
  for (uint32_t i = 0; i < args.log_streams_count_; ++i) {
    StreamStatus status;
    WRAP_ERROR_CODE(status.init(args.log_streams_[i]));
    while (!status.ended_) {
      const SequentialAppendLogType* entry = status.get_entry();
      Epoch epoch = entry->header_.xct_id_.get_epoch();
      min_epoch.store_min(epoch);
      max_epoch.store_max(epoch);

      // need to allocate a new page?
      if (!cur_page->can_insert_record(entry->payload_count_)) {
        // need to flush the buffer?
        if (allocated_pages >= max_pages) {
          // dump everything and allocate a new head page
          CHECK_ERROR(dump_pages(snapshot_writer, false, allocated_pages, &total_pages));
          cur_page = compose_new_head(snapshot_writer);
          allocated_pages = 1;
        } else {
          // sequential storage is a bit special. As every page is written-once, we need only
          // snapshot pointer. No dual page pointers.
          SequentialPage* next_page = base + allocated_pages;
          ++allocated_pages;
          next_page->initialize_snapshot_page(storage_id_, cur_page->header().page_id_ + 1ULL);
          cur_page->next_page().snapshot_pointer_ = next_page->header().page_id_;
          cur_page = next_page;
          ASSERT_ND(extract_numa_node_from_snapshot_pointer(cur_page->header().page_id_)
              == snapshot_writer->get_numa_node());
          ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(cur_page->header().page_id_)
              == snapshot_writer->get_snapshot_id());
        }
      }

      ASSERT_ND(cur_page->can_insert_record(entry->payload_count_));
      cur_page->append_record_nosync(
        status.cur_owner_id_,
        entry->payload_count_,
        status.cur_payload_);

      // then, read next
      WRAP_ERROR_CODE(status.next());
    }
  }
  // dump everything
  CHECK_ERROR(dump_pages(snapshot_writer, true, allocated_pages, &total_pages));

  // this compose() emits just one pointer to the head page.
  RootInfoPage* root_info_page_casted = reinterpret_cast<RootInfoPage*>(args.root_info_page_);
  root_info_page_casted->header_.storage_id_ = storage_id_;
  root_info_page_casted->pointer_.page_id_ = head_page_id;
  root_info_page_casted->pointer_.from_epoch_ = min_epoch;
  root_info_page_casted->pointer_.to_epoch_ = max_epoch.one_more();  // to make it exclusive
  root_info_page_casted->pointer_.page_count_ = total_pages;

  stop_watch.stop();
  LOG(INFO) << to_string() << " compose() done in " << stop_watch.elapsed_ms() << "ms. # pages="
    << total_pages << ", head_page=" << assorted::Hex(head_page_id, 16)
      << ", min_epoch=" << min_epoch << ", max_epoch=" << max_epoch;
  return kRetOk;
}

ErrorStack SequentialComposer::construct_root(const Composer::ConstructRootArguments& args) {
  debugging::StopWatch stop_watch;

  snapshot::SnapshotWriter* snapshot_writer = args.snapshot_writer_;
  std::vector<HeadPagePointer> all_head_pages;
  SequentialStorage storage(engine_, storage_id_);
  SnapshotPagePointer previous_root_page_pointer = storage.get_metadata()->root_snapshot_page_id_;
  for (SnapshotPagePointer page_id = previous_root_page_pointer; page_id != 0;) {
    // if there already is a root page, read them all.
    // we have to anyway re-write all of them, at least the next pointer.
    SequentialRootPage* root_page = reinterpret_cast<SequentialRootPage*>(
      args.gleaner_resource_->work_memory_.get_block());
    WRAP_ERROR_CODE(args.previous_snapshot_files_->read_page(page_id, root_page));
    ASSERT_ND(root_page->header().storage_id_ == storage_id_);
    ASSERT_ND(root_page->header().page_id_ == page_id);
    for (uint16_t i = 0; i < root_page->get_pointer_count(); ++i) {
      all_head_pages.push_back(root_page->get_pointers()[i]);
    }
    page_id = root_page->get_next_page();
  }

  // each root_info_page contains just one pointer to the head page.
  for (uint32_t i = 0; i < args.root_info_pages_count_; ++i) {
    const RootInfoPage* info_page = reinterpret_cast<const RootInfoPage*>(args.root_info_pages_[i]);
    ASSERT_ND(info_page->pointer_.page_id_ > 0);
    all_head_pages.push_back(info_page->pointer_);
  }
  VLOG(0) << to_string() << " construct_root() total head page pointers=" << all_head_pages.size();

  // now simply write out root pages that contain these pointers.
  SequentialRootPage* base = reinterpret_cast<SequentialRootPage*>(
    snapshot_writer->get_page_base());
  SequentialRootPage* cur_page = base;
  uint32_t allocated_pages = 1;
  SnapshotPagePointer root_of_root_page_id = snapshot_writer->get_next_page_id();
  cur_page->initialize_snapshot_page(storage_id_, root_of_root_page_id);
  for (uint32_t written_pointers = 0; written_pointers < all_head_pages.size();) {
    uint16_t count_in_this_page = std::min<uint64_t>(
      all_head_pages.size() - written_pointers,
      kRootPageMaxHeadPointers);
    cur_page->set_pointers(&all_head_pages[written_pointers], count_in_this_page);
    written_pointers += count_in_this_page;
    if (written_pointers < all_head_pages.size()) {
      // we need next page in root page.
      SequentialRootPage* new_page = cur_page + 1;
      new_page->initialize_snapshot_page(storage_id_, cur_page->header().page_id_ + 1);
      ASSERT_ND(extract_numa_node_from_snapshot_pointer(new_page->header().page_id_)
          == snapshot_writer->get_numa_node());
      ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(new_page->header().page_id_)
          == snapshot_writer->get_snapshot_id());
      cur_page->set_next_page(new_page->header().page_id_);
      cur_page = new_page;
      ++allocated_pages;
    } else {
      ASSERT_ND(written_pointers == all_head_pages.size());
    }
  }

  // write out the new root pages
  WRAP_ERROR_CODE(snapshot_writer->dump_pages(0, allocated_pages));
  ASSERT_ND(snapshot_writer->get_next_page_id() == root_of_root_page_id + allocated_pages);
  *args.new_root_page_pointer_ = root_of_root_page_id;

  // In sequential, there is only one snapshot pointer to install, the root page.
  storage.get_control_block()->root_page_pointer_.snapshot_pointer_ = root_of_root_page_id;
  storage.get_control_block()->meta_.root_snapshot_page_id_ = root_of_root_page_id;

  stop_watch.stop();
  VLOG(0) << to_string() << " construct_root() done in " << stop_watch.elapsed_us() << "us."
    << " total head page pointers=" << all_head_pages.size()
    << ". new root head page=" << assorted::Hex(*args.new_root_page_pointer_)
    << ". root_page_count=" << allocated_pages;
  return kRetOk;
}

std::string SequentialComposer::to_string() const {
  return std::string("SequentialComposer-") + std::to_string(storage_id_);
}

Composer::DropResult SequentialComposer::drop_volatiles(
  const Composer::DropVolatilesArguments& args) {
  // In sequential, no need to determine what volatile pages to keep.
  SequentialStorage storage(engine_, storage_id_);
  SequentialStoragePimpl pimpl(engine_, storage.get_control_block());
  uint16_t nodes = engine_->get_options().thread_.group_count_;
  uint16_t threads_per_node = engine_->get_options().thread_.thread_count_per_group_;
  for (uint16_t node = 0; node < nodes; ++node) {
    if (args.partitioned_drop_ && args.my_partition_ != node) {
      continue;
    }
    const memory::LocalPageResolver& resolver
      = engine_->get_memory_manager()->get_node_memory(node)->get_volatile_pool()->get_resolver();
    for (uint16_t local_ordinal = 0; local_ordinal < threads_per_node; ++local_ordinal) {
      thread::ThreadId thread_id = thread::compose_thread_id(node, local_ordinal);
      memory::PagePoolOffset* head_ptr = pimpl.get_head_pointer(thread_id);
      memory::PagePoolOffset* tail_ptr = pimpl.get_tail_pointer(thread_id);
      memory::PagePoolOffset tail_offset = *tail_ptr;
      if ((*head_ptr) == 0) {
        ASSERT_ND(tail_offset == 0);
        VLOG(0) << "No volatile pages for thread-" << thread_id << " in sequential-" << storage_id_;
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
        if (head->get_record_count() > 0
          && head->get_first_record_epoch() > args.snapshot_.valid_until_epoch_) {
          VLOG(0) << "Thread-" << thread_id << " in sequential-" << storage_id_ << " keeps volatile"
            << " pages at and after epoch-" << head->get_first_record_epoch();
          break;
        }

        // okay, drop this
        memory::PagePoolOffset next = head->next_page().volatile_pointer_.components.offset;
        ASSERT_ND(next != offset);
        ASSERT_ND(next == 0 || head->next_page().volatile_pointer_.components.numa_node == node);
        args.drop(engine_, combine_volatile_page_pointer(node, 0, 0, offset));
        if (next == 0) {
          // it was the tail
          ASSERT_ND(tail_offset == offset);
          VLOG(0) << "Thread-" << thread_id << " in sequential-" << storage_id_ << " dropped all"
            << " volatile pages";
          *head_ptr = 0;
          *tail_ptr = 0;
          break;
        } else {
          // move head
          *head_ptr = next;
          DVLOG(2) << "Thread-" << thread_id << " in sequential-" << storage_id_ << " dropped a"
            << " page.";
        }
      }
    }
  }
  return Composer::DropResult(args);  // always everything dropped
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
