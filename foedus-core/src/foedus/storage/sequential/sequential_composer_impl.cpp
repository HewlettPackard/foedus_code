/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/sequential/sequential_composer_impl.hpp"

#include <glog/logging.h>

#include <ostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/sequential/sequential_log_types.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/storage/sequential/sequential_partitioner_impl.hpp"

namespace foedus {
namespace storage {
namespace sequential {

SequentialComposer::SequentialComposer(
    Engine *engine,
    const SequentialPartitioner* partitioner,
    snapshot::SnapshotWriter* snapshot_writer,
    cache::SnapshotFileSet* previous_snapshot_files,
    const snapshot::Snapshot& new_snapshot)
  : engine_(engine),
    partitioner_(partitioner),
    snapshot_writer_(snapshot_writer),
    previous_snapshot_files_(previous_snapshot_files),
    new_snapshot_(new_snapshot) {
  ASSERT_ND(partitioner);
}

/**
 * @todo some of below should become a SortedBuffer method.
 */
struct StreamStatus {
  void init(snapshot::SortedBuffer* stream) {
    stream_ = stream;
    buffer_ = stream->get_buffer();
    buffer_size_ = stream->get_buffer_size();
    cur_absolute_pos_ = stream->get_cur_block_abosulte_begin();
    cur_relative_pos_ = stream->get_offset();
    end_absolute_pos_ = stream->get_cur_block_abosulte_end();
    ended_ = false;
    read_entry();
  }
  ErrorCode next() {
    ASSERT_ND(!ended_);
    cur_absolute_pos_ += cur_length_;
    cur_relative_pos_ += cur_length_;
    if (UNLIKELY(cur_absolute_pos_ >= end_absolute_pos_)) {
      ended_ = true;
      return kErrorCodeOk;
    } else if (UNLIKELY(cur_relative_pos_ >= buffer_size_)) {
      CHECK_ERROR_CODE(stream_->wind(cur_absolute_pos_));
      cur_relative_pos_ = stream_->to_relative_pos(cur_absolute_pos_);
    }
    read_entry();
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

SnapshotPagePointer SequentialComposer::to_snapshot_pointer(
  SnapshotLocalPageId local_id) const {
  return to_snapshot_page_pointer(
    snapshot_writer_->get_snapshot_id(),
    snapshot_writer_->get_numa_node(),
    local_id);
}

ErrorCode SequentialComposer::fix_and_dump(
  SequentialPage* first_unfixed_page,
  SequentialPage** cur_page) {
  // it's simple. we write out all pages we allocated.
  // replace page pointers with fixed ones
  memory::PagePoolOffset first_unfixed_offset = first_unfixed_page->header().page_id_;
  ASSERT_ND(first_unfixed_offset > 0);
  memory::PagePoolOffset upto_offset = snapshot_writer_->get_allocated_pages();
  ASSERT_ND(first_unfixed_offset < upto_offset);
  if (*cur_page) {
    --upto_offset;  // exclude the current page
    ASSERT_ND(snapshot_writer_->resolve(reinterpret_cast<Page*>(*cur_page)) == upto_offset);
  }

  CHECK_ERROR_CODE(snapshot_writer_->dump_pages(
    first_unfixed_offset,
    upto_offset - first_unfixed_offset));
  if (*cur_page) {
    memory::PagePoolOffset exclude_offset =
      snapshot_writer_->resolve(reinterpret_cast<Page*>(*cur_page));
    memory::PagePoolOffset new_offset = snapshot_writer_->reset_pool(&exclude_offset, 1);
    // now the only surviving page, cur_page, gets the new_offset.
    *cur_page = reinterpret_cast<SequentialPage*>(snapshot_writer_->resolve(new_offset));
  } else {
    snapshot_writer_->reset_pool(nullptr, 0);
  }
  return kErrorCodeOk;
}

SequentialPage* SequentialComposer::allocate_page(SnapshotPagePointer *next_allocated_page_id) {
  memory::PagePoolOffset offset = snapshot_writer_->allocate_new_page();
  SequentialPage* page = reinterpret_cast<SequentialPage*>(snapshot_writer_->resolve(offset));
  ASSERT_ND(page);
  page->initialize_data_page(partitioner_->get_storage_id(), *next_allocated_page_id);
  ++(*next_allocated_page_id);
  return page;
}

ErrorStack SequentialComposer::compose(
  snapshot::SortedBuffer* const* log_streams,
  uint32_t log_streams_count,
  const memory::AlignedMemorySlice& /*work_memory*/,
  Page* root_info_page) {
  debugging::StopWatch stop_watch;

  // this compose() emits just one pointer to the head page.
  std::memset(root_info_page, 0, sizeof(Page));
  RootInfoPage* root_info_page_casted = reinterpret_cast<RootInfoPage*>(root_info_page);
  root_info_page_casted->header_.storage_id_ = partitioner_->get_storage_id();
  root_info_page_casted->pointer_count_ = 1;

  // we write out all pages we allocate. much simpler than other storage types.
  // thus, we don't need a dedicated "fix" phase. we know the page ID after dumping already.
  // we pass around next_allocated_page_id for that.
  const SnapshotLocalPageId first_local_page_id = snapshot_writer_->get_dumped_pages();
  const SnapshotPagePointer first_page_id = to_snapshot_pointer(first_local_page_id);
  root_info_page_casted->pointers_[0] = first_page_id;
  SnapshotPagePointer next_allocated_page_id = first_page_id;
  SequentialPage* first_unfixed_page = allocate_page(&next_allocated_page_id);
  SequentialPage* cur_page = first_unfixed_page;

  VLOG(0) << to_string() << " composing with " << log_streams_count << " streams."
    << " first_page_id=" << assorted::Hex(first_page_id);

  for (uint32_t i = 0; i < log_streams_count; ++i) {
    StreamStatus status;
    status.init(log_streams[i]);
    const SequentialAppendLogType* entry = status.get_entry();

    // need to allocate a new page?
    if (!first_unfixed_page->can_insert_record(entry->payload_count_)) {
      // need to flush the buffer?
      if (snapshot_writer_->is_full()) {
        // dump everything except the current page
        WRAP_ERROR_CODE(fix_and_dump(first_unfixed_page, &cur_page));
        first_unfixed_page = cur_page;
      }

      // sequential storage is a bit special. As every page is written-once, we need only
      // snapshot pointer. No dual page pointers.
      SequentialPage* new_page = allocate_page(&next_allocated_page_id);
      ASSERT_ND(new_page->header().page_id_);
      ASSERT_ND(extract_numa_node_from_snapshot_pointer(new_page->header().page_id_)
          == snapshot_writer_->get_numa_node());
      ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(new_page->header().page_id_)
          == new_snapshot_.id_);
      cur_page->next_page().snapshot_pointer_ = new_page->header().page_id_;
      cur_page = new_page;
    }

    ASSERT_ND(cur_page->can_insert_record(entry->payload_count_));
    cur_page->append_record_nosync(status.cur_owner_id_, status.cur_length_, status.cur_payload_);

    // then, read next
    WRAP_ERROR_CODE(status.next());
    if (status.ended_) {
      break;
    }
  }
  // dump everything
  cur_page = nullptr;
  WRAP_ERROR_CODE(fix_and_dump(first_unfixed_page, &cur_page));

  stop_watch.stop();
  VLOG(0) << to_string() << " compose() done in " << stop_watch.elapsed_ms() << "ms. head page="
    << assorted::Hex(first_page_id);
  return kRetOk;
}

SequentialRootPage* SequentialComposer::allocate_root_page(
  SnapshotPagePointer *next_allocated_page_id) {
  memory::PagePoolOffset offset = snapshot_writer_->allocate_new_page();
  SequentialRootPage* page = reinterpret_cast<SequentialRootPage*>(
    snapshot_writer_->resolve(offset));
  ASSERT_ND(page);
  page->initialize_root_page(partitioner_->get_storage_id(), *next_allocated_page_id);
  ++(*next_allocated_page_id);
  return page;
}

ErrorStack SequentialComposer::construct_root(
  const Page* const* root_info_pages,
  uint32_t root_info_pages_count,
  const memory::AlignedMemorySlice& work_memory,
  SnapshotPagePointer* new_root_page_pointer) {
  debugging::StopWatch stop_watch;

  std::vector<SnapshotPagePointer> all_head_pages;
  Storage* storage = engine_->get_storage_manager().get_storage(partitioner_->get_storage_id());
  ASSERT_ND(storage);
  SnapshotPagePointer previous_root_page_pointer = storage->get_metadata()->root_snapshot_page_id_;
  for (SnapshotPagePointer page_id = previous_root_page_pointer; page_id != 0;) {
    // if there already is a root page, read them all.
    // we have to anyway re-write all of them, at least the next pointer.
    fs::DirectIoFile* file;
    WRAP_ERROR_CODE(previous_snapshot_files_->get_or_open_file(page_id, &file));
    SnapshotLocalPageId local_page_id = extract_local_page_id_from_snapshot_pointer(page_id);
    CHECK_ERROR(file->seek(local_page_id * sizeof(Page), fs::DirectIoFile::kDirectIoSeekSet));
    CHECK_ERROR(file->read(sizeof(Page), work_memory));
    SequentialRootPage* root_page = reinterpret_cast<SequentialRootPage*>(work_memory.get_block());
    ASSERT_ND(root_page->header().storage_id_ == partitioner_->get_storage_id());
    ASSERT_ND(root_page->header().page_id_ == page_id);
    for (uint16_t i = 0; i < root_page->get_pointer_count(); ++i) {
      all_head_pages.push_back(root_page->get_pointers()[i]);
    }
    page_id = root_page->get_next_page();
  }

  // each root_info_page contains just one pointer.
  for (uint32_t i = 0; i < root_info_pages_count; ++i) {
    const RootInfoPage* info_page = reinterpret_cast<const RootInfoPage*>(root_info_pages[i]);
    ASSERT_ND(info_page->header_.storage_id_ == partitioner_->get_storage_id());
    ASSERT_ND(info_page->pointer_count_ == 1);
    ASSERT_ND(info_page->pointers_[0] != 0);
    all_head_pages.push_back(info_page->pointers_[0]);
  }

  // now simply write out root pages that contain these pointers.
  const SnapshotLocalPageId new_root_local_page_id = snapshot_writer_->get_dumped_pages();
  *new_root_page_pointer = to_snapshot_pointer(new_root_local_page_id);
  SnapshotPagePointer next_allocated_page_id = *new_root_page_pointer;
  SequentialRootPage* cur_page = allocate_root_page(&next_allocated_page_id);
  uint16_t root_page_count = 1;
  for (uint32_t written_pointers = 0; written_pointers < all_head_pages.size();) {
    uint16_t count_in_this_page = std::min<uint64_t>(
      all_head_pages.size() - written_pointers,
      kRootPageMaxHeadPointers);
    cur_page->set_pointers(&all_head_pages[written_pointers], count_in_this_page);
    written_pointers += count_in_this_page;
    if (written_pointers < all_head_pages.size()) {
      // we need next page in root page.
      SequentialRootPage* new_page = allocate_root_page(&next_allocated_page_id);
      ASSERT_ND(new_page->header().page_id_);
      ASSERT_ND(extract_numa_node_from_snapshot_pointer(new_page->header().page_id_)
          == snapshot_writer_->get_numa_node());
      ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(new_page->header().page_id_)
          == new_snapshot_.id_);
      cur_page->set_next_page(new_page->header().page_id_);
      cur_page = new_page;
      ++root_page_count;
    } else {
      ASSERT_ND(written_pointers == all_head_pages.size());
    }
  }

  // write out the new root pages
  memory::PagePoolOffset from_offset = snapshot_writer_->get_allocated_pages() - root_page_count;
  WRAP_ERROR_CODE(snapshot_writer_->dump_pages(from_offset, root_page_count));
  ASSERT_ND(snapshot_writer_->get_dumped_pages() == new_root_local_page_id + root_page_count);
  snapshot_writer_->reset_pool(nullptr, 0);

  stop_watch.stop();
  VLOG(0) << to_string() << " construct_root() done in " << stop_watch.elapsed_us() << "us."
    << " total head page pointers=" << all_head_pages.size()
    << ". new root head page=" << assorted::Hex(*new_root_page_pointer)
    << ". root_page_count=" << root_page_count;
  return kRetOk;
}

void SequentialComposer::describe(std::ostream* o_ptr) const {
  std::ostream &o = *o_ptr;
  o << "<SequentialComposer>"
      << "<partitioner_>" << partitioner_ << "</partitioner_>"
      << "<snapshot_writer_>" << snapshot_writer_ << "</snapshot_writer_>"
      << "<new_snapshot>" << new_snapshot_ << "</new_snapshot>"
    << "</SequentialComposer>";
}

std::string SequentialComposer::to_string() const {
  return std::string("SequentialComposer:storage-")
    + std::to_string(partitioner_->get_storage_id());
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
