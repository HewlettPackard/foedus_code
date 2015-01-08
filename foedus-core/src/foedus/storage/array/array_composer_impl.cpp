/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_composer_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <ostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/page_resolver.hpp"
#include "foedus/snapshot/merge_sort.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/storage/array/array_partitioner_impl.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/array_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace array {

///////////////////////////////////////////////////////////////////////
///
///  ArrayComposer methods
///
///////////////////////////////////////////////////////////////////////
ArrayComposer::ArrayComposer(Composer *parent)
  : engine_(parent->get_engine()),
    storage_id_(parent->get_storage_id()),
    storage_(engine_, storage_id_) {
  ASSERT_ND(storage_.exists());
}

ErrorStack ArrayComposer::compose(const Composer::ComposeArguments& args) {
  VLOG(0) << to_string() << " composing with " << args.log_streams_count_ << " streams.";
  debugging::StopWatch stop_watch;

  snapshot::MergeSort merge_sort(
    storage_id_,
    kArrayStorage,
    args.base_epoch_,
    args.log_streams_,
    args.log_streams_count_,
    kMaxLevels,
    args.work_memory_);
  CHECK_ERROR(merge_sort.initialize());

  ArrayComposeContext context(
    engine_,
    &merge_sort,
    args.snapshot_writer_,
    args.previous_snapshot_files_,
    args.root_info_page_);
  CHECK_ERROR(context.execute());

  CHECK_ERROR(merge_sort.uninitialize());  // no need for scoped release. its destructor is safe.

  stop_watch.stop();
  LOG(INFO) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}

ErrorStack ArrayComposer::construct_root(const Composer::ConstructRootArguments& args) {
  // compose() created root_info_pages that contain pointers to fill in the root page,
  // so we just find non-zero entry and copy it to root page.
  uint8_t levels = storage_.get_levels();
  uint16_t payload_size = storage_.get_payload_size();
  snapshot::SnapshotId new_snapshot_id = args.snapshot_writer_->get_snapshot_id();
  if (levels == 1U) {
    // if it's single-page array, we have already created the root page in compose().
    ASSERT_ND(args.root_info_pages_count_ == 1U);
    const ArrayRootInfoPage* casted
      = reinterpret_cast<const ArrayRootInfoPage*>(args.root_info_pages_[0]);
    ASSERT_ND(casted->pointers_[0] != 0);
    *args.new_root_page_pointer_ = casted->pointers_[0];

    // and we have already installed it, right?
    ASSERT_ND(storage_.get_control_block()->meta_.root_snapshot_page_id_
      == casted->pointers_[0]);
    ASSERT_ND(storage_.get_control_block()->root_page_pointer_.snapshot_pointer_
      == casted->pointers_[0]);
  } else {
    ArrayPage* root_page = reinterpret_cast<ArrayPage*>(args.snapshot_writer_->get_page_base());
    SnapshotPagePointer page_id = storage_.get_metadata()->root_snapshot_page_id_;
    SnapshotPagePointer new_page_id = args.snapshot_writer_->get_next_page_id();
    *args.new_root_page_pointer_ = new_page_id;
    if (page_id != 0) {
      WRAP_ERROR_CODE(args.previous_snapshot_files_->read_page(page_id, root_page));
      ASSERT_ND(root_page->header().storage_id_ == storage_id_);
      ASSERT_ND(root_page->header().page_id_ == page_id);
      root_page->header().page_id_ = new_page_id;
    } else {
      uint64_t root_interval = LookupRouteFinder(levels, payload_size).get_records_in_leaf();
      for (uint8_t level = 1; level < levels; ++level) {
        root_interval *= kInteriorFanout;
      }
      ArrayRange range(0, root_interval, storage_.get_array_size());
      root_page->initialize_snapshot_page(
        storage_id_,
        new_page_id,
        payload_size,
        levels - 1,
        range);
    }

    // overwrite pointers with root_info_pages.
    for (uint32_t i = 0; i < args.root_info_pages_count_; ++i) {
      const ArrayRootInfoPage* casted
        = reinterpret_cast<const ArrayRootInfoPage*>(args.root_info_pages_[i]);
      for (uint16_t j = 0; j < kInteriorFanout; ++j) {
        SnapshotPagePointer pointer = casted->pointers_[j];
        if (pointer != 0) {
          ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(pointer) == new_snapshot_id);
          DualPagePointer& record = root_page->get_interior_record(j);
          // partitioning has no overlap, so this must be the only overwriting pointer
          ASSERT_ND(record.snapshot_pointer_ == 0 ||
            extract_snapshot_id_from_snapshot_pointer(record.snapshot_pointer_)
              != new_snapshot_id);
          record.snapshot_pointer_ = pointer;
        }
      }
    }

    WRAP_ERROR_CODE(args.snapshot_writer_->dump_pages(0, 1));
    ASSERT_ND(args.snapshot_writer_->get_next_page_id() == new_page_id + 1ULL);
    // AFTER writing out the root page, install the pointer to new root page
    storage_.get_control_block()->root_page_pointer_.snapshot_pointer_ = new_page_id;
    storage_.get_control_block()->meta_.root_snapshot_page_id_ = new_page_id;
  }
  return kRetOk;
}


std::string ArrayComposer::to_string() const {
  return std::string("ArrayComposer-") + std::to_string(storage_id_);
}

///////////////////////////////////////////////////////////////////////
///
///  ArrayComposeContext methods
///
///////////////////////////////////////////////////////////////////////
ArrayComposeContext::ArrayComposeContext(
  Engine*                           engine,
  snapshot::MergeSort*              merge_sort,
  snapshot::SnapshotWriter*         snapshot_writer,
  cache::SnapshotFileSet*           previous_snapshot_files,
  Page*                             root_info_page)
  : engine_(engine),
    merge_sort_(merge_sort),
    storage_id_(merge_sort_->get_storage_id()),
    snapshot_id_(snapshot_writer->get_snapshot_id()),
    storage_(engine, storage_id_),
    snapshot_writer_(snapshot_writer),
    previous_snapshot_files_(previous_snapshot_files),
    root_info_page_(reinterpret_cast<ArrayRootInfoPage*>(root_info_page)),
    payload_size_(storage_.get_payload_size()),
    levels_(storage_.get_levels()),
    previous_root_page_pointer_(storage_.get_metadata()->root_snapshot_page_id_) {
  LookupRouteFinder route_finder(levels_, payload_size_);
  offset_intervals_[0] = route_finder.get_records_in_leaf();
  for (uint8_t level = 1; level < levels_; ++level) {
    offset_intervals_[level] = offset_intervals_[level - 1] * kInteriorFanout;
  }
  std::memset(cur_path_, 0, sizeof(cur_path_));

  allocated_pages_ = 0;
  allocated_intermediates_ = 0;
  page_base_ = reinterpret_cast<ArrayPage*>(snapshot_writer_->get_page_base());
  max_pages_ = snapshot_writer_->get_page_size();
  intermediate_base_ = reinterpret_cast<ArrayPage*>(snapshot_writer_->get_intermediate_base());
  max_intermediates_ = snapshot_writer_->get_intermediate_size();

  PartitionerMetadata* metadata = PartitionerMetadata::get_metadata(engine_, storage_id_);
  ASSERT_ND(metadata->valid_);  // otherwise composer invoked without partitioner. maybe testcase?
  partitioning_data_ = reinterpret_cast<ArrayPartitionerData*>(metadata->locate_data(engine_));
  ASSERT_ND(levels_ == partitioning_data_->array_levels_);
  ASSERT_ND(storage_.get_array_size() == partitioning_data_->array_size_);
}

ErrorStack ArrayComposeContext::execute() {
  std::memset(root_info_page_, 0, kPageSize);
  root_info_page_->header_.storage_id_ = storage_id_;

  if (levels_ <= 1U) {
    // this storage has only one page. This is very special and trivial.
    // we process this case separately.
    return execute_single_level_array();
  }

  // This implementation is batch-based to make it significantly more efficient.
  // We receive a fully-sorted/integrated stream of logs from merge_sort_, and merge them with
  // previous snapshot on page-basis. Most pages have many logs, so this achieves a tight loop
  // without expensive cost to switch pages.
  bool processed_any = false;
  while (true) {
    CHECK_ERROR(merge_sort_->next_batch());
    uint64_t count = merge_sort_->get_current_count();
    if (count == 0 && merge_sort_->is_ended_all()) {
      break;
    }
    const snapshot::MergeSort::SortEntry* sort_entries = merge_sort_->get_sort_entries();
    if (!processed_any) {
      // this is the first log. let's initialize cur_path to this log.
      processed_any = true;
      ArrayOffset initial_offset = sort_entries[0].get_key();

      CHECK_ERROR(initialize(initial_offset));
    }

    uint64_t cur = 0;
    while (cur < count) {
      const ArrayCommonUpdateLogType* head = reinterpret_cast<const ArrayCommonUpdateLogType*>(
        merge_sort_->resolve_sort_position(cur));
      ArrayOffset head_offset = head->offset_;
      ASSERT_ND(head_offset == sort_entries[cur].get_key());
      // switch to a page containing this offset
      WRAP_ERROR_CODE(update_cur_path(head_offset));
      ArrayRange page_range = cur_path_[0]->get_array_range();
      ASSERT_ND(page_range.contains(head_offset));

      // grab a range of logs that are in the same page.
      uint64_t next;
      for (next = cur + 1U; LIKELY(next < count); ++next) {
        // this check uses sort_entries which are nicely contiguous.
        ArrayOffset offset = sort_entries[next].get_key();
        ASSERT_ND(offset >= page_range.begin_);
        if (UNLIKELY(offset >= page_range.end_)) {
          break;
        }
      }

      apply_batch(cur, next);
      cur = next;
    }
    ASSERT_ND(cur == count);
  }

  if (processed_any) {
    CHECK_ERROR(finalize());
  } else {
    LOG(ERROR) << "wtf? no logs? storage-" << storage_id_;
  }
  return kRetOk;
}

void ArrayComposeContext::apply_batch(uint64_t cur, uint64_t next) {
  const uint16_t kFetchSize = 8;
  const log::RecordLogType* logs[kFetchSize];
  ArrayPage* leaf = cur_path_[0];
  ArrayRange range = leaf->get_array_range();
  while (cur < next) {
    uint16_t desired = std::min<uint16_t>(kFetchSize, next - cur);
    uint16_t fetched = merge_sort_->fetch_logs(cur, desired, logs);
    for (uint16_t i = 0; i < kFetchSize && LIKELY(i < fetched); ++i) {
      const ArrayCommonUpdateLogType* log
        = reinterpret_cast<const ArrayCommonUpdateLogType*>(logs[i]);
      ASSERT_ND(range.contains(log->offset_));
      uint16_t index = log->offset_ - range.begin_;
      Record* record = leaf->get_leaf_record(index, payload_size_);
      if (log->header_.get_type() == log::kLogCodeArrayOverwrite) {
        const ArrayOverwriteLogType* casted
          = reinterpret_cast<const ArrayOverwriteLogType*>(log);
        casted->apply_record(nullptr, storage_id_, &record->owner_id_, record->payload_);
      } else {
        ASSERT_ND(log->header_.get_type() == log::kLogCodeArrayIncrement);
        const ArrayIncrementLogType* casted
          = reinterpret_cast<const ArrayIncrementLogType*>(log);
        casted->apply_record(nullptr, storage_id_, &record->owner_id_, record->payload_);
      }
    }
    cur += fetched;
    ASSERT_ND(cur <= next);
  }
}

ErrorStack ArrayComposeContext::execute_single_level_array() {
  // no page-switch in this case
  ArrayRange range(0, storage_.get_array_size());
  // single-page array. root is a leaf page.
  cur_path_[0] = page_base_;
  SnapshotPagePointer page_id = snapshot_writer_->get_next_page_id();
  ASSERT_ND(allocated_pages_ == 0);
  allocated_pages_ = 1;
  WRAP_ERROR_CODE(read_or_init_page(previous_root_page_pointer_, page_id, 0, range, cur_path_[0]));

  while (true) {
    CHECK_ERROR(merge_sort_->next_batch());
    uint64_t count = merge_sort_->get_current_count();
    if (count == 0 && merge_sort_->is_ended_all()) {
      break;
    }

    apply_batch(0, count);
  }

  ASSERT_ND(allocated_pages_ == 1U);
  ASSERT_ND(allocated_intermediates_ == 0);
  ASSERT_ND(cur_path_[0] == page_base_);
  ASSERT_ND(page_id == page_base_[0].header().page_id_);
  ASSERT_ND(snapshot_writer_->get_next_page_id() == page_id);
  WRAP_ERROR_CODE(snapshot_writer_->dump_pages(0, 1));
  root_info_page_->pointers_[0] = page_id;

  // further, we install the only snapshot pointer now.
  storage_.get_control_block()->meta_.root_snapshot_page_id_ = page_id;
  storage_.get_control_block()->root_page_pointer_.snapshot_pointer_ = page_id;
  return kRetOk;
}

ErrorStack ArrayComposeContext::finalize() {
  ASSERT_ND(levels_ > 1U);

  ArrayRange last_range = cur_path_[0]->get_array_range();
  if (is_initial_snapshot() && last_range.end_ < storage_.get_array_size()) {
    VLOG(0) << "Need to fill out empty pages in initial snapshot of array-" << storage_id_
      << ", from " << last_range.end_ << " to the end of array";
    WRAP_ERROR_CODE(create_empty_pages(last_range.end_, storage_.get_array_size()));
  }

  // flush the main buffer. now we finalized all leaf pages
  if (allocated_pages_ > 0) {
    WRAP_ERROR_CODE(dump_leaf_pages());
    ASSERT_ND(allocated_pages_ == 0);
  }

  // intermediate pages are different animals.
  // we store them in a separate buffer, and now finally we can get their page IDs.
  // Until now, we relative used indexes in intermediate buffer as page ID, storing them in
  // page ID header. now let's convert all of them to be final page ID.
  ArrayPage* root_page = intermediate_base_;
  ASSERT_ND(root_page == cur_path_[levels_ - 1]);
  ASSERT_ND(root_page->get_level() == levels_ - 1);
  ASSERT_ND(root_page->header().page_id_ == 0);  // this is the only page that has page-id 0

  // base_pointer + offset in intermediate buffer will be the new page ID.
  const SnapshotPagePointer base_pointer = snapshot_writer_->get_next_page_id();
  root_page->header().page_id_ = base_pointer;
  for (uint32_t i = 1; i < allocated_intermediates_; ++i) {
    SnapshotPagePointer new_page_id = base_pointer + i;
    ArrayPage* page = intermediate_base_ + i;
    ASSERT_ND(page->header().page_id_ == i);
    ASSERT_ND(page->get_level() > 0);
    ASSERT_ND(page->get_level() < levels_ - 1U);
    page->header().page_id_ = new_page_id;
    if (page->get_level() > 1U) {
      // also updates pointers to new children.
      // we can tell whether the pointer is created during this snapshot by seeing the page ID.
      // we used the relative index (1~allocated_intermediates_-1) as pointer, which means
      // they have 0 (kNullSnapshotId) as snapshot ID. Thus, if there is a non-null pointer whose
      // snapshot-Id is 0, that's a pointer we have created here.
      for (uint16_t j = 0; j < kInteriorFanout; ++j) {
        DualPagePointer& pointer = page->get_interior_record(j);
        ASSERT_ND(pointer.volatile_pointer_.is_null());
        SnapshotPagePointer page_id = pointer.snapshot_pointer_;
        snapshot::SnapshotId snapshot_id = extract_snapshot_id_from_snapshot_pointer(page_id);
        ASSERT_ND(snapshot_id != snapshot_id_);
        if (page_id != 0 && snapshot_id == snapshot::kNullSnapshotId) {
          ASSERT_ND(extract_numa_node_from_snapshot_pointer(page_id) == 0);
          ASSERT_ND(page_id < allocated_intermediates_);
          pointer.snapshot_pointer_ = base_pointer + page_id;
          ASSERT_ND(verify_snapshot_pointer(pointer.snapshot_pointer_));
        }
      }
    }
  }

  // we also write out root page, but we don't use it as we just put an equivalent information to
  // root_info_page. construct_root() will combine all composers' output later.
  snapshot_writer_->dump_intermediates(0, allocated_intermediates_);
  for (uint16_t j = 0; j < kInteriorFanout; ++j) {
    DualPagePointer& pointer = root_page->get_interior_record(j);
    ASSERT_ND(pointer.volatile_pointer_.is_null());
    SnapshotPagePointer page_id = pointer.snapshot_pointer_;
    snapshot::SnapshotId snapshot_id = extract_snapshot_id_from_snapshot_pointer(page_id);
    if (page_id != 0) {
      if (snapshot_id == snapshot_id_) {
        // we already have snapshot pointers because it points to leaf pages. (2 level array)
        ASSERT_ND(extract_numa_node_from_snapshot_pointer(page_id)
          == snapshot_writer_->get_numa_node());
        ASSERT_ND(root_page->get_level() == 1U);
        ASSERT_ND(verify_snapshot_pointer(pointer.snapshot_pointer_));
        root_info_page_->pointers_[j] = pointer.snapshot_pointer_;
      } else if (snapshot_id == snapshot::kNullSnapshotId) {
        // intermediate pages created in this snapshot
        ASSERT_ND(root_page->get_level() > 1U);
        ASSERT_ND(extract_numa_node_from_snapshot_pointer(page_id) == 0);
        ASSERT_ND(page_id < allocated_intermediates_);
        pointer.snapshot_pointer_ = base_pointer + page_id;
        ASSERT_ND(verify_snapshot_pointer(pointer.snapshot_pointer_));
        root_info_page_->pointers_[j] = pointer.snapshot_pointer_;
      } else {
        // then, it's a page in some previous snapshots created in some node. skip
      }
    }
  }


  // AFTER durably writing out the intermediate pages to the file, we install snapshot pointers.
  uint64_t installed_count = 0;
  CHECK_ERROR(install_snapshot_pointers(base_pointer, &installed_count));

  return kRetOk;
}

ErrorStack ArrayComposeContext::initialize(ArrayOffset initial_offset) {
  ASSERT_ND(allocated_intermediates_ == 0);
  ASSERT_ND(allocated_pages_ == 0);
  ASSERT_ND(levels_ > 1U);

  // First, load or create the root page.
  CHECK_ERROR(init_root_page());

  // If an initial snapshot, we might have to create empty pages that received no logs.
  if (is_initial_snapshot()) {
    ArrayRange leaf_range = to_leaf_range(initial_offset);
    if (leaf_range.begin_ != 0) {
      VLOG(0) << "Need to fill out empty pages in initial snapshot of array-" << storage_id_
        << ", upto " << leaf_range.begin_;
      WRAP_ERROR_CODE(create_empty_pages(0, leaf_range.begin_));
    }
  }
  return kRetOk;
}

ErrorStack ArrayComposeContext::init_root_page() {
  uint8_t level = levels_ - 1U;
  ASSERT_ND(level > 0);
  ArrayPage* page = intermediate_base_;
  ArrayRange range(0, storage_.get_array_size());
  ASSERT_ND(allocated_intermediates_ == 0);
  allocated_intermediates_ = 1;

  WRAP_ERROR_CODE(read_or_init_page(previous_root_page_pointer_, 0, level, range, page));
  cur_path_[level] = page;
  return kRetOk;
}

ErrorCode ArrayComposeContext::create_empty_pages(ArrayOffset from, ArrayOffset to) {
  ASSERT_ND(is_initial_snapshot());  // this must be called only at initial snapshot
  ASSERT_ND(levels_ > 1U);  // single-page array is handled separately, and no need for this func.
  ASSERT_ND(from < to);
  ASSERT_ND(to <= storage_.get_array_size());
  ArrayPage* page = cur_path_[levels_ - 1U];
  ASSERT_ND(page);

  // This composer only takes care of partition-<node>. We must fill empty pages only in
  // the assigned subtrees.
  PartitionId partition = snapshot_writer_->get_numa_node();
  ASSERT_ND(partitioning_data_);
  ASSERT_ND(!partitioning_data_->partitionable_
    || partitioning_data_->bucket_size_ == offset_intervals_[levels_ - 2U]);

  // basically same flow as create_empty_pages_recurse, but we skip other partitions.
  // in lower levels, we don't have to worry about partitioning. The subtrees are solely ours.
  const uint8_t child_level = levels_ - 2U;
  const uint64_t interval = offset_intervals_[child_level];
  const uint16_t first_child = from / interval;  // floor

  // the followings are ceil because right-most might be partial.
  uint16_t children = assorted::int_div_ceil(storage_.get_array_size(), interval);
  if (interval * children > to) {
    children = assorted::int_div_ceil(to, interval);
  }
  ASSERT_ND(children <= kInteriorFanout);

  for (uint16_t i = first_child; i < children; ++i) {
    if (partitioning_data_->partitionable_ && partitioning_data_->bucket_owners_[i] != partition) {
      continue;
    }

    // are we filling out empty pages in order?
    ASSERT_ND(i == first_child || page->get_interior_record(i).snapshot_pointer_ == 0);

    ArrayRange child_range(i * interval, (i + 1U) * interval, storage_.get_array_size());
    if (page->get_interior_record(i).snapshot_pointer_ == 0) {
      if (child_level > 0) {
        CHECK_ERROR_CODE(create_empty_intermediate_page(page, i, child_range));
      } else {
        CHECK_ERROR_CODE(create_empty_leaf_page(page, i, child_range));
      }
    }

    ASSERT_ND(cur_path_[child_level]);
    if (child_level > 0) {
      CHECK_ERROR_CODE(create_empty_pages_recurse(from, to, cur_path_[child_level]));
    }
  }
  return kErrorCodeOk;
}

ErrorCode ArrayComposeContext::create_empty_pages_recurse(
  ArrayOffset from,
  ArrayOffset to,
  ArrayPage* page) {
  const uint8_t cur_level = page->get_level();
  ASSERT_ND(cur_level > 0);
  ArrayRange page_range = page->get_array_range();
  ASSERT_ND(page_range.begin_ < to);
  ASSERT_ND(from < page_range.end_);

  const uint8_t child_level = cur_level - 1U;
  const uint64_t interval = offset_intervals_[child_level];
  ASSERT_ND(page_range.end_ == page_range.begin_ + interval * kInteriorFanout
    || (page_range.begin_ + interval * kInteriorFanout > storage_.get_array_size()
        && page_range.end_ == storage_.get_array_size()));

  uint16_t first_child = 0;
  if (from > page_range.begin_) {
    ASSERT_ND(from < page_range.begin_ + interval * kInteriorFanout);
    first_child = (from - page_range.begin_) / interval;  // floor. left-most might be partial.
  }

  // the followings are ceil because right-most might be partial.
  uint16_t children = assorted::int_div_ceil(page_range.end_ - page_range.begin_, interval);
  if (page_range.begin_ + interval * children > to) {
    children = assorted::int_div_ceil(to - page_range.begin_, interval);
  }
  ASSERT_ND(children <= kInteriorFanout);

  // we assume this method is called in order, thus null-pointer means the pages we should fill out.
  for (uint16_t i = first_child; i < children; ++i) {
    ASSERT_ND(i == first_child || page->get_interior_record(i).snapshot_pointer_ == 0);
    ArrayRange child_range(
      page_range.begin_ + i * interval,
      page_range.begin_ + (i + 1U) * interval,
      page_range.end_);

    if (page->get_interior_record(i).snapshot_pointer_ == 0) {
      if (child_level > 0) {
        CHECK_ERROR_CODE(create_empty_intermediate_page(page, i, child_range));
      } else {
        CHECK_ERROR_CODE(create_empty_leaf_page(page, i, child_range));
      }
    }

    ASSERT_ND(cur_path_[child_level]);
    if (child_level > 0) {
      CHECK_ERROR_CODE(create_empty_pages_recurse(from, to, cur_path_[child_level]));
    }
  }

  return kErrorCodeOk;
}

ErrorCode ArrayComposeContext::dump_leaf_pages() {
  CHECK_ERROR_CODE(snapshot_writer_->dump_pages(0, allocated_pages_));
  ASSERT_ND(snapshot_writer_->get_next_page_id()
    == page_base_[0].header().page_id_ + allocated_pages_);
  ASSERT_ND(snapshot_writer_->get_next_page_id()
    == page_base_[allocated_pages_ - 1].header().page_id_ + 1ULL);
  allocated_pages_ = 0;
  return kErrorCodeOk;
}

ErrorCode ArrayComposeContext::create_empty_intermediate_page(
  ArrayPage* parent,
  uint16_t index,
  ArrayRange range) {
  ASSERT_ND(parent->get_level() > 1U);
  DualPagePointer& pointer = parent->get_interior_record(index);
  ASSERT_ND(pointer.is_both_null());
  uint8_t level = parent->get_level() - 1U;
  ArrayPage* page = intermediate_base_ + allocated_intermediates_;
  SnapshotPagePointer new_page_id = allocated_intermediates_;
  ++allocated_intermediates_;
  CHECK_ERROR_CODE(read_or_init_page(0, new_page_id, level, range, page));

  cur_path_[level] = page;
  return kErrorCodeOk;
}

ErrorCode ArrayComposeContext::create_empty_leaf_page(
  ArrayPage* parent,
  uint16_t index,
  ArrayRange range) {
  ASSERT_ND(parent->get_level() == 1U);
  DualPagePointer& pointer = parent->get_interior_record(index);
  ASSERT_ND(pointer.is_both_null());
  if (allocated_pages_ >= max_pages_) {
    CHECK_ERROR_CODE(dump_leaf_pages());
    ASSERT_ND(allocated_pages_ == 0);
  }

  // remember, we can finalize the page ID of leaf pages at this point
  ArrayPage* page = page_base_ + allocated_pages_;
  SnapshotPagePointer new_page_id = snapshot_writer_->get_next_page_id() + allocated_pages_;
  ASSERT_ND(verify_snapshot_pointer(new_page_id));
  ++allocated_pages_;
  CHECK_ERROR_CODE(read_or_init_page(0, new_page_id, 0, range, page));
  pointer.snapshot_pointer_ = new_page_id;

  cur_path_[0] = page;
  return kErrorCodeOk;
}

ErrorCode ArrayComposeContext::update_cur_path(ArrayOffset next_offset) {
  ASSERT_ND(levels_ > 1U);
  ASSERT_ND(cur_path_[0] == nullptr || next_offset >= cur_path_[0]->get_array_range().begin_);
  if (cur_path_[0] != nullptr && next_offset < cur_path_[0]->get_array_range().end_) {
    // already in the page. this usually doesn't happen as we batch-apply as many as possible,
    // but might happen when logs for the same page are on a boundary of windows.
    return kErrorCodeOk;
  }

  ArrayRange next_range = to_leaf_range(next_offset);
  ArrayOffset jump_from = cur_path_[0] == nullptr ? 0 : cur_path_[0]->get_array_range().end_;
  ArrayOffset jump_to = next_range.begin_;
  if (jump_to > jump_from && is_initial_snapshot()) {
    VLOG(0) << "Need to fill out empty pages in initial snapshot of array-" << storage_id_
      << ", from " << jump_from << " to " << jump_to;
    CHECK_ERROR_CODE(create_empty_pages(jump_from, jump_to));
  }

  // then switch pages. we might have to switch parent pages, too.
  ASSERT_ND(cur_path_[levels_ - 1U]->get_array_range().contains(next_offset));
  for (uint8_t level = levels_ - 2U; level < kMaxLevels; --level) {  // note, unsigned.
    // we don't care changes in route[0] (record ordinals in page)
    if (cur_path_[level] != nullptr && cur_path_[level]->get_array_range().contains(next_offset)) {
      // skip non-changed path. most likely only the leaf has changed.
      continue;
    }

    // page switched! we have to allocate a new page and point to it.
    ArrayPage* parent = cur_path_[level + 1U];
    ArrayRange parent_range = parent->get_array_range();
    ASSERT_ND(parent_range.contains(next_offset));
    uint64_t interval = offset_intervals_[level];
    uint16_t i = (next_offset - parent_range.begin_) / interval;
    ASSERT_ND(i < kInteriorFanout);

    ArrayRange child_range(
      parent_range.begin_ + i * interval,
      parent_range.begin_ + (i + 1U) * interval,
      parent_range.end_);

    DualPagePointer& pointer = parent->get_interior_record(i);
    ASSERT_ND(pointer.volatile_pointer_.is_null());
    SnapshotPagePointer old_page_id = pointer.snapshot_pointer_;
    ASSERT_ND((!is_initial_snapshot() && old_page_id != 0)
      || (is_initial_snapshot() && old_page_id == 0));

    ArrayPage* page;
    if (level > 0U) {
      // we switched an intermediate page
      page = intermediate_base_ + allocated_intermediates_;
      SnapshotPagePointer new_page_id = allocated_intermediates_;
      ++allocated_intermediates_;
      CHECK_ERROR_CODE(read_or_init_page(old_page_id, new_page_id, level, child_range, page));
    } else {
      // we switched a leaf page. in this case, we might have to flush the buffer
      if (allocated_pages_ >= max_pages_) {
        CHECK_ERROR_CODE(dump_leaf_pages());
        ASSERT_ND(allocated_pages_ == 0);
      }

      // remember, we can finalize the page ID of leaf pages at this point
      page = page_base_ + allocated_pages_;
      SnapshotPagePointer new_page_id = snapshot_writer_->get_next_page_id() + allocated_pages_;
      ASSERT_ND(verify_snapshot_pointer(new_page_id));
      ++allocated_pages_;
      CHECK_ERROR_CODE(read_or_init_page(old_page_id, new_page_id, level, child_range, page));
      pointer.snapshot_pointer_ = new_page_id;
    }
    cur_path_[level] = page;
  }
  ASSERT_ND(verify_cur_path());
  return kErrorCodeOk;
}

inline ErrorCode ArrayComposeContext::read_or_init_page(
  SnapshotPagePointer old_page_id,
  SnapshotPagePointer new_page_id,
  uint8_t level,
  ArrayRange range,
  ArrayPage* page) {
  ASSERT_ND(new_page_id != 0 || level == levels_ - 1U);
  if (old_page_id != 0) {
    ASSERT_ND(!is_initial_snapshot());
    CHECK_ERROR_CODE(previous_snapshot_files_->read_page(old_page_id, page));
    ASSERT_ND(page->header().storage_id_ == storage_id_);
    ASSERT_ND(page->header().page_id_ == old_page_id);
    ASSERT_ND(page->get_level() == level);
    ASSERT_ND(page->get_array_range() == range);
    page->header().page_id_ = new_page_id;
  } else {
    ASSERT_ND(is_initial_snapshot());
    page->initialize_snapshot_page(
      storage_id_,
      new_page_id,
      payload_size_,
      level,
      range);
  }
  return kErrorCodeOk;
}

bool ArrayComposeContext::verify_cur_path() const {
  for (uint8_t level = 0; level < kMaxLevels; ++level) {
    if (level >= levels_) {
      ASSERT_ND(cur_path_[level] == nullptr);
      continue;
    }
    ASSERT_ND(cur_path_[level]);
    ASSERT_ND(cur_path_[level]->get_level() == level);
    ASSERT_ND(cur_path_[level]->get_storage_id() == storage_id_);
  }
  return true;
}

bool ArrayComposeContext::verify_snapshot_pointer(SnapshotPagePointer pointer) {
  ASSERT_ND(extract_local_page_id_from_snapshot_pointer(pointer) > 0U);
  if (!engine_->is_master()) {
    ASSERT_ND(extract_numa_node_from_snapshot_pointer(pointer)
      == snapshot_writer_->get_numa_node());
  }
  ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(pointer)
    == snapshot_writer_->get_snapshot_id());
  return true;
}

///////////////////////////////////////////////////////////////////////
///
///  ArrayComposeContext::install_snapshot_pointers() related methods
///
///////////////////////////////////////////////////////////////////////
ErrorStack ArrayComposeContext::install_snapshot_pointers(
  SnapshotPagePointer snapshot_base,
  uint64_t* installed_count) const {
  ASSERT_ND(levels_ > 1U);  // no need to call this method in one-page array
  ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(snapshot_base) == snapshot_id_);

  *installed_count = 0;
  VolatilePagePointer pointer = storage_.get_control_block()->root_page_pointer_.volatile_pointer_;
  if (pointer.is_null()) {
    VLOG(0) << "No volatile pages.. maybe while restart?";
    return kRetOk;
  }

  const memory::GlobalVolatilePageResolver& resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  ArrayPage* volatile_root = reinterpret_cast<ArrayPage*>(resolver.resolve_offset(pointer));

  // compared to masstree, array is much easier to install snapshot pointers because the
  // shape of the tree is exactly same between volatile and snapshot.
  // we just recurse with the corresponding snapshot and volatile pages.
  debugging::StopWatch watch;
  const ArrayPage* snapshot_root = intermediate_base_;
  WRAP_ERROR_CODE(install_snapshot_pointers_recurse(
    snapshot_base,
    resolver,
    snapshot_root,
    volatile_root,
    installed_count));
  watch.stop();
  VLOG(0) << "ArrayStorage-" << storage_id_ << " installed " << *installed_count << " pointers"
    << " in " << watch.elapsed_ms() << "ms";
  return kRetOk;
}

ErrorCode ArrayComposeContext::install_snapshot_pointers_recurse(
  SnapshotPagePointer snapshot_base,
  const memory::GlobalVolatilePageResolver& resolver,
  const ArrayPage* snapshot_page,
  ArrayPage* volatile_page,
  uint64_t* installed_count) const {
  ASSERT_ND(snapshot_page->get_array_range() == volatile_page->get_array_range());
  ASSERT_ND(!snapshot_page->is_leaf());
  ASSERT_ND(!volatile_page->is_leaf());
  const bool needs_recursion = snapshot_page->get_level() > 1U;
  for (uint16_t i = 0; i < kInteriorFanout; ++i) {
    SnapshotPagePointer pointer = snapshot_page->get_interior_record(i).snapshot_pointer_;
    if (pointer == 0) {
      continue;  // either this is right-most page or the range is not in this partition
    }
    snapshot::SnapshotId snapshot_id = extract_snapshot_id_from_snapshot_pointer(pointer);
    ASSERT_ND(snapshot_id != snapshot::kNullSnapshotId);
    if (snapshot_id != snapshot_id_) {
      continue;
    }
    ASSERT_ND(extract_numa_node_from_snapshot_pointer(pointer)
      == snapshot_writer_->get_numa_node());
    DualPagePointer& target = volatile_page->get_interior_record(i);
    target.snapshot_pointer_ = pointer;
    ++(*installed_count);

    if (needs_recursion) {
      ASSERT_ND(pointer > snapshot_base);
      // if it has a volatile page, further recurse.
      VolatilePagePointer volatile_pointer = target.volatile_pointer_;
      if (!volatile_pointer.is_null()) {
        ArrayPage* volatile_next
          = reinterpret_cast<ArrayPage*>(resolver.resolve_offset(volatile_pointer));
        uint64_t offset = pointer - snapshot_base;
        const ArrayPage* snapshot_next = intermediate_base_ + offset;
        CHECK_ERROR_CODE(install_snapshot_pointers_recurse(
          snapshot_base,
          resolver,
          snapshot_next,
          volatile_next,
          installed_count));
      }
    }
  }
  return kErrorCodeOk;
}


/////////////////////////////////////////////////////////////////////////////
///
///  drop_volatiles and related methods
///
/////////////////////////////////////////////////////////////////////////////
bool ArrayComposer::drop_volatiles(const Composer::DropVolatilesArguments& args) {
  if (storage_.get_array_metadata()->keeps_all_volatile_pages()) {
    LOG(INFO) << "Keep-all-volatile: Storage-" << storage_.get_name()
      << " is configured to keep all volatile pages.";
    return false;
  }

  DualPagePointer* root_pointer = &storage_.get_control_block()->root_page_pointer_;
  ArrayPage* volatile_page = resolve_volatile(root_pointer->volatile_pointer_);
  if (volatile_page == nullptr) {
    LOG(INFO) << "No volatile root page. Probably while restart";
    return true;  // this case doesn't matter. true/false are same things
  }

  if (volatile_page->is_leaf()) {
    return drop_volatiles_leaf(args, root_pointer, volatile_page);
  }

  // We iterate through all existing volatile pages to drop volatile pages of
  // level-3 or deeper (if the storage has only 2 levels, keeps all).
  // this "level-3 or deeper" is a configuration per storage.
  // Even if the volatile page is deeper than that, we keep them if it contains newer modification,
  // including descendants (so, probably we will keep higher levels anyways).
  bool kept_any = false;
  for (uint16_t i = 0; i < kInteriorFanout; ++i) {
    DualPagePointer &child_pointer = volatile_page->get_interior_record(i);
    uint16_t partition = extract_numa_node_from_snapshot_pointer(child_pointer.snapshot_pointer_);
    if (!args.partitioned_drop_ || partition == args.my_partition_) {
      bool dropped_all = drop_volatiles_recurse(args, &child_pointer);
      if (!dropped_all) {
        kept_any = true;
      }
    }
  }
  // root page is kept at this point in this case. we need to check with other threads
  return !kept_any;
}

inline ArrayPage* ArrayComposer::resolve_volatile(VolatilePagePointer pointer) {
  if (pointer.is_null()) {
    return nullptr;
  }
  const memory::GlobalVolatilePageResolver& page_resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  return reinterpret_cast<ArrayPage*>(page_resolver.resolve_offset(pointer));
}

inline bool ArrayComposer::drop_volatiles_recurse(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer) {
  if (pointer->volatile_pointer_.is_null()) {
    return true;
  }
  ASSERT_ND(pointer->snapshot_pointer_);
  snapshot::SnapshotId snapshot_id
    = extract_snapshot_id_from_snapshot_pointer(pointer->snapshot_pointer_);
  ASSERT_ND(snapshot_id != snapshot::kNullSnapshotId);
  if (snapshot_id != args.snapshot_.id_) {
    // if we have any page modified under this pointer, we should have a new snapshot page
    // here, too. Thus, we can ignore this subtree.
    return true;
  }

  ArrayPage* child_page = resolve_volatile(pointer->volatile_pointer_);
  if (child_page->is_leaf()) {
    return drop_volatiles_leaf(args, pointer, child_page);
  } else {
    return drop_volatiles_intermediate(args, pointer, child_page);
  }
}

bool ArrayComposer::drop_volatiles_intermediate(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer,
  ArrayPage* volatile_page) {
  ASSERT_ND(!volatile_page->header().snapshot_);
  ASSERT_ND(!volatile_page->is_leaf());

  // Explore/replace children first because we need to know if there is new modification.
  // In that case, we must keep this volatile page, too.
  bool kept_any = false;
  for (uint16_t i = 0; i < kInteriorFanout; ++i) {
    DualPagePointer& child_pointer = volatile_page->get_interior_record(i);
    bool dropped_all = drop_volatiles_recurse(args, &child_pointer);
    if (!dropped_all) {
      kept_any = true;
    }
  }

  if (!kept_any) {
    if (is_to_keep_volatile(volatile_page->get_level())) {
      DVLOG(2) << "Exempted";
    } else {
      args.drop(engine_, pointer->volatile_pointer_);
      pointer->volatile_pointer_.clear();
    }
  } else {
    DVLOG(1) << "Couldn't drop an intermediate volatile page that has a recent modification";
  }
  return !kept_any;
}

inline bool ArrayComposer::drop_volatiles_leaf(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer,
  ArrayPage* volatile_page) {
  ASSERT_ND(!volatile_page->header().snapshot_);
  ASSERT_ND(volatile_page->is_leaf());
  const uint16_t payload_size = storage_.get_payload_size();
  for (uint16_t i = 0; i < volatile_page->get_leaf_record_count(); ++i) {
    Record* record = volatile_page->get_leaf_record(i, payload_size);
    Epoch epoch = record->owner_id_.xct_id_.get_epoch();
    if (epoch.is_valid() && epoch > args.snapshot_.valid_until_epoch_) {
      // new record exists! so we must keep this volatile page
      DVLOG(1) << "Couldn't drop a leaf volatile page that has a recent modification";
      return false;
    }
  }
  if (is_to_keep_volatile(volatile_page->get_level())) {
    DVLOG(2) << "Exempted";
  } else {
    args.drop(engine_, pointer->volatile_pointer_);
    pointer->volatile_pointer_.clear();
  }
  return true;
}
inline bool ArrayComposer::is_to_keep_volatile(uint16_t level) {
  uint16_t threshold = storage_.get_array_metadata()->snapshot_drop_volatile_pages_threshold_;
  uint16_t array_levels = storage_.get_levels();
  ASSERT_ND(level < array_levels);
  // examples:
  // when threshold=0, all levels (0~array_levels-1) should return false.
  // when threshold=1, only root level (array_levels-1) should return true
  // when threshold=2, upto array_levels-2..
  return threshold >= array_levels - level;
}


}  // namespace array
}  // namespace storage
}  // namespace foedus
