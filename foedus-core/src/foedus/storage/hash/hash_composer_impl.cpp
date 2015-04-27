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
#include "foedus/storage/hash/hash_composer_impl.hpp"

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
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/snapshot/merge_sort.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/storage/hash/hash_partitioner_impl.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/hash/hash_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace hash {

///////////////////////////////////////////////////////////////////////
///
///  HashComposer methods
///
///////////////////////////////////////////////////////////////////////
HashComposer::HashComposer(Composer *parent)
  : engine_(parent->get_engine()),
    storage_id_(parent->get_storage_id()),
    storage_(engine_, storage_id_) {
  ASSERT_ND(storage_.exists());
}

ErrorStack HashComposer::compose(const Composer::ComposeArguments& args) {
  VLOG(0) << to_string() << " composing with " << args.log_streams_count_ << " streams.";
  debugging::StopWatch stop_watch;

  snapshot::MergeSort merge_sort(
    storage_id_,
    kHashStorage,
    args.base_epoch_,
    args.log_streams_,
    args.log_streams_count_,
    kHashMaxLevels + 1U,  // +1 for data page.
    args.work_memory_);
  CHECK_ERROR(merge_sort.initialize());

  HashComposeContext context(
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

ErrorStack HashComposer::construct_root(const Composer::ConstructRootArguments& args) {
  // compose() created root_info_pages that contain pointers to fill in the root page,
  // so we just find non-zero entry and copy it to root page.
  uint8_t levels = storage_.get_levels();
  uint16_t payload_size = storage_.get_payload_size();
  snapshot::SnapshotId new_snapshot_id = args.snapshot_writer_->get_snapshot_id();
  Epoch system_initial_epoch = engine_->get_savepoint_manager()->get_initial_durable_epoch();
  if (levels == 1U) {
    // if it's single-page hash, we have already created the root page in compose().
    ASSERT_ND(args.root_info_pages_count_ == 1U);
    const HashRootInfoPage* casted
      = reinterpret_cast<const HashRootInfoPage*>(args.root_info_pages_[0]);
    ASSERT_ND(casted->pointers_[0] != 0);
    *args.new_root_page_pointer_ = casted->pointers_[0];

    // and we have already installed it, right?
    ASSERT_ND(storage_.get_control_block()->meta_.root_snapshot_page_id_
      == casted->pointers_[0]);
    ASSERT_ND(storage_.get_control_block()->root_page_pointer_.snapshot_pointer_
      == casted->pointers_[0]);
  } else {
    HashPage* root_page = reinterpret_cast<HashPage*>(args.snapshot_writer_->get_page_base());
    SnapshotPagePointer page_id = storage_.get_metadata()->root_snapshot_page_id_;
    SnapshotPagePointer new_page_id = args.snapshot_writer_->get_next_page_id();
    *args.new_root_page_pointer_ = new_page_id;

    uint64_t root_interval = LookupRouteFinder(levels, payload_size).get_records_in_leaf();
    for (uint8_t level = 1; level < levels; ++level) {
      root_interval *= kInteriorFanout;
    }
    HashRange range(0, root_interval, storage_.get_hash_size());
    if (page_id != 0) {
      WRAP_ERROR_CODE(args.previous_snapshot_files_->read_page(page_id, root_page));
      ASSERT_ND(root_page->header().storage_id_ == storage_id_);
      ASSERT_ND(root_page->header().page_id_ == page_id);
      ASSERT_ND(root_page->get_hash_range() == range);
      root_page->header().page_id_ = new_page_id;
    } else {
      root_page->initialize_snapshot_page(
        system_initial_epoch,
        storage_id_,
        new_page_id,
        payload_size,
        levels - 1,
        range);
    }

    uint64_t child_interval = root_interval / kInteriorFanout;
    uint16_t root_children = assorted::int_div_ceil(storage_.get_hash_size(), child_interval);

    // overwrite pointers with root_info_pages.
    for (uint32_t i = 0; i < args.root_info_pages_count_; ++i) {
      const HashRootInfoPage* casted
        = reinterpret_cast<const HashRootInfoPage*>(args.root_info_pages_[i]);
      for (uint16_t j = 0; j < root_children; ++j) {
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
      for (uint16_t j = root_children; j < kInteriorFanout; ++j) {
        ASSERT_ND(casted->pointers_[j] == 0);
      }
    }

    // even in initial snapshot, all pointers must be set because we create empty pages
    // even if some sub-tree receives no logs.
    for (uint16_t j = 0; j < root_children; ++j) {
      ASSERT_ND(root_page->get_interior_record(j).snapshot_pointer_ != 0);
    }

    WRAP_ERROR_CODE(args.snapshot_writer_->dump_pages(0, 1));
    ASSERT_ND(args.snapshot_writer_->get_next_page_id() == new_page_id + 1ULL);
    // AFTER writing out the root page, install the pointer to new root page
    storage_.get_control_block()->root_page_pointer_.snapshot_pointer_ = new_page_id;
    storage_.get_control_block()->meta_.root_snapshot_page_id_ = new_page_id;
  }
  return kRetOk;
}


std::string HashComposer::to_string() const {
  return std::string("HashComposer-") + std::to_string(storage_id_);
}

///////////////////////////////////////////////////////////////////////
///
///  HashComposeContext methods
///
///////////////////////////////////////////////////////////////////////
HashComposeContext::HashComposeContext(
  Engine*                           engine,
  snapshot::MergeSort*              merge_sort,
  snapshot::SnapshotWriter*         snapshot_writer,
  cache::SnapshotFileSet*           previous_snapshot_files,
  Page*                             root_info_page)
  : engine_(engine),
    merge_sort_(merge_sort),
    system_initial_epoch_(engine->get_savepoint_manager()->get_initial_durable_epoch()),
    storage_id_(merge_sort_->get_storage_id()),
    snapshot_id_(snapshot_writer->get_snapshot_id()),
    storage_(engine, storage_id_),
    snapshot_writer_(snapshot_writer),
    previous_snapshot_files_(previous_snapshot_files),
    root_info_page_(reinterpret_cast<HashRootInfoPage*>(root_info_page)),
    partitionable_(engine_->get_soc_count() > 1U),
    levels_(storage_.get_levels()),
    bin_bits_(storage_.get_bin_bits()),
    bin_shifts_(storage_.get_bin_shifts()),
    root_children_(storage_.get_root_children()),
    total_bin_count_(storage_.get_bin_count()),
    previous_root_page_pointer_(storage_.get_metadata()->root_snapshot_page_id_) {
  cur_path_memory_.alloc(
    kPageSize * kHashMaxLevels,
    kPageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    snapshot_writer->get_numa_node());
  cur_path_ = reinterpret_cast<HashIntermediatePage*>(cur_path_memory_.get_block());

  allocated_pages_ = 0;
  allocated_intermediates_ = 0;
  page_base_ = reinterpret_cast<HashDataPage*>(snapshot_writer_->get_page_base());
  max_pages_ = snapshot_writer_->get_page_size();
  intermediate_base_
    = reinterpret_cast<HashComposedBinsPage*>(snapshot_writer_->get_intermediate_base());
  max_intermediates_ = snapshot_writer_->get_intermediate_size();
}

ErrorStack HashComposeContext::execute() {
  std::memset(root_info_page_, 0, kPageSize);
  root_info_page_->header_.storage_id_ = storage_id_;

  if (levels_ <= 1U) {
    // this storage has only one page. This is very special and trivial.
    // we process this case separately.
    return execute_single_level_hash();
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
      HashOffset initial_offset = sort_entries[0].get_key();

      CHECK_ERROR(initialize(initial_offset));
    }

    uint64_t cur = 0;
    while (cur < count) {
      const HashCommonUpdateLogType* head = reinterpret_cast<const HashCommonUpdateLogType*>(
        merge_sort_->resolve_sort_position(cur));
      HashOffset head_offset = head->offset_;
      ASSERT_ND(head_offset == sort_entries[cur].get_key());
      // switch to a page containing this offset
      WRAP_ERROR_CODE(update_cur_path(head_offset));
      HashRange page_range = cur_path_[0]->get_hash_range();
      ASSERT_ND(page_range.contains(head_offset));

      // grab a range of logs that are in the same page.
      uint64_t next;
      for (next = cur + 1U; LIKELY(next < count); ++next) {
        // this check uses sort_entries which are nicely contiguous.
        HashOffset offset = sort_entries[next].get_key();
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

void HashComposeContext::apply_batch(uint64_t cur, uint64_t next) {
  const uint16_t kFetchSize = 8;
  const log::RecordLogType* logs[kFetchSize];
  HashPage* leaf = cur_path_[0];
  HashRange range = leaf->get_hash_range();
  while (cur < next) {
    uint16_t desired = std::min<uint16_t>(kFetchSize, next - cur);
    uint16_t fetched = merge_sort_->fetch_logs(cur, desired, logs);
    for (uint16_t i = 0; i < kFetchSize && LIKELY(i < fetched); ++i) {
      const HashCommonUpdateLogType* log
        = reinterpret_cast<const HashCommonUpdateLogType*>(logs[i]);
      ASSERT_ND(range.contains(log->offset_));
      uint16_t index = log->offset_ - range.begin_;
      Record* record = leaf->get_leaf_record(index, payload_size_);
      if (log->header_.get_type() == log::kLogCodeHashOverwrite) {
        const HashOverwriteLogType* casted
          = reinterpret_cast<const HashOverwriteLogType*>(log);
        casted->apply_record(nullptr, storage_id_, &record->owner_id_, record->payload_);
      } else {
        ASSERT_ND(log->header_.get_type() == log::kLogCodeHashIncrement);
        const HashIncrementLogType* casted
          = reinterpret_cast<const HashIncrementLogType*>(log);
        casted->apply_record(nullptr, storage_id_, &record->owner_id_, record->payload_);
      }
    }
    cur += fetched;
    ASSERT_ND(cur <= next);
  }
}

ErrorStack HashComposeContext::read_root_page() {
}

ErrorStack HashComposeContext::execute_single_level_hash() {
  // single-page hash. root is a leaf page.
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

uint16_t HashComposeContext::get_root_children() const {
  uint64_t child_interval = offset_intervals_[levels_ - 2U];
  return assorted::int_div_ceil(storage_.get_hash_size(), child_interval);
}

ErrorStack HashComposeContext::finalize() {
  ASSERT_ND(levels_ > 1U);

  HashRange last_range = cur_path_[0]->get_hash_range();
  if (is_initial_snapshot() && last_range.end_ < storage_.get_hash_size()) {
    VLOG(0) << "Need to fill out empty pages in initial snapshot of hash-" << storage_id_
      << ", from " << last_range.end_ << " to the end of hash";
    WRAP_ERROR_CODE(create_empty_pages(last_range.end_, storage_.get_hash_size()));
  }

  // flush the main buffer. now we finalized all leaf pages
  if (allocated_pages_ > 0) {
    WRAP_ERROR_CODE(dump_data_pages());
    ASSERT_ND(allocated_pages_ == 0);
  }

  // intermediate pages are different animals.
  // we store them in a separate buffer, and now finally we can get their page IDs.
  // Until now, we used relative indexes in intermediate buffer as page ID, storing them in
  // page ID header. now let's convert all of them to be final page ID.
  HashPage* root_page = intermediate_base_;
  ASSERT_ND(root_page == cur_path_[levels_ - 1]);
  ASSERT_ND(root_page->get_level() == levels_ - 1);
  ASSERT_ND(root_page->header().page_id_ == 0);  // this is the only page that has page-id 0

  // base_pointer + offset in intermediate buffer will be the new page ID.
  const SnapshotPagePointer base_pointer = snapshot_writer_->get_next_page_id();
  root_page->header().page_id_ = base_pointer;
  for (uint32_t i = 1; i < allocated_intermediates_; ++i) {
    SnapshotPagePointer new_page_id = base_pointer + i;
    HashPage* page = intermediate_base_ + i;
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

  const uint16_t root_children = get_root_children();
  const PartitionId partition = snapshot_writer_->get_numa_node();
  ASSERT_ND(partitioning_data_);
  for (uint16_t j = 0; j < root_children; ++j) {
    DualPagePointer& pointer = root_page->get_interior_record(j);
    ASSERT_ND(pointer.volatile_pointer_.is_null());
    SnapshotPagePointer page_id = pointer.snapshot_pointer_;
    snapshot::SnapshotId snapshot_id = extract_snapshot_id_from_snapshot_pointer(page_id);

    if (!partitioning_data_->partitionable_ || partitioning_data_->bucket_owners_[j] == partition) {
      ASSERT_ND(page_id != 0);
      // okay, this is a page this node is responsible for.
      if (snapshot_id == snapshot_id_) {
        // we already have snapshot pointers because it points to leaf pages. (2 level hash)
        // the pointer is already valid as a snapshot pointer
        ASSERT_ND(extract_numa_node_from_snapshot_pointer(page_id)
          == snapshot_writer_->get_numa_node());
        ASSERT_ND(root_page->get_level() == 1U);
        ASSERT_ND(verify_snapshot_pointer(pointer.snapshot_pointer_));
      } else if (snapshot_id == snapshot::kNullSnapshotId) {
        // intermediate pages created in this snapshot.
        // just like other pages adjusted above, it's an offset from intermediate_base_
        ASSERT_ND(root_page->get_level() > 1U);
        ASSERT_ND(extract_numa_node_from_snapshot_pointer(page_id) == 0);
        ASSERT_ND(page_id < allocated_intermediates_);
        pointer.snapshot_pointer_ = base_pointer + page_id;
        ASSERT_ND(verify_snapshot_pointer(pointer.snapshot_pointer_));
      } else {
        // then, it's a page in previous snapshots we didn't modify
        ASSERT_ND(!is_initial_snapshot());
        ASSERT_ND(snapshot_id != snapshot_id_);
      }
      root_info_page_->pointers_[j] = pointer.snapshot_pointer_;
    } else {
      ASSERT_ND((!is_initial_snapshot() && page_id != 0 && snapshot_id != snapshot_id_)
        || (is_initial_snapshot() && page_id == 0));
    }
  }
  for (uint16_t j = root_children; j < kInteriorFanout; ++j) {
    ASSERT_ND(root_page->get_interior_record(j).is_both_null());
  }


  // AFTER durably writing out the intermediate pages to the file, we install snapshot pointers.
  uint64_t installed_count = 0;
  CHECK_ERROR(install_snapshot_pointers(base_pointer, &installed_count));

  return kRetOk;
}

ErrorStack HashComposeContext::initialize(HashBin initial_bin) {
  ASSERT_ND(allocated_intermediates_ == 0);
  ASSERT_ND(allocated_pages_ == 0);
  ASSERT_ND(levels_ > 1U);

  // First, load or create the root page.
  CHECK_ERROR(init_root_page());

  // If an initial snapshot, we have to create empty pages first.
  if (is_initial_snapshot()) {
    HashRange leaf_range = to_leaf_range(initial_offset);
    VLOG(0) << "Need to fill out empty pages in initial snapshot of hash-" << storage_id_
      << ", upto " << leaf_range.end_;
    WRAP_ERROR_CODE(create_empty_pages(0, leaf_range.end_));
    ASSERT_ND(cur_path_[0]);
    ASSERT_ND(cur_path_[0]->get_hash_range() == leaf_range);
  }
  return kRetOk;
}

ErrorStack HashComposeContext::init_root_page() {
  uint8_t level = levels_ - 1U;
  ASSERT_ND(level > 0);
  HashPage* page = intermediate_base_;
  HashRange range(0, storage_.get_hash_size());

  WRAP_ERROR_CODE(read_or_init_page(previous_root_page_pointer_, 0, level, range, page));
  cur_path_[level] = page;
  return kRetOk;
}

ErrorCode HashComposeContext::dump_data_pages() {
  CHECK_ERROR_CODE(snapshot_writer_->dump_pages(0, allocated_pages_));
  ASSERT_ND(snapshot_writer_->get_next_page_id()
    == page_base_[0].header().page_id_ + allocated_pages_);
  ASSERT_ND(snapshot_writer_->get_next_page_id()
    == page_base_[allocated_pages_ - 1].header().page_id_ + 1ULL);
  allocated_pages_ = 0;
  return kErrorCodeOk;
}

inline ErrorCode HashComposeContext::expand_intermediate_pool_if_needed() {
  ASSERT_ND(allocated_intermediates_ <= max_intermediates_);
  if (UNLIKELY(allocated_intermediates_ == max_intermediates_)) {
    LOG(INFO) << "Automatically expanding intermediate_pool. This should be a rare event";
    uint32_t required = allocated_intermediates_ + 1U;
    CHECK_ERROR_CODE(snapshot_writer_->expand_intermediate_memory(required, true));
    intermediate_base_ = reinterpret_cast<HashPage*>(snapshot_writer_->get_intermediate_base());
    max_intermediates_ = snapshot_writer_->get_intermediate_size();
  }
  return kErrorCodeOk;
}

ErrorCode HashComposeContext::update_cur_path(HashOffset next_offset) {
  ASSERT_ND(levels_ > 1U);
  ASSERT_ND(cur_path_[0] == nullptr || next_offset >= cur_path_[0]->get_hash_range().begin_);
  if (cur_path_[0] != nullptr && next_offset < cur_path_[0]->get_hash_range().end_) {
    // already in the page. this usually doesn't happen as we batch-apply as many as possible,
    // but might happen when logs for the same page are on a boundary of windows.
    return kErrorCodeOk;
  }

  HashRange next_range = to_leaf_range(next_offset);
  HashOffset jump_from = cur_path_[0] == nullptr ? 0 : cur_path_[0]->get_hash_range().end_;
  HashOffset jump_to = next_range.begin_;
  if (jump_to > jump_from && is_initial_snapshot()) {
    VLOG(0) << "Need to fill out empty pages in initial snapshot of hash-" << storage_id_
      << ", from " << jump_from << " to " << jump_to;
    CHECK_ERROR_CODE(create_empty_pages(jump_from, jump_to));
  }

  // then switch pages. we might have to switch parent pages, too.
  ASSERT_ND(cur_path_[levels_ - 1U]->get_hash_range().contains(next_offset));
  for (uint8_t level = levels_ - 2U; level < kMaxLevels; --level) {  // note, unsigned.
    // we don't care changes in route[0] (record ordinals in page)
    if (cur_path_[level] != nullptr && cur_path_[level]->get_hash_range().contains(next_offset)) {
      // skip non-changed path. most likely only the leaf has changed.
      continue;
    }

    // page switched! we have to allocate a new page and point to it.
    HashPage* parent = cur_path_[level + 1U];
    HashRange parent_range = parent->get_hash_range();
    ASSERT_ND(parent_range.contains(next_offset));
    uint64_t interval = offset_intervals_[level];
    uint16_t i = (next_offset - parent_range.begin_) / interval;
    ASSERT_ND(i < kInteriorFanout);

    HashRange child_range(
      parent_range.begin_ + i * interval,
      parent_range.begin_ + (i + 1U) * interval,
      parent_range.end_);

    DualPagePointer& pointer = parent->get_interior_record(i);
    ASSERT_ND(pointer.volatile_pointer_.is_null());
    SnapshotPagePointer old_page_id = pointer.snapshot_pointer_;
    ASSERT_ND((!is_initial_snapshot() && old_page_id != 0)
      || (is_initial_snapshot() && old_page_id == 0));

    HashPage* page;
    SnapshotPagePointer new_page_id;
    if (level > 0U) {
      // we switched an intermediate page
      CHECK_ERROR_CODE(expand_intermediate_pool_if_needed());
      page = intermediate_base_ + allocated_intermediates_;
      new_page_id = allocated_intermediates_;
      ++allocated_intermediates_;
      CHECK_ERROR_CODE(read_or_init_page(old_page_id, new_page_id, level, child_range, page));
    } else {
      // we switched a leaf page. in this case, we might have to flush the buffer
      if (allocated_pages_ >= max_pages_) {
        CHECK_ERROR_CODE(dump_data_pages());
        ASSERT_ND(allocated_pages_ == 0);
      }

      // remember, we can finalize the page ID of leaf pages at this point
      page = page_base_ + allocated_pages_;
      new_page_id = snapshot_writer_->get_next_page_id() + allocated_pages_;
      ASSERT_ND(verify_snapshot_pointer(new_page_id));
      ++allocated_pages_;
      CHECK_ERROR_CODE(read_or_init_page(old_page_id, new_page_id, level, child_range, page));
    }
    ASSERT_ND(page->header().page_id_ == new_page_id);
    pointer.snapshot_pointer_ = new_page_id;
    cur_path_[level] = page;
  }
  ASSERT_ND(verify_cur_path());
  return kErrorCodeOk;
}

inline ErrorCode HashComposeContext::read_or_init_page(
  SnapshotPagePointer old_page_id,
  SnapshotPagePointer new_page_id,
  uint8_t level,
  HashRange range,
  HashPage* page) {
  ASSERT_ND(new_page_id != 0 || level == levels_ - 1U);
  if (old_page_id != 0) {
    ASSERT_ND(!is_initial_snapshot());
    CHECK_ERROR_CODE(previous_snapshot_files_->read_page(old_page_id, page));
    ASSERT_ND(page->header().storage_id_ == storage_id_);
    ASSERT_ND(page->header().page_id_ == old_page_id);
    ASSERT_ND(page->get_level() == level);
    ASSERT_ND(page->get_hash_range() == range);
    page->header().page_id_ = new_page_id;
  } else {
    ASSERT_ND(is_initial_snapshot());
    page->initialize_snapshot_page(
      system_initial_epoch_,
      storage_id_,
      new_page_id,
      payload_size_,
      level,
      range);
  }
  return kErrorCodeOk;
}

inline ErrorCode HashComposeContext::read_cur_path(
  SnapshotPagePointer page_id,
  uint8_t level) {
  ASSERT_ND(page_id != 0 && level == levels_ - 1U);
  HashIntermediatePage* page = get_cur_path(level);
  if (page_id != 0) {
    ASSERT_ND(!is_initial_snapshot());
    CHECK_ERROR_CODE(previous_snapshot_files_->read_page(page_id, page));
    ASSERT_ND(page->header().storage_id_ == storage_id_);
    ASSERT_ND(page->header().page_id_ == page_id);
    ASSERT_ND(page->get_level() == level);
  } else {
    ASSERT_ND(is_initial_snapshot());
    page->initialize_snapshot_page(
      storage_id_,
      page_id,
      level,
      );
  }
  return kErrorCodeOk;
}

bool HashComposeContext::verify_cur_path() const {
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

bool HashComposeContext::verify_snapshot_pointer(SnapshotPagePointer pointer) {
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
///  HashComposeContext::install_snapshot_pointers() related methods
///
///////////////////////////////////////////////////////////////////////
ErrorStack HashComposeContext::install_snapshot_pointers(
  SnapshotPagePointer snapshot_base,
  uint64_t* installed_count) const {
  ASSERT_ND(levels_ > 1U);  // no need to call this method in one-page hash
  ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(snapshot_base) == snapshot_id_);

  *installed_count = 0;
  VolatilePagePointer pointer = storage_.get_control_block()->root_page_pointer_.volatile_pointer_;
  if (pointer.is_null()) {
    VLOG(0) << "No volatile pages.. maybe while restart?";
    return kRetOk;
  }

  const memory::GlobalVolatilePageResolver& resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  HashPage* volatile_root = reinterpret_cast<HashPage*>(resolver.resolve_offset(pointer));

  // compared to masstree, hash is much easier to install snapshot pointers because the
  // shape of the tree is exactly same between volatile and snapshot.
  // we just recurse with the corresponding snapshot and volatile pages.
  debugging::StopWatch watch;
  const HashPage* snapshot_root = intermediate_base_;
  WRAP_ERROR_CODE(install_snapshot_pointers_recurse(
    snapshot_base,
    resolver,
    snapshot_root,
    volatile_root,
    installed_count));
  watch.stop();
  VLOG(0) << "HashStorage-" << storage_id_ << " installed " << *installed_count << " pointers"
    << " in " << watch.elapsed_ms() << "ms";
  return kRetOk;
}

ErrorCode HashComposeContext::install_snapshot_pointers_recurse(
  SnapshotPagePointer snapshot_base,
  const memory::GlobalVolatilePageResolver& resolver,
  const HashPage* snapshot_page,
  HashPage* volatile_page,
  uint64_t* installed_count) const {
  ASSERT_ND(snapshot_page->get_hash_range() == volatile_page->get_hash_range());
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
        HashPage* volatile_next
          = reinterpret_cast<HashPage*>(resolver.resolve_offset(volatile_pointer));
        uint64_t offset = pointer - snapshot_base;
        const HashPage* snapshot_next = intermediate_base_ + offset;
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
Composer::DropResult HashComposer::drop_volatiles(const Composer::DropVolatilesArguments& args) {
  Composer::DropResult result(args);
  if (storage_.get_hash_metadata()->keeps_all_volatile_pages()) {
    LOG(INFO) << "Keep-all-volatile: Storage-" << storage_.get_name()
      << " is configured to keep all volatile pages.";
    result.dropped_all_ = false;
    return result;
  }

  DualPagePointer* root_pointer = &storage_.get_control_block()->root_page_pointer_;
  HashPage* volatile_page = resolve_volatile(root_pointer->volatile_pointer_);
  if (volatile_page == nullptr) {
    LOG(INFO) << "No volatile root page. Probably while restart";
    return result;
  }

  // single-page hash has only the root page. nothing to do here.
  // we might drop the root page later, just like non-single-page cases.
  if (volatile_page->is_leaf()) {
    LOG(INFO) << "Single-page hash skipped by .";
    return result;
  }

  // We iterate through all existing volatile pages to drop volatile pages of
  // level-3 or deeper (if the storage has only 2 levels, keeps all).
  // this "level-3 or deeper" is a configuration per storage.
  // Even if the volatile page is deeper than that, we keep them if it contains newer modification,
  // including descendants (so, probably we will keep higher levels anyways).
  for (uint16_t i = 0; i < kInteriorFanout; ++i) {
    DualPagePointer& child_pointer = volatile_page->get_interior_record(i);
    if (!child_pointer.volatile_pointer_.is_null()) {
      ASSERT_ND(child_pointer.snapshot_pointer_ != 0);
      uint16_t partition = extract_numa_node_from_snapshot_pointer(child_pointer.snapshot_pointer_);
      if (!args.partitioned_drop_ || partition == args.my_partition_) {
        result.combine(drop_volatiles_recurse(args, &child_pointer));
      }
    }
  }
  // root page is kept at this point in this case. we need to check with other threads
  return result;
}

void HashComposer::drop_root_volatile(const Composer::DropVolatilesArguments& args) {
  if (storage_.get_hash_metadata()->keeps_all_volatile_pages()) {
    LOG(INFO) << "Oh, but keep-all-volatile is on. Storage-" << storage_.get_name()
      << " is configured to keep all volatile pages.";
    return;
  }
  if (is_to_keep_volatile(storage_.get_levels() - 1U)) {
    LOG(INFO) << "Oh, but Storage-" << storage_.get_name() << " is configured to keep"
      << " the root page.";
    return;
  }
  DualPagePointer* root_pointer = &storage_.get_control_block()->root_page_pointer_;
  HashPage* volatile_page = resolve_volatile(root_pointer->volatile_pointer_);
  if (volatile_page == nullptr) {
    LOG(INFO) << "Oh, but root volatile page already null";
    return;
  }

  if (volatile_page->is_leaf()) {
    // if this is a single-level hash. we now have to check epochs of records in the root page.
    uint16_t records = storage_.get_hash_size();
    for (uint16_t i = 0; i < records; ++i) {
      Record* record = volatile_page->get_leaf_record(i, storage_.get_payload_size());
      Epoch epoch = record->owner_id_.xct_id_.get_epoch();
      ASSERT_ND(epoch.is_valid());
      if (epoch > args.snapshot_.valid_until_epoch_) {
        LOG(INFO) << "Oh, but the root volatile page in single-level hash contains a new rec";
        return;
      }
    }
  } else {
    // otherwise, all verifications already done. go drop everything!
  }
  LOG(INFO) << "Okay, drop em all!!";
  drop_all_recurse(args, root_pointer);
}

void HashComposer::drop_all_recurse(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer) {
  if (pointer->volatile_pointer_.is_null()) {
    return;
  }
  HashPage* page = resolve_volatile(pointer->volatile_pointer_);
  if (!page->is_leaf()) {
    for (uint16_t i = 0; i < kInteriorFanout; ++i) {
      DualPagePointer& child_pointer = page->get_interior_record(i);
      drop_all_recurse(args, &child_pointer);
    }
  }
  args.drop(engine_, pointer->volatile_pointer_);
  pointer->volatile_pointer_.clear();
}

inline HashPage* HashComposer::resolve_volatile(VolatilePagePointer pointer) {
  if (pointer.is_null()) {
    return nullptr;
  }
  const memory::GlobalVolatilePageResolver& page_resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  return reinterpret_cast<HashPage*>(page_resolver.resolve_offset(pointer));
}

inline Composer::DropResult HashComposer::drop_volatiles_recurse(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer) {
  if (pointer->volatile_pointer_.is_null()) {
    return Composer::DropResult(args);
  }
  ASSERT_ND(pointer->snapshot_pointer_ == 0
    || extract_snapshot_id_from_snapshot_pointer(pointer->snapshot_pointer_)
        != snapshot::kNullSnapshotId);
  // The snapshot pointer CAN be null.
  // It means that this subtree has not constructed a new snapshot page in this snapshot.
  HashPage* child_page = resolve_volatile(pointer->volatile_pointer_);
  if (child_page->is_leaf()) {
    return drop_volatiles_leaf(args, pointer, child_page);
  } else {
    return drop_volatiles_intermediate(args, pointer, child_page);
  }
}

Composer::DropResult HashComposer::drop_volatiles_intermediate(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer,
  HashPage* volatile_page) {
  ASSERT_ND(!volatile_page->header().snapshot_);
  ASSERT_ND(!volatile_page->is_leaf());
  Composer::DropResult result(args);

  // Explore/replace children first because we need to know if there is new modification.
  // In that case, we must keep this volatile page, too.
  // Intermediate volatile page is kept iff there are no child volatile pages.
  for (uint16_t i = 0; i < kInteriorFanout; ++i) {
    DualPagePointer& child_pointer = volatile_page->get_interior_record(i);
    result.combine(drop_volatiles_recurse(args, &child_pointer));
  }

  if (result.dropped_all_) {
    if (is_to_keep_volatile(volatile_page->get_level())) {
      DVLOG(2) << "Exempted";
      result.dropped_all_ = false;
    } else {
      args.drop(engine_, pointer->volatile_pointer_);
      pointer->volatile_pointer_.clear();
    }
  } else {
    DVLOG(1) << "Couldn't drop an intermediate page that has a recent modification in child";
  }
  ASSERT_ND(!result.dropped_all_ || pointer->volatile_pointer_.is_null());
  return result;
}

inline Composer::DropResult HashComposer::drop_volatiles_leaf(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer,
  HashPage* volatile_page) {
  ASSERT_ND(!volatile_page->header().snapshot_);
  ASSERT_ND(volatile_page->is_leaf());
  Composer::DropResult result(args);
  if (is_to_keep_volatile(volatile_page->get_level())) {
    DVLOG(2) << "Exempted";
    result.dropped_all_ = false;
    return result;
  }

  const uint16_t payload_size = storage_.get_payload_size();
  const HashRange& range = volatile_page->get_hash_range();
  ASSERT_ND(range.end_ <= range.begin_ + volatile_page->get_leaf_record_count());
  ASSERT_ND(range.end_ == range.begin_ + volatile_page->get_leaf_record_count()
    || range.end_ == storage_.get_hash_size());
  uint16_t records = range.end_ - range.begin_;
  for (uint16_t i = 0; i < records; ++i) {
    Record* record = volatile_page->get_leaf_record(i, payload_size);
    Epoch epoch = record->owner_id_.xct_id_.get_epoch();
    ASSERT_ND(epoch.is_valid());
    result.on_rec_observed(epoch);
  }
  if (result.dropped_all_) {
    args.drop(engine_, pointer->volatile_pointer_);
    pointer->volatile_pointer_.clear();
  }
  return result;
}
inline bool HashComposer::is_to_keep_volatile(uint16_t level) {
  uint16_t threshold = storage_.get_hash_metadata()->snapshot_drop_volatile_pages_threshold_;
  uint16_t hash_levels = storage_.get_levels();
  ASSERT_ND(level < hash_levels);
  // examples:
  // when threshold=0, all levels (0~hash_levels-1) should return false.
  // when threshold=1, only root level (hash_levels-1) should return true
  // when threshold=2, upto hash_levels-2..
  return threshold + level >= hash_levels;
}


}  // namespace hash
}  // namespace storage
}  // namespace foedus
