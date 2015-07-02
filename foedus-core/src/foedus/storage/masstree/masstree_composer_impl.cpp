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
#include "foedus/storage/masstree/masstree_composer_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <ostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/snapshot/merge_sort.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/metadata.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"

namespace foedus {
namespace storage {
namespace masstree {

inline void fill_payload_padded(char* destination, const char* source, uint16_t count) {
  if (count > 0) {
    ASSERT_ND(is_key_aligned_and_zero_padded(source, count));
    std::memcpy(destination, source, assorted::align8(count));
  }
}


inline MasstreeBorderPage* as_border(MasstreePage* page) {
  ASSERT_ND(page->is_border());
  return reinterpret_cast<MasstreeBorderPage*>(page);
}

inline MasstreeIntermediatePage* as_intermediate(MasstreePage* page) {
  ASSERT_ND(!page->is_border());
  return reinterpret_cast<MasstreeIntermediatePage*>(page);
}

///////////////////////////////////////////////////////////////////////
///
///  MasstreeComposer methods
///
///////////////////////////////////////////////////////////////////////
MasstreeComposer::MasstreeComposer(Composer *parent)
  : engine_(parent->get_engine()),
    storage_id_(parent->get_storage_id()),
    storage_(engine_, storage_id_) {
  ASSERT_ND(storage_.exists());
}

ErrorStack MasstreeComposer::compose(const Composer::ComposeArguments& args) {
  VLOG(0) << to_string() << " composing with " << args.log_streams_count_ << " streams.";
  debugging::StopWatch stop_watch;

  snapshot::MergeSort merge_sort(
    storage_id_,
    kMasstreeStorage,
    args.base_epoch_,
    args.log_streams_,
    args.log_streams_count_,
    MasstreeComposeContext::kMaxLevels,
    args.work_memory_);
  CHECK_ERROR(merge_sort.initialize());

  MasstreeComposeContext context(engine_, &merge_sort, args);
  CHECK_ERROR(context.execute());

  CHECK_ERROR(merge_sort.uninitialize());  // no need for scoped release. its destructor is safe.

  stop_watch.stop();
  LOG(INFO) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}

bool from_this_snapshot(SnapshotPagePointer pointer, const Composer::ConstructRootArguments& args) {
  if (pointer == 0) {
    return false;
  }
  snapshot::SnapshotId snapshot_id = extract_snapshot_id_from_snapshot_pointer(pointer);
  ASSERT_ND(snapshot_id != snapshot::kNullSnapshotId);
  return snapshot_id == args.snapshot_writer_->get_snapshot_id();
}

ErrorStack MasstreeComposer::construct_root(const Composer::ConstructRootArguments& args) {
  ASSERT_ND(args.root_info_pages_count_ > 0);
  VLOG(0) << to_string() << " combining " << args.root_info_pages_count_ << " root page info.";
  debugging::StopWatch stop_watch;
  SnapshotPagePointer new_root_id = args.snapshot_writer_->get_next_page_id();
  Page* new_root = args.snapshot_writer_->get_page_base();
  std::memcpy(new_root, args.root_info_pages_[0], kPageSize);  // use the first one as base
  if (args.root_info_pages_count_ == 1U) {
    VLOG(0) << to_string() << " only 1 root info page, so we just use it as the new root.";
  } else {
    // otherwise, we merge inputs from each reducer. because of how we design partitions,
    // the inputs are always intermediate pages with same partitioning. we just place new
    // pointers to children
    CHECK_ERROR(check_buddies(args));

    // because of how we partition, B-tree levels might be unbalanced at the root of first layer.
    // thus, we take the max of btree-level here.
    MasstreeIntermediatePage* base = reinterpret_cast<MasstreeIntermediatePage*>(new_root);
    ASSERT_ND(!base->is_border());
    for (uint16_t buddy = 1; buddy < args.root_info_pages_count_; ++buddy) {
      const MasstreeIntermediatePage* page
        = reinterpret_cast<const MasstreeIntermediatePage*>(args.root_info_pages_[buddy]);
      if (page->get_btree_level() > base->get_btree_level()) {
        LOG(INFO) << "MasstreeStorage-" << storage_id_ << " has an unbalanced depth in sub-tree";
        base->header().masstree_in_layer_level_ = page->get_btree_level();
      }
    }

    uint16_t updated_count = 0;
    for (uint16_t i = 0; i <= base->get_key_count(); ++i) {
      MasstreeIntermediatePage::MiniPage& base_mini = base->get_minipage(i);
      for (uint16_t j = 0; j <= base_mini.key_count_; ++j) {
        SnapshotPagePointer base_pointer = base_mini.pointers_[j].snapshot_pointer_;
        if (from_this_snapshot(base_pointer, args)) {
          ++updated_count;
          continue;  // base has updated it
        }

        for (uint16_t buddy = 1; buddy < args.root_info_pages_count_; ++buddy) {
          const MasstreeIntermediatePage* page
            = reinterpret_cast<const MasstreeIntermediatePage*>(args.root_info_pages_[buddy]);
          const MasstreeIntermediatePage::MiniPage& mini = page->get_minipage(i);
          SnapshotPagePointer pointer = mini.pointers_[j].snapshot_pointer_;
          if (from_this_snapshot(pointer, args)) {
            base_mini.pointers_[j].snapshot_pointer_ = pointer;  // this buddy has updated it
            ++updated_count;
            break;
          }
        }
      }
    }
    VLOG(0) << to_string() << " has " << updated_count << " new pointers from root.";
  }

  // In either case, some pointer might be null if that partition had no log record
  // and if this is an initial snapshot. Create a dummy page for them.
  MasstreeIntermediatePage* merged = reinterpret_cast<MasstreeIntermediatePage*>(new_root);
  ASSERT_ND(!merged->is_border());
  uint16_t dummy_count = 0;
  for (MasstreeIntermediatePointerIterator it(merged); it.is_valid(); it.next()) {
    DualPagePointer& pointer = merged->get_minipage(it.index_).pointers_[it.index_mini_];
    if (pointer.snapshot_pointer_ == 0) {
      // this should happen only while an initial snapshot for this storage.
      ASSERT_ND(storage_.get_control_block()->root_page_pointer_.snapshot_pointer_ == 0);
      KeySlice low, high;
      merged->extract_separators_snapshot(it.index_, it.index_mini_, &low, &high);
      uint32_t offset = 1U + dummy_count;  // +1 for new_root
      SnapshotPagePointer page_id = args.snapshot_writer_->get_next_page_id() + offset;
      MasstreeBorderPage* dummy = reinterpret_cast<MasstreeBorderPage*>(
        args.snapshot_writer_->get_page_base() + offset);
      dummy->initialize_snapshot_page(storage_id_, page_id, 0, low, high);
      pointer.snapshot_pointer_ = page_id;
      ++dummy_count;
    }
  }
  VLOG(0) << to_string() << " has added " << dummy_count << " dummy pointers in root.";

  ASSERT_ND(new_root->get_header().snapshot_);
  ASSERT_ND(new_root->get_header().storage_id_ == storage_id_);
  ASSERT_ND(new_root->get_header().get_page_type() == kMasstreeIntermediatePageType);

  new_root->get_header().page_id_ = new_root_id;
  *args.new_root_page_pointer_ = new_root_id;
  WRAP_ERROR_CODE(args.snapshot_writer_->dump_pages(0, 1 + dummy_count));

  // AFTER writing out the root page, install the pointer to new root page
  storage_.get_control_block()->root_page_pointer_.snapshot_pointer_ = new_root_id;
  storage_.get_control_block()->meta_.root_snapshot_page_id_ = new_root_id;

  stop_watch.stop();
  VLOG(0) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}

ErrorStack MasstreeComposer::check_buddies(const Composer::ConstructRootArguments& args) const {
  const MasstreeIntermediatePage* base
    = reinterpret_cast<const MasstreeIntermediatePage*>(args.root_info_pages_[0]);
  const uint16_t key_count = base->header().key_count_;
  const uint32_t buddy_count = args.root_info_pages_count_;
  // check key_count
  for (uint16_t buddy = 1; buddy < buddy_count; ++buddy) {
    const MasstreeIntermediatePage* page
      = reinterpret_cast<const MasstreeIntermediatePage*>(args.root_info_pages_[buddy]);
    if (page->header().key_count_ != key_count) {
      ASSERT_ND(false);
      return ERROR_STACK_MSG(kErrorCodeInternalError, "key count");
    } else if (base->get_low_fence() != page->get_low_fence()) {
      ASSERT_ND(false);
      return ERROR_STACK_MSG(kErrorCodeInternalError, "low fence");
    } else if (base->get_high_fence() != page->get_high_fence()) {
      ASSERT_ND(false);
      return ERROR_STACK_MSG(kErrorCodeInternalError, "high fence");
    }
  }

  // check inside minipages
  for (uint16_t i = 0; i <= key_count; ++i) {
    const MasstreeIntermediatePage::MiniPage& base_mini = base->get_minipage(i);
    uint16_t mini_count = base_mini.key_count_;
    // check separator
    for (uint16_t buddy = 1; buddy < buddy_count; ++buddy) {
      const MasstreeIntermediatePage* page
        = reinterpret_cast<const MasstreeIntermediatePage*>(args.root_info_pages_[buddy]);
      if (i < key_count) {
        if (base->get_separator(i) != page->get_separator(i)) {
          ASSERT_ND(false);
          return ERROR_STACK_MSG(kErrorCodeInternalError, "separator");
        }
      }
    }
    // check individual separator and pointers
    for (uint16_t j = 0; j <= mini_count; ++j) {
      ASSERT_ND(base_mini.pointers_[j].volatile_pointer_.is_null());
      uint16_t updated_by = buddy_count;  // only one should have updated each pointer
      SnapshotPagePointer old_pointer = base_mini.pointers_[j].snapshot_pointer_;
      if (from_this_snapshot(old_pointer, args)) {
        updated_by = 0;
        old_pointer = 0;  // will use buddy-1's pointer below
      }

      for (uint16_t buddy = 1; buddy < buddy_count; ++buddy) {
        const MasstreeIntermediatePage* page
          = reinterpret_cast<const MasstreeIntermediatePage*>(args.root_info_pages_[buddy]);
        const MasstreeIntermediatePage::MiniPage& mini = page->get_minipage(i);
        ASSERT_ND(mini.pointers_[j].volatile_pointer_.is_null());
        if (j < mini_count) {
          if (mini.separators_[j] != base_mini.separators_[j]) {
            ASSERT_ND(false);
            return ERROR_STACK_MSG(kErrorCodeInternalError, "individual separator");
          }
        }
        SnapshotPagePointer pointer = mini.pointers_[j].snapshot_pointer_;
        if (buddy == 1U && updated_by == 0) {
          old_pointer = pointer;
        }
        if (from_this_snapshot(pointer, args)) {
          if (updated_by != buddy_count) {
            return ERROR_STACK_MSG(kErrorCodeInternalError, "multiple updaters");
          }
          updated_by = buddy;
        } else if (old_pointer != pointer) {
          return ERROR_STACK_MSG(kErrorCodeInternalError, "original pointer mismatch");
        }
      }
    }
  }

  return kRetOk;
}


std::string MasstreeComposer::to_string() const {
  return std::string("MasstreeComposer-") + std::to_string(storage_id_);
}

///////////////////////////////////////////////////////////////////////
///
///  MasstreeComposeContext methods
///
///////////////////////////////////////////////////////////////////////
MasstreeComposeContext::MasstreeComposeContext(
  Engine* engine,
  snapshot::MergeSort* merge_sort,
  const Composer::ComposeArguments& args)
  : engine_(engine),
    merge_sort_(merge_sort),
    id_(merge_sort->get_storage_id()),
    storage_(engine, id_),
    args_(args),
    snapshot_id_(args.snapshot_writer_->get_snapshot_id()),
    numa_node_(get_writer()->get_numa_node()),
    max_pages_(get_writer()->get_page_size()),
    root_(reinterpret_cast<MasstreeIntermediatePage*>(args.root_info_page_)),
    page_base_(reinterpret_cast<Page*>(get_writer()->get_page_base())),
    original_base_(merge_sort->get_original_pages()) {
  page_id_base_ = get_writer()->get_next_page_id();
  refresh_page_boundary_info_variables();
  root_index_ = 0;
  root_index_mini_ = 0;
  allocated_pages_ = 0;
  cur_path_levels_ = 0;
  page_boundary_info_cur_pos_ = 0;
  page_boundary_elements_ = 0;
  std::memset(cur_path_, 0, sizeof(cur_path_));
  std::memset(cur_prefix_be_, 0, sizeof(cur_prefix_be_));
  std::memset(cur_prefix_slices_, 0, sizeof(cur_prefix_slices_));
  std::memset(tmp_boundary_array_, 0, sizeof(tmp_boundary_array_));
}

void MasstreeComposeContext::refresh_page_boundary_info_variables() {
  // we use intermediate pool of snapshot writer for page_boundary_info_/sort_
  uint32_t size_in_pages = get_writer()->get_intermediate_size();
  uint64_t size_in_bytes = size_in_pages * sizeof(Page);
  // page_boundary_info_ consumes many more bytes than page_boundary_sort_ per entry.
  // PageBoundaryInfo: (layer+3) * 8 bytes. So, at least 24 bytes. Likely 40-60 bytes or more.
  // PageBoundarySort: Always 8 bytes.
  // Let's be conservative. 15:1 distribution between the two.
  max_page_boundary_elements_ = size_in_bytes / (16ULL * 8ULL);
  page_boundary_info_capacity_ = max_page_boundary_elements_ * 15ULL;
  page_boundary_info_ = reinterpret_cast<char*>(get_writer()->get_intermediate_base());
  page_boundary_sort_ = reinterpret_cast<PageBoundarySort*>(
    page_boundary_info_ + page_boundary_info_capacity_ * 8ULL);
}

void MasstreeComposeContext::write_dummy_page_zero() {
  ASSERT_ND(allocated_pages_ == 0);
  Page* dummy_page = page_base_;
  std::memset(dummy_page, 0xDA, sizeof(Page));
  dummy_page->get_header().page_id_ = get_writer()->get_next_page_id();
  dummy_page->get_header().storage_id_ = id_;
  dummy_page->get_header().page_type_ = kMasstreeBorderPageType;
  dummy_page->get_header().snapshot_ = true;
  allocated_pages_ = 1;
}

inline MasstreePage* MasstreeComposeContext::get_page(memory::PagePoolOffset offset) const {
  ASSERT_ND(offset > 0);
  ASSERT_ND(offset < allocated_pages_);
  ASSERT_ND(offset < max_pages_);
  return reinterpret_cast<MasstreePage*>(page_base_ + offset);
}
inline MasstreeIntermediatePage* MasstreeComposeContext::as_intermdiate(MasstreePage* page) const {
  ASSERT_ND(!page->is_border());
  return reinterpret_cast<MasstreeIntermediatePage*>(page);
}
inline MasstreeBorderPage* MasstreeComposeContext::as_border(MasstreePage* page) const {
  ASSERT_ND(page->is_border());
  return reinterpret_cast<MasstreeBorderPage*>(page);
}
inline MasstreePage* MasstreeComposeContext::get_original(memory::PagePoolOffset offset) const {
  ASSERT_ND(offset < kMaxLevels);
  ASSERT_ND(offset < cur_path_levels_);
  return reinterpret_cast<MasstreePage*>(original_base_ + offset);
}

///////////////////////////////////////////////////////////////////////
///
///  MasstreeComposeContext::execute() and subroutines for log groups
///
///////////////////////////////////////////////////////////////////////
ErrorStack MasstreeComposeContext::execute() {
  write_dummy_page_zero();
  CHECK_ERROR(init_root());

#ifndef NDEBUG
  char debug_prev[8192];
  uint16_t debug_prev_length = 0;
#endif  // NDEBUG

  uint64_t processed = 0;
  while (true) {
    CHECK_ERROR(merge_sort_->next_batch());
    uint64_t count = merge_sort_->get_current_count();
    if (count == 0 && merge_sort_->is_ended_all()) {
      break;
    }

#ifndef NDEBUG
    // confirm that it's fully sorted
    for (uint64_t i = 0; i < count; ++i) {
      const MasstreeCommonLogType* entry =
        reinterpret_cast<const MasstreeCommonLogType*>(merge_sort_->resolve_sort_position(i));
      const char* key = entry->get_key();
      uint16_t key_length = entry->key_length_;
      ASSERT_ND(key_length > 0);
      if (key_length < debug_prev_length) {
        ASSERT_ND(std::memcmp(debug_prev, key, key_length) <= 0);
      } else {
        ASSERT_ND(std::memcmp(debug_prev, key, debug_prev_length) <= 0);
      }
      std::memcpy(debug_prev, key, key_length);
      debug_prev_length = key_length;
    }
#endif  // NDEBUG

    // within the batch, we further split them into "groups" that share the same key or log types
    // so that we can process a number of logs in a tight loop.
    uint64_t cur = 0;
    uint32_t group_count = 0;
    while (cur < count) {
      snapshot::MergeSort::GroupifyResult group = merge_sort_->groupify(cur, kMaxLogGroupSize);
      ASSERT_ND(group.count_ <= kMaxLogGroupSize);
      if (LIKELY(group.count_ == 1U)) {  // misprediction is amortized by group, so LIKELY is better
        // no grouping. slowest case. has to be reasonably fast even in this case.
        ASSERT_ND(!group.has_common_key_);
        ASSERT_ND(!group.has_common_log_code_);
        CHECK_ERROR(execute_a_log(cur));
      } else if (group.has_common_key_) {
        CHECK_ERROR(execute_same_key_group(cur, cur + group.count_));
      } else {
        ASSERT_ND(group.has_common_log_code_);
        log::LogCode log_type = group.get_log_type();
        if (log_type == log::kLogCodeMasstreeInsert) {
          CHECK_ERROR(execute_insert_group(cur, cur + group.count_));
        } else if (log_type == log::kLogCodeMasstreeDelete) {
          CHECK_ERROR(execute_delete_group(cur, cur + group.count_));
        } else if (log_type == log::kLogCodeMasstreeUpdate) {
          CHECK_ERROR(execute_update_group(cur, cur + group.count_));
        } else {
          ASSERT_ND(log_type == log::kLogCodeMasstreeOverwrite);
          CHECK_ERROR(execute_overwrite_group(cur, cur + group.count_));
        }
      }
      cur += group.count_;
      processed += group.count_;
      ++group_count;
    }
    ASSERT_ND(cur == count);
    VLOG(1) << storage_ << " processed a batch of " << cur << " logs that has " << group_count
      << " groups";
  }

  VLOG(0) << storage_ << " processed " << processed << " logs. now pages=" << allocated_pages_;
  CHECK_ERROR(flush_buffer());

  // at the end, install pointers to the created snapshot pages except root page.
  uint64_t installed_count = 0;
  CHECK_ERROR(install_snapshot_pointers(&installed_count));
  return kRetOk;
}

ErrorStack MasstreeComposeContext::execute_same_key_group(uint32_t from, uint32_t to) {
  if (from == to) {
    return kRetOk;
  }
  // As these logs are on the same key, we check which logs can be nullified.

  // Let's say I:Insert, U:Update, D:Delete, O:Overwrite
  // overwrite: this is the easiest one that is nullified by following delete/update.
  // insert: if there is following delete, everything in-between disappear, including insert/delete.
  // update: nullified by following delete/update
  // delete: strongest. never nullified except the insert-delete pairing.

  // Merge rule 1: delete+insert in a row can be converted into one update
  // because update is delete+insert. Here, we ignore delete log and change insert log to update.
  // Merge rule 2: insert+update in a row can be converted into one insert

  // As a consequence, we can compact every sequence of logs for the same key into:
  // - Special case. One delete, nothing else. The old page contains a record of the key.
  // - Case A. The old page contains a record of the key:
  //   Zero or one update, followed by zero or more overwrites ("[U]?O*").
  // - Case B. The old page does not contain a record of the key:
  //   One insert, followed by zero or one update, followed by zero or more overwrites ("I[U]?O*").

  // Conversion examples..
  // Case A1: OODIOUO => DIOUO (nullified everything before D)
  //                  => UOUO (DI -> U conversion)
  //                  => UUO (nullified O before U)
  //                  => UO (nullified U before U)
  // Case A2: OODIOUOD => D (nullified everything before D)
  // Case B1: IUUDIOU => IOU (I and D cancel each other)
  //                  => IU (nullified O before U)
  // Case B2: IOUODID => ID (I and D cancel each other)
  //                  => <empty> (I and D cancel each other)


  // Iff it starts with an insert, the old page doesn't have a record of the key.
  const bool starts_with_insert
    = (merge_sort_->get_log_type_from_sort_position(from) == log::kLogCodeMasstreeInsert);

  // Index of non-nullified delete. if this is != to, it's the special case above.
  // Thus, this value must be "to - 1" in that case.
  uint32_t last_active_delete = to;
  // First, look for the last delete. It determines many things if exists.
  ASSERT_ND(to > 0);
  for (uint32_t i = static_cast<uint32_t>(to - 1); i >= from; ++i) {
    log::LogCode log_type = merge_sort_->get_log_type_from_sort_position(i);
    if (log_type == log::kLogCodeMasstreeDelete) {
      ASSERT_ND(last_active_delete == to);
      last_active_delete = i;
      // Okay, found a delete. Everything before this will be nullified.

#ifndef NDEBUG
      // Let's do sanity check. Is it consistent with the number of preceding inserts/deletes?
      uint32_t insert_count = 0;
      uint32_t delete_count = 0;
      for (uint32_t j = from; j < i; ++j) {
        log::LogCode log_type_j = merge_sort_->get_log_type_from_sort_position(j);
        switch (log_type_j) {
        case log::kLogCodeMasstreeInsert:
          ASSERT_ND((starts_with_insert && insert_count == delete_count)
            || (!starts_with_insert && insert_count + 1U == delete_count));
          ++insert_count;
          break;
        case log::kLogCodeMasstreeDelete:
          ASSERT_ND((!starts_with_insert && insert_count == delete_count)
            || (starts_with_insert && insert_count == delete_count + 1U));
          ++delete_count;
          break;
        default:
          ASSERT_ND(log_type_j == log::kLogCodeMasstreeUpdate
            || log_type_j == log::kLogCodeMasstreeOverwrite);
          ASSERT_ND((!starts_with_insert && insert_count == delete_count)
            || (starts_with_insert && insert_count == delete_count + 1U));
          break;
        }
      }

      // it ends with a delete, so there must be something to delete.
      ASSERT_ND((!starts_with_insert && insert_count == delete_count)
        || (starts_with_insert && insert_count == delete_count + 1U));
#endif  // NDEBUG

      break;
    }
  }

  // Index of non-nullified insert. if this is != to, it's always case B and all other active logs
  // are after this.
  uint32_t last_active_insert = to;
  // Whether the last active insert is a "merged" insert, meaning an update
  // following an insert. In this case, we treat the update-log as if it's an insert log.
  bool is_last_active_insert_merged = false;

  // Index of non-nullified update. if this is != to, all active overwrite logs are after this.
  uint32_t last_active_update = to;
  // Whether the last active update is a "merged" update, meaning an insert
  // following a delete. In this case, we treat the insert-log as if it's an update log.
  bool is_last_active_update_merged = false;

  uint32_t next_to_check = from;
  if (last_active_delete != to) {
    ASSERT_ND(last_active_delete >= from && last_active_delete < to);
    if (last_active_delete + 1U == to) {
      // This delete concludes the series of logs. Great!
      if (starts_with_insert) {
        DVLOG(1) << "yay, same-key-group completely eliminated a group of "
          << (to - from) << " logs";
        // TASK(Hideaki) assertion to check if the old snapshot doesn't contain this key.
      } else {
        DVLOG(1) << "yay, same-key-group compressed " << (to - from) << " logs into a delete";
        CHECK_ERROR(execute_a_log(last_active_delete));
      }

      return kRetOk;
    } else {
      // The only possible log right after delete is insert.
      uint32_t next = last_active_delete + 1U;
      log::LogCode next_type = merge_sort_->get_log_type_from_sort_position(next);
      ASSERT_ND(next_type == log::kLogCodeMasstreeInsert);

      if (starts_with_insert) {
        // I...DI,,,
        // We are sure the ... has the same number of I/D.
        // So, we just cancel them each other, turning into I,,,
        DVLOG(2) << "I and D cancelled each other. from=" << from
          << " delete at=" << last_active_delete;
        last_active_insert = next;
      } else {
        // ...DI,,,
        // In ..., #-of-I == #-of-D
        // Now the merge rule 1 applies. D+I => U, turning into U,,,
        DVLOG(2) << "D+I merged into U. from=" << from << " delete at=" << last_active_delete;
        last_active_update = next;
        is_last_active_update_merged = true;
      }
      next_to_check = next + 1U;
      last_active_delete = to;
    }
  }

  // From now on, we are sure there is no more delete or insert.
  // We just look for updates, which can nullify or merge into other operations.
  ASSERT_ND(next_to_check >= from);
  ASSERT_ND(last_active_delete == to);
  for (uint32_t i = next_to_check; i < to; ++i) {
    log::LogCode log_type = merge_sort_->get_log_type_from_sort_position(i);
    ASSERT_ND(log_type != log::kLogCodeMasstreeInsert);
    ASSERT_ND(log_type != log::kLogCodeMasstreeDelete);
    if (log_type == log::kLogCodeMasstreeUpdate) {
      // Update nullifies the preceding overwrite, and also merges with the preceding insert.
      if (last_active_insert != to) {
        DVLOG(2) << "U replaced all preceding I and O, turning into a new I. i=" << i;
        ASSERT_ND(last_active_update == to);  // then it should have already merged
        ASSERT_ND(!is_last_active_update_merged);
        last_active_insert = i;
        is_last_active_insert_merged = true;
      } else {
        DVLOG(2) << "U nullified all preceding O and U. i=" << i;
        last_active_update = i;
        is_last_active_update_merged = false;
      }
    } else {
      // Overwrites are just skipped.
      ASSERT_ND(log_type == log::kLogCodeMasstreeOverwrite);
      ASSERT_ND(starts_with_insert || last_active_insert != to);
    }
  }

  // After these conversions, the only remaining log patterns are:
  //   a) "IO*"  : the data page doesn't have the key.
  //   b) "U?O*" : the data page already has the key.
  ASSERT_ND(last_active_delete == to);
  uint32_t cur = from;
  if (last_active_insert != to || last_active_update != to) {
    // Process I/U separately. The I/U might be a merged one.
    log::LogCode type_before_merge;
    log::LogCode type_after_merge;
    if (last_active_insert != to) {
      ASSERT_ND(last_active_update == to);
      ASSERT_ND(!is_last_active_update_merged);
      type_after_merge = log::kLogCodeMasstreeInsert;
      cur = last_active_insert;
      if (is_last_active_insert_merged) {
        type_before_merge = log::kLogCodeMasstreeUpdate;
      } else {
        type_before_merge = log::kLogCodeMasstreeInsert;
      }
    } else {
      ASSERT_ND(!is_last_active_insert_merged);
      type_after_merge = log::kLogCodeMasstreeUpdate;
      cur = last_active_update;
      if (is_last_active_update_merged) {
        type_before_merge = log::kLogCodeMasstreeInsert;
      } else {
        type_before_merge = log::kLogCodeMasstreeUpdate;
      }
    }

    ASSERT_ND(type_before_merge == merge_sort_->get_log_type_from_sort_position(cur));
    if (type_after_merge != type_before_merge) {
      DVLOG(2) << "Disguising log type for merge. cur=" << cur
        << ", type_before_merge=" << type_before_merge << ", type_after_merge=" << type_after_merge;
      // Change log type. Insert and Update have exactly same log layout, so it's safe.
      merge_sort_->change_log_type_at(cur, type_before_merge, type_after_merge);
    }

    // Process the I/U as usual. This also makes sure that the tail-record is the key.
  } else {
    ASSERT_ND(log::kLogCodeMasstreeOverwrite == merge_sort_->get_log_type_from_sort_position(cur));
    // All logs are overwrites.
    // Even in this case, we must process the first log as usual so that
    // the tail-record in the tail page points to the record.
  }
  ASSERT_ND(cur < to);
  CHECK_ERROR(execute_a_log(cur));
  ++cur;
  if (cur == to) {
    return kRetOk;
  }

  // All the followings are overwrites.
  // Process the remaining overwrites in a tight loop.
  // We made sure sure the tail-record in the tail page points to the record.
  PathLevel* last = get_last_level();
  ASSERT_ND(get_page(last->tail_)->is_border());
  MasstreeBorderPage* page = as_border(get_page(last->tail_));
  uint16_t key_count = page->get_key_count();
  ASSERT_ND(key_count > 0);
  uint16_t index = key_count - 1;
  ASSERT_ND(!page->does_point_to_layer(index));
  char* record = page->get_record(index);

  for (uint32_t i = cur; i < to; ++i) {
    const MasstreeOverwriteLogType* casted =
      reinterpret_cast<const MasstreeOverwriteLogType*>(merge_sort_->resolve_sort_position(i));
    ASSERT_ND(casted->header_.get_type() == log::kLogCodeMasstreeOverwrite);
    ASSERT_ND(page->equal_key(index, casted->get_key(), casted->key_length_));

    // Also, we look for a chance to ignore redundant overwrites.
    // If next overwrite log covers the same or more data range, we can skip the log.
    // Ideally, we should have removed such logs back in mappers.
    if (i < to) {
      const MasstreeOverwriteLogType* next =
        reinterpret_cast<const MasstreeOverwriteLogType*>(
          merge_sort_->resolve_sort_position(i + 1U));
      if ((next->payload_offset_ <= casted->payload_offset_)
        && (next->payload_offset_ + next->payload_count_
          >= casted->payload_offset_ + casted->payload_count_)) {
        DVLOG(3) << "Skipped redundant overwrites";
        continue;
      }
    }

    casted->apply_record(nullptr, id_, page->get_owner_id(index), record);
  }
  return kRetOk;
}

ErrorStack MasstreeComposeContext::execute_insert_group(uint32_t from, uint32_t to) {
  uint32_t cur = from;
  while (cur < to) {
    CHECK_ERROR(flush_if_nearly_full());  // so that the append loop doesn't have to check

    // let's see if the remaining logs are smaller than existing records or the page boundary.
    // if so, we can simply create new pages for a (hopefully large) number of logs.
    // in the worst case, it's like a marble (newrec, oldrec, newrec, oldrec, ...) in a page
    // or even worse a newrec in a page, a newrec in another page, ... If that is the case,
    // we insert as usual. but, in many cases a group of inserts create a new region.
    KeySlice min_slice;
    KeySlice max_slice;
    uint32_t run_count = execute_insert_group_get_cur_run(cur, to, &min_slice, &max_slice);
    if (run_count <= 1U) {
      ASSERT_ND(merge_sort_->get_log_type_from_sort_position(cur) == log::kLogCodeMasstreeInsert);
      CHECK_ERROR(execute_a_log(cur));
      ++cur;
      continue;
    }

    DVLOG(2) << "Great, " << run_count << " insert-logs to process in the current context.";

    PathLevel* last = get_last_level();
    ASSERT_ND(get_page(last->tail_)->is_border());
    MasstreeBorderPage* page = as_border(get_page(last->tail_));
    // now these logs are guaranteed to be:
    //  1) fence-wise fit in the current B-tree page (of course might need page splits).
    //  2) can be stored in the current B-trie layer.
    //  3) no overlap with existing records.
    // thus, we just create a sequence of new B-tree pages next to the current page.

    // first, let's get hints about new page boundaries to align well with volatile pages.
    ASSERT_ND(min_slice >= page->get_low_fence());
    ASSERT_ND(max_slice <= page->get_high_fence());
    uint32_t found_boundaries_count = 0;
    MasstreeStorage::PeekBoundariesArguments args = {
      cur_prefix_slices_,
      last->layer_,
      kTmpBoundaryArraySize,
      min_slice,
      max_slice,
      tmp_boundary_array_,
      &found_boundaries_count };
    WRAP_ERROR_CODE(storage_.peek_volatile_page_boundaries(engine_, args));
    ASSERT_ND(found_boundaries_count <= kTmpBoundaryArraySize);
    DVLOG(2) << found_boundaries_count << " boundary hints for " << run_count << " insert-logs.";
    if (found_boundaries_count >= kTmpBoundaryArraySize) {
      LOG(INFO) << "holy crap, we get more than " << kTmpBoundaryArraySize << " page boundaries"
        << " as hints for " << run_count << " insert-logs!!";
      // as this is just a hint, we can go on. if this often happens, reduce kMaxLogGroupSize.
      // or increase kTmpBoundaryArraySize.
    }

    // then, append all of them in a tight loop
    WRAP_ERROR_CODE(execute_insert_group_append_loop(cur, cur + run_count, found_boundaries_count));
    cur += run_count;
  }

  ASSERT_ND(cur == to);
  return kRetOk;
}

ErrorCode MasstreeComposeContext::execute_insert_group_append_loop(
  uint32_t from,
  uint32_t to,
  uint32_t hint_count) {
  const uint16_t kFetch = 8;  // parallel fetch to amortize L1 cachemiss cost.
  const KeySlice* hints = tmp_boundary_array_;
  const log::RecordLogType* logs[kFetch];
  uint32_t cur = from;
  uint32_t next_hint = 0;
  uint32_t dubious_hints = 0;  // # of previous hints that resulted in too-empty pages
  PathLevel* last = get_last_level();
  ASSERT_ND(get_page(last->tail_)->is_border());
  MasstreeBorderPage* page = as_border(get_page(last->tail_));
  uint8_t cur_layer = page->get_layer();

  while (cur < to) {
    uint16_t fetch_count = std::min<uint32_t>(kFetch, to - cur);
    uint16_t actual_count = merge_sort_->fetch_logs(cur, fetch_count, logs);
    ASSERT_ND(actual_count == fetch_count);

    for (uint32_t i = 0; i < actual_count; ++i) {
      // hints[next_hint] tells when we should close the current page. check if we overlooked it.
      // whether we followed the hints or not, we advance next_hint, so the followings always hold.
      ASSERT_ND(next_hint >= hint_count || page->get_low_fence() < hints[next_hint]);
      ASSERT_ND(next_hint >= hint_count
        || page->get_key_count() == 0
        || page->get_slice(page->get_key_count() - 1) < hints[next_hint]);

      const MasstreeInsertLogType* entry
        = reinterpret_cast<const MasstreeInsertLogType*>(ASSUME_ALIGNED(logs[i], 8));
      ASSERT_ND(entry->header_.get_type() == log::kLogCodeMasstreeInsert);
      const char* key = entry->get_key();
      uint16_t key_length = entry->key_length_;

#ifndef NDEBUG
      ASSERT_ND(key_length > cur_layer * sizeof(KeySlice));
      for (uint8_t layer = 0; layer < cur_layer; ++layer) {
        KeySlice prefix_slice = normalize_be_bytes_full_aligned(key + layer * sizeof(KeySlice));
        ASSERT_ND(prefix_slice == cur_prefix_slices_[layer]);
      }
#endif  // NDEBUG

      uint16_t remaining_length = key_length - cur_layer * sizeof(KeySlice);
      const char* suffix = key + (cur_layer + 1) * sizeof(KeySlice);
      KeySlice slice = normalize_be_bytes_full_aligned(key + cur_layer * sizeof(KeySlice));
      ASSERT_ND(page->within_fences(slice));
      uint8_t key_count = page->get_key_count();
      ASSERT_ND(key_count == 0 || slice > page->get_slice(key_count - 1));
      uint16_t payload_count = entry->payload_count_;
      const char* payload = entry->get_payload();
      xct::XctId xct_id = entry->header_.xct_id_;
      ASSERT_ND(!merge_sort_->get_base_epoch().is_valid()
        || xct_id.get_epoch() >= merge_sort_->get_base_epoch());

      // If the hint says the new record should be in a new page, we close the current page
      // EXCEPT when we are getting too many cases that result in too-empty pages.
      // It can happen when the volatile world is way ahead of the snapshotted epochs.
      // We still have to tolerate some too-empty pages, otherwise, :
      //   hint tells: 0-10, 10-20, 20-30, ...
      //   the first page for some reason became full at 0-8. when processing 10, the new page is
      //   still almost empty, we ignore the hint. close at 10-18. same thing happens at 20, ...
      // Hence, we ignore hints if kDubiousHintsThreshold previous hints caused too empty pages.
      const uint32_t kDubiousHintsThreshold = 2;  // to tolerate more, increase this.
      const uint16_t kTooEmptySize = kBorderPageDataPartSize * 2 / 10;
      bool page_switch_hinted = false;
      KeySlice hint_suggested_slice = kInfimumSlice;
      if (UNLIKELY(next_hint < hint_count && slice >= hints[next_hint])) {
        DVLOG(3) << "the hint tells that we should close the page now.";
        bool too_empty = key_count == 0 || page->available_space() <= kTooEmptySize;
        if (too_empty) {
          DVLOG(1) << "however, the page is still quite empty!";
          if (dubious_hints >= kDubiousHintsThreshold) {
            DVLOG(1) << "um, let's ignore the hints.";
          } else {
            DVLOG(1) << "neverthelss, let's trust the hints for now.";
            ++dubious_hints;
            page_switch_hinted = true;
            hint_suggested_slice = hints[next_hint];
          }
        }
        // consume all hints up to this slice.
        ++next_hint;
        while (next_hint < hint_count && slice >= hints[next_hint]) {
          ++next_hint;
        }
      }

      if (UNLIKELY(page_switch_hinted)
        || UNLIKELY(!page->can_accomodate(key_count, remaining_length, payload_count))) {
        // unlike append_border_newpage(), which is used in the per-log method, this does no
        // page migration. much simpler and faster.
        memory::PagePoolOffset new_offset = allocate_page();
        SnapshotPagePointer new_page_id = page_id_base_ + new_offset;
        MasstreeBorderPage* new_page = reinterpret_cast<MasstreeBorderPage*>(get_page(new_offset));

        KeySlice middle;
        if (page_switch_hinted) {
          ASSERT_ND(hint_suggested_slice > page->get_low_fence());
          ASSERT_ND(hint_suggested_slice <= slice);
          ASSERT_ND(hint_suggested_slice < page->get_high_fence());
          middle = hint_suggested_slice;
        } else {
          middle = slice;
        }

        KeySlice high_fence = page->get_high_fence();
        new_page->initialize_snapshot_page(id_, new_page_id, cur_layer, middle, high_fence);
        page->set_foster_major_offset_unsafe(new_offset);  // set next link
        page->set_high_fence_unsafe(middle);
        last->tail_ = new_offset;
        ++last->page_count_;
        page = new_page;
        key_count = 0;
      }

      ASSERT_ND(page->can_accomodate(key_count, remaining_length, payload_count));
      page->reserve_record_space(key_count, xct_id, slice, suffix, remaining_length, payload_count);
      page->increment_key_count();
      fill_payload_padded(page->get_record_payload(key_count), payload, payload_count);
    }

    cur += actual_count;
  }
  ASSERT_ND(cur == to);
  return kErrorCodeOk;
}

uint32_t MasstreeComposeContext::execute_insert_group_get_cur_run(
  uint32_t cur,
  uint32_t to,
  KeySlice* min_slice,
  KeySlice* max_slice) {
  ASSERT_ND(cur < to);
  ASSERT_ND(merge_sort_->get_log_type_from_sort_position(cur) == log::kLogCodeMasstreeInsert);
  *min_slice = kInfimumSlice;
  *max_slice = kInfimumSlice;
  if (cur_path_levels_ == 0) {
    // initial call. even get_last_level() doesn't work. process it as usual
    return 0;
  }

  PathLevel* last = get_last_level();
  MasstreePage* tail_page = get_page(last->tail_);
  if (!tail_page->is_border()) {
    LOG(WARNING) << "um, why this can happen? anyway, let's process it per log";
    return 0;
  }

  const MasstreeBorderPage* page = as_border(tail_page);
  const KeySlice low_fence = page->get_low_fence();
  const KeySlice high_fence = page->get_high_fence();
  const uint8_t existing_keys = page->get_key_count();
  const KeySlice existing_slice
    = existing_keys == 0 ? kInfimumSlice : page->get_slice(existing_keys - 1);
  ASSERT_ND(last->layer_ == page->get_layer());
  const uint8_t prefix_count = last->layer_;
  const snapshot::MergeSort::SortEntry* sort_entries = merge_sort_->get_sort_entries();
  uint32_t run_count;
  KeySlice prev_slice = existing_slice;
  if (prefix_count == 0) {
    // optimized impl for this common case. sort_entries provides enough information.
    for (run_count = 0; cur + run_count < to; ++run_count) {
      KeySlice slice = sort_entries[cur + run_count].get_key();  // sole key of interest.
      ASSERT_ND(slice >= low_fence);  // because logs are sorted
      if (slice >= high_fence) {  // page boundary
        return run_count;
      }
      if (slice == prev_slice) {  // next-layer
        // if both happen to be 0 (infimum) or key_length is not a multiply of 8, this check is
        // conservative. but, it's rare, so okay to process them per log.
        return run_count;
      }
      if (last->has_next_original() && slice >= last->next_original_slice_) {  // original record
        // this is a conservative break because we don't check the key length and it's ">=".
        // it's fine, if that happens, we just process them per log.
        return run_count;
      }

      prev_slice = slice;
      if (run_count == 0) {
        *min_slice = slice;
      } else {
        ASSERT_ND(slice > *min_slice);
      }
      *max_slice = slice;
    }
  } else {
    // then, a bit more expensive. we might have to grab the log itself, which causes L1 cachemiss.
    for (run_count = 0; cur + run_count < to; ++run_count) {
      // a low-cost check first. match the first prefix layer
      KeySlice first_slice = sort_entries[cur + run_count].get_key();
      ASSERT_ND(first_slice >= cur_prefix_slices_[0]);  // because logs are sorted
      if (first_slice != cur_prefix_slices_[0]) {
        return run_count;
      }

      // ah, we have to grab the log.
      const MasstreeCommonLogType* entry =
        reinterpret_cast<const MasstreeCommonLogType*>(
          merge_sort_->resolve_sort_position(cur + run_count));
      const char* key = entry->get_key();
      uint16_t key_length = entry->key_length_;
      ASSERT_ND(is_key_aligned_and_zero_padded(key, key_length));
      if (key_length <= prefix_count * sizeof(KeySlice)) {
        return run_count;
      }

      // match remaining prefix layers
      for (uint8_t layer = 1; layer < prefix_count; ++layer) {
        KeySlice prefix_slice = normalize_be_bytes_full_aligned(key + layer * sizeof(KeySlice));
        ASSERT_ND(prefix_slice >= cur_prefix_slices_[layer]);  // because logs are sorted
        if (prefix_slice != cur_prefix_slices_[layer]) {
          return run_count;
        }
      }

      // then same checks
      KeySlice slice = normalize_be_bytes_full_aligned(key + prefix_count * sizeof(KeySlice));
      ASSERT_ND(slice >= low_fence);  // because logs are sorted
      if (slice >= high_fence
        || slice == prev_slice
        || (last->has_next_original() && slice >= last->next_original_slice_)) {
        return run_count;
      }

      prev_slice = slice;
      if (run_count == 0) {
        *min_slice = slice;
      } else {
        ASSERT_ND(slice > *min_slice);
      }
      *max_slice = slice;
    }
  }
  ASSERT_ND(cur + run_count == to);  // because above code returns rather than breaks.
  return run_count;
}

ErrorStack MasstreeComposeContext::execute_delete_group(uint32_t from, uint32_t to) {
  // TASK(Hideaki) batched impl. but this case is not common, so later, later..
  for (uint32_t i = from; i < to; ++i) {
    CHECK_ERROR(execute_a_log(i));
  }
  return kRetOk;
}

ErrorStack MasstreeComposeContext::execute_update_group(uint32_t from, uint32_t to) {
  // TASK(Hideaki) batched impl. but this case is not common, so later, later..
  for (uint32_t i = from; i < to; ++i) {
    CHECK_ERROR(execute_a_log(i));
  }
  return kRetOk;
}

ErrorStack MasstreeComposeContext::execute_overwrite_group(uint32_t from, uint32_t to) {
  // TASK(Hideaki) batched impl
  for (uint32_t i = from; i < to; ++i) {
    CHECK_ERROR(execute_a_log(i));
  }
  return kRetOk;
}

inline ErrorStack MasstreeComposeContext::execute_a_log(uint32_t cur) {
  ASSERT_ND(cur < merge_sort_->get_current_count());

  const MasstreeCommonLogType* entry =
    reinterpret_cast<const MasstreeCommonLogType*>(merge_sort_->resolve_sort_position(cur));
  const char* key = entry->get_key();
  uint16_t key_length = entry->key_length_;
  ASSERT_ND(key_length > 0);
  ASSERT_ND(cur_path_levels_ == 0 || get_page(get_last_level()->tail_)->is_border());
  ASSERT_ND(is_key_aligned_and_zero_padded(key, key_length));

  if (UNLIKELY(does_need_to_adjust_path(key, key_length))) {
    CHECK_ERROR(adjust_path(key, key_length));
  }

  PathLevel* last = get_last_level();
  ASSERT_ND(get_page(last->tail_)->is_border());
  KeySlice slice = normalize_be_bytes_full_aligned(key + last->layer_ * kSliceLen);
  ASSERT_ND(last->contains_slice(slice));

  // we might have to consume original records before processing this log.
  if (last->needs_to_consume_original(slice, key_length)) {
    CHECK_ERROR(consume_original_upto_border(slice, key_length, last));
    ASSERT_ND(!last->needs_to_consume_original(slice, key_length));
  }

  MasstreeBorderPage* page = as_border(get_page(last->tail_));
  uint16_t key_count = page->get_key_count();

  // we might have to follow next-layer pointers. Check it.
  // notice it's "while", not "if", because we might follow more than one next-layer pointer
  while (key_count > 0
      && key_length > (last->layer_ + 1U) * kSliceLen
      && page->get_slice(key_count - 1) == slice
      && page->get_remaining_key_length(key_count - 1) > kSliceLen) {
    // then we have to either go next layer.
    if (page->does_point_to_layer(key_count - 1)) {
      // the next layer already exists. just follow it.
      ASSERT_ND(page->get_next_layer(key_count - 1)->volatile_pointer_.is_null());
      SnapshotPagePointer* next_pointer = &page->get_next_layer(key_count - 1)->snapshot_pointer_;
      CHECK_ERROR(open_next_level(key, key_length, page, slice, next_pointer));
    } else {
      // then we have to newly create a layer.
      ASSERT_ND(page->will_conflict(key_count - 1, slice, key_length - last->layer_ * kSliceLen));
      open_next_level_create_layer(key, key_length, page, slice, key_count - 1);
      ASSERT_ND(page->does_point_to_layer(key_count - 1));
    }

    // in either case, we went deeper. retrieve these again
    last = get_last_level();
    ASSERT_ND(get_page(last->tail_)->is_border());
    slice = normalize_be_bytes_full_aligned(key + last->layer_ * kSliceLen);
    ASSERT_ND(last->contains_slice(slice));
    page = as_border(get_page(last->tail_));
    key_count = page->get_key_count();
  }

  // Now we are sure the tail of the last level is the only relevant record. process the log.
  if (entry->header_.get_type() == log::kLogCodeMasstreeOverwrite) {
    // [Overwrite] simply reuse log.apply
    uint16_t index = key_count - 1;
    ASSERT_ND(!page->does_point_to_layer(index));
    ASSERT_ND(page->equal_key(index, key, key_length));
    char* record = page->get_record(index);
    const MasstreeOverwriteLogType* casted
      = reinterpret_cast<const MasstreeOverwriteLogType*>(entry);
    casted->apply_record(nullptr, id_, page->get_owner_id(index), record);
  } else {
    // DELETE/INSERT/UPDATE
    ASSERT_ND(
      entry->header_.get_type() == log::kLogCodeMasstreeDelete
      || entry->header_.get_type() == log::kLogCodeMasstreeInsert
      || entry->header_.get_type() == log::kLogCodeMasstreeUpdate);

    if (entry->header_.get_type() == log::kLogCodeMasstreeDelete
      || entry->header_.get_type() == log::kLogCodeMasstreeUpdate) {
      // [Delete/Update] Physically deletes the last record in this page.
      uint16_t index = key_count - 1;
      ASSERT_ND(!page->does_point_to_layer(index));
      ASSERT_ND(page->equal_key(index, key, key_length));
      page->set_key_count(index);
    }
    // Notice that this is "if", not "else if". UPDATE = DELETE + INSERT.
    if (entry->header_.get_type() == log::kLogCodeMasstreeInsert
      || entry->header_.get_type() == log::kLogCodeMasstreeUpdate) {
      // [Insert/Update] next-layer is already handled above, so just append it.
      ASSERT_ND(entry->header_.get_type() == log::kLogCodeMasstreeInsert);
      ASSERT_ND(key_count == 0 || !page->equal_key(key_count - 1, key, key_length));  // no dup
      uint16_t skip = last->layer_ * kSliceLen;
      append_border(
        slice,
        entry->header_.xct_id_,
        key_length - skip,
        key + skip + kSliceLen,
        entry->payload_count_,
        entry->get_payload(),
        last);
    }
  }

  CHECK_ERROR(flush_if_nearly_full());

  return kRetOk;
}

///////////////////////////////////////////////////////////////////////
///
///  MasstreeComposeContext open/close/init of path levels
///
///////////////////////////////////////////////////////////////////////
ErrorStack MasstreeComposeContext::init_root() {
  SnapshotPagePointer snapshot_page_id
    = storage_.get_control_block()->root_page_pointer_.snapshot_pointer_;
  if (snapshot_page_id != 0) {
    // based on previous snapshot page.
    ASSERT_ND(page_boundary_info_cur_pos_ == 0 && page_boundary_elements_ == 0);
    WRAP_ERROR_CODE(args_.previous_snapshot_files_->read_page(snapshot_page_id, root_));
  } else {
    // if this is the initial snapshot, we create a dummy image of the root page based on
    // partitioning. all composers in all reducers follow the same protocol so that
    // we can easily merge their outputs.
    PartitionerMetadata* metadata = PartitionerMetadata::get_metadata(engine_, id_);
    ASSERT_ND(metadata->valid_);
    MasstreePartitionerData* data
      = reinterpret_cast<MasstreePartitionerData*>(metadata->locate_data(engine_));
    ASSERT_ND(data->partition_count_ > 0);
    ASSERT_ND(data->partition_count_ < kMaxIntermediatePointers);

    root_->initialize_snapshot_page(id_, 0, 0, 1, kInfimumSlice, kSupremumSlice);
    ASSERT_ND(data->low_keys_[0] == kInfimumSlice);
    root_->get_minipage(0).pointers_[0].snapshot_pointer_ = 0;
    for (uint16_t i = 1; i < data->partition_count_; ++i) {
      root_->append_pointer_snapshot(data->low_keys_[i], 0);
    }
  }

  return kRetOk;
}

ErrorStack MasstreeComposeContext::open_first_level(const char* key, uint16_t key_length) {
  ASSERT_ND(cur_path_levels_ == 0);
  ASSERT_ND(key_length > 0);
  ASSERT_ND(is_key_aligned_and_zero_padded(key, key_length));
  PathLevel* first = cur_path_;

  KeySlice slice = normalize_be_bytes_full_aligned(key);
  uint8_t index = root_->find_minipage(slice);
  MasstreeIntermediatePage::MiniPage& minipage = root_->get_minipage(index);
  uint8_t index_mini = minipage.find_pointer(slice);
  SnapshotPagePointer* pointer_address = &minipage.pointers_[index_mini].snapshot_pointer_;
  SnapshotPagePointer old_pointer = *pointer_address;
  root_index_ = index;
  root_index_mini_ = index_mini;

  KeySlice low_fence;
  KeySlice high_fence;
  root_->extract_separators_snapshot(index, index_mini, &low_fence, &high_fence);

  if (old_pointer != 0)  {
    // When we are opening an existing page, we digg further until we find a border page
    // to update/insert/delete the given key. this might recurse
    CHECK_ERROR(open_next_level(key, key_length, root_, kInfimumSlice, pointer_address));
  } else {
    // this should happen only while an initial snapshot for this storage.
    ASSERT_ND(storage_.get_control_block()->root_page_pointer_.snapshot_pointer_ == 0);
    // newly creating a whole subtree. Start with a border page.
    first->layer_ = 0;
    first->head_ = allocate_page();
    first->page_count_ = 1;
    first->tail_ = first->head_;
    first->set_no_more_next_original();
    first->low_fence_ = low_fence;
    first->high_fence_ = high_fence;
    MasstreePage* page = get_page(first->head_);
    SnapshotPagePointer page_id = page_id_base_ + first->head_;
    *pointer_address = page_id;
    MasstreeBorderPage* casted = reinterpret_cast<MasstreeBorderPage*>(page);
    casted->initialize_snapshot_page(id_, page_id, 0, low_fence, high_fence);
    page->header().page_id_ = page_id;
    ++cur_path_levels_;
    // this is it. this is an easier case. no recurse.
  }

  ASSERT_ND(cur_path_levels_ > 0);
  ASSERT_ND(first->low_fence_ == low_fence);
  ASSERT_ND(first->high_fence_ == high_fence);
  ASSERT_ND(minipage.pointers_[index_mini].snapshot_pointer_ != old_pointer);
  return kRetOk;
}

ErrorStack MasstreeComposeContext::open_next_level(
  const char* key,
  uint16_t key_length,
  MasstreePage* parent,
  KeySlice prefix_slice,
  SnapshotPagePointer* next_page_id) {
  ASSERT_ND(next_page_id);
  ASSERT_ND(key_length > 0);
  ASSERT_ND(is_key_aligned_and_zero_padded(key, key_length));
  SnapshotPagePointer old_page_id = *next_page_id;
  ASSERT_ND(old_page_id != 0);
  uint8_t this_level = cur_path_levels_;
  PathLevel* level = cur_path_ + this_level;
  ++cur_path_levels_;

  if (parent->is_border()) {
    store_cur_prefix(parent->get_layer(), prefix_slice);
    level->layer_ = parent->get_layer() + 1;
  } else {
    level->layer_ = parent->get_layer();
  }
  level->head_ = allocate_page();
  level->page_count_ = 1;
  level->tail_ = level->head_;
  level->set_no_more_next_original();

  SnapshotPagePointer page_id = page_id_base_ + level->head_;
  *next_page_id = page_id;
  uint16_t skip = level->layer_ * kSliceLen;
  KeySlice slice = normalize_be_bytes_full_aligned(key + skip);

  MasstreePage* target = get_page(level->head_);
  MasstreePage* original = get_original(this_level);
  WRAP_ERROR_CODE(args_.previous_snapshot_files_->read_page(old_page_id, original));
  level->low_fence_ = original->get_low_fence();
  level->high_fence_ = original->get_high_fence();

  snapshot::SnapshotId old_snapshot_id = extract_snapshot_id_from_snapshot_pointer(old_page_id);
  ASSERT_ND(old_snapshot_id != snapshot::kNullSnapshotId);
  if (UNLIKELY(old_snapshot_id == snapshot_id_)) {
    // if we are re-opening a page we have created in this execution,
    // we have to remove the old entry in page_boundary_info we created when we closed it.
    remove_old_page_boundary_info(old_page_id, original);
  }

  // We need to determine up to which records are before/equal to the key.
  // target page is initialized with the original page, but we will reduce key_count
  // to omit entries after the key.
  std::memcpy(target, original, kPageSize);
  target->header().page_id_ = page_id;

  if (original->is_border()) {
    // When this is a border page we might or might not have to go down.
    const MasstreeBorderPage* casted = as_border(original);
    uint16_t key_count = original->get_key_count();
    auto result = casted->find_key_for_snapshot(slice, key + skip, key_length - skip);

    uint16_t copy_count;
    bool go_down = false;
    bool create_layer = false;
    switch (result.match_type_) {
    case MasstreeBorderPage::kNotFound:
      copy_count = result.index_;  // do not copy the next key yet. it's larger than key
      break;
    case MasstreeBorderPage::kExactMatchLocalRecord:
      ASSERT_ND(result.index_ < key_count);
      copy_count = result.index_ + 1U;  // copy the matched key too
      break;
    case MasstreeBorderPage::kConflictingLocalRecord:
      go_down = true;
      create_layer = true;
      ASSERT_ND(result.index_ < key_count);
      copy_count = result.index_ + 1U;  // copy the matched key too
      break;
    default:
      ASSERT_ND(result.match_type_ == MasstreeBorderPage::kExactMatchLayerPointer);
      go_down = true;
      ASSERT_ND(result.index_ < key_count);
      copy_count = result.index_ + 1U;  // copy the matched key too
      break;
    }

    MasstreeBorderPage* target_casted = as_border(target);
    ASSERT_ND(copy_count <= key_count);
    target_casted->set_key_count(copy_count);
    level->next_original_ = copy_count + 1U;
    if (level->next_original_ >= key_count) {
      level->set_no_more_next_original();
    } else {
      level->next_original_slice_ = casted->get_slice(level->next_original_);
      level->next_original_remaining_ = casted->get_remaining_key_length(level->next_original_);
    }

    // now, do we have to go down further?
    if (!go_down) {
      return kRetOk;  // we are done
    }

    ASSERT_ND(key_length > kSliceLen + skip);  // otherwise cant go down next laye

    // go down, we don't have to worry about moving more original records to next layer.
    // if there are such records, the snapshot page has already done so.
    ASSERT_ND(!level->has_next_original() || level->next_original_slice_ > slice);  // check it

    uint8_t next_layer_index = target_casted->get_key_count() - 1;
    ASSERT_ND(target_casted->get_slice(next_layer_index) == slice);
    if (create_layer) {
      // this is more complicated. we have to migrate the current record to next layer
      open_next_level_create_layer(key, key_length, target_casted, slice, next_layer_index);
      ASSERT_ND(target_casted->does_point_to_layer(next_layer_index));
    } else {
      // then we follow the existing next layer pointer.
      ASSERT_ND(target_casted->does_point_to_layer(next_layer_index));
      ASSERT_ND(target_casted->get_next_layer(next_layer_index)->volatile_pointer_.is_null());
      SnapshotPagePointer* next_layer
        = &target_casted->get_next_layer(next_layer_index)->snapshot_pointer_;
      CHECK_ERROR(open_next_level(key, key_length, target_casted, slice, next_layer));
    }
    ASSERT_ND(cur_path_levels_ > this_level + 1U);
  } else {
    const MasstreeIntermediatePage* casted = as_intermdiate(original);
    uint8_t index = casted->find_minipage(slice);
    const MasstreeIntermediatePage::MiniPage& minipage = casted->get_minipage(index);
    uint8_t index_mini = minipage.find_pointer(slice);

    // inclusively up to index-index_mini can be copied. note that this also means
    // index=0/index_mini=0 is always guaranteed.
    MasstreeIntermediatePage* target_casted = as_intermdiate(target);
    target_casted->set_key_count(index);
    target_casted->get_minipage(index).key_count_ = index_mini;
    KeySlice this_fence;
    KeySlice next_fence;
    target_casted->extract_separators_snapshot(index, index_mini, &this_fence, &next_fence);
    level->next_original_ = index;
    level->next_original_mini_ = index_mini + 1U;
    level->next_original_slice_ = next_fence;
    if (level->next_original_mini_ > minipage.key_count_) {
      ++level->next_original_;
      level->next_original_mini_ = 0;
      if (level->next_original_ > casted->get_key_count()) {
        level->set_no_more_next_original();
      }
    }

    // as this is an intermediate page, we have to further go down.
    SnapshotPagePointer* next_address
      = &target_casted->get_minipage(index).pointers_[index_mini].snapshot_pointer_;
    CHECK_ERROR(open_next_level(key, key_length, target_casted, slice, next_address));
    ASSERT_ND(cur_path_levels_ > this_level + 1U);
  }
  return kRetOk;
}

void MasstreeComposeContext::open_next_level_create_layer(
  const char* key,
  uint16_t key_length,
  MasstreeBorderPage* parent,
  KeySlice prefix_slice,
  uint8_t parent_index) {
  ASSERT_ND(!parent->does_point_to_layer(parent_index));  // not yet a next-layer pointer
  ASSERT_ND(parent->get_slice(parent_index) == prefix_slice);
  ASSERT_ND(key_length > 0);
  ASSERT_ND(is_key_aligned_and_zero_padded(key, key_length));
  // DLOG(INFO) << "asdasdas " << *parent;
  uint8_t this_level = cur_path_levels_;
  PathLevel* level = cur_path_ + this_level;
  ++cur_path_levels_;

  store_cur_prefix(parent->get_layer(), prefix_slice);
  level->layer_ = parent->get_layer() + 1;
  level->head_ = allocate_page();
  level->page_count_ = 1;
  level->tail_ = level->head_;
  level->set_no_more_next_original();
  level->low_fence_ = kInfimumSlice;
  level->high_fence_ = kSupremumSlice;

  // DLOG(INFO) << "Fdsdsfsdas " << *parent;
  ASSERT_ND(key_length > level->layer_ * kSliceLen);

  SnapshotPagePointer page_id = page_id_base_ + level->head_;
  MasstreeBorderPage* target = reinterpret_cast<MasstreeBorderPage*>(get_page(level->head_));
  target->initialize_snapshot_page(id_, page_id, level->layer_, kInfimumSlice, kSupremumSlice);

  // DLOG(INFO) << "eqeqwe2 " << *parent;

  // migrate the exiting record from parent.
  uint16_t remaining_length = parent->get_remaining_key_length(parent_index) - kSliceLen;
  const char* parent_suffix = parent->get_record(parent_index);
  ASSERT_ND(is_key_aligned_and_zero_padded(parent_suffix, remaining_length));
  KeySlice slice = normalize_be_bytes_full_aligned(parent_suffix);
  const char* suffix = parent_suffix + kSliceLen;
  const char* payload = parent->get_record_payload(parent_index);
  uint16_t payload_count = parent->get_payload_length(parent_index);
  xct::XctId xct_id = parent->get_owner_id(parent_index)->xct_id_;

  // DLOG(INFO) << "eqeqwe3 " << *parent;

  // see the comment in PathLevel. If the existing key in parent is larger than searching key,
  // we move the existing record to a dummy original page here.
  MasstreeBorderPage* original = reinterpret_cast<MasstreeBorderPage*>(get_original(this_level));
  original->initialize_snapshot_page(id_, 0, level->layer_, kInfimumSlice, kSupremumSlice);
  ASSERT_ND(original->can_accomodate(0, remaining_length, payload_count));
  original->reserve_record_space(0, xct_id, slice, suffix, remaining_length, payload_count);
  original->increment_key_count();
  fill_payload_padded(original->get_record_payload(0), payload, payload_count);

  ASSERT_ND(original->ltgt_key(0, key, key_length) != 0);  // if exactly same key, why here
  if (original->ltgt_key(0, key, key_length) < 0) {  // the original record is larger than key
    VLOG(1) << "Interesting, moving an original record to a dummy page in next layer";
    // as the record is still not supposed to be added, we keep it in the original page
    level->next_original_ = 0;
    level->next_original_slice_ = slice;
    level->next_original_remaining_ = remaining_length;
  } else {
    // okay, insert it now.
    target->reserve_record_space(0, xct_id, slice, suffix, remaining_length, payload_count);
    target->increment_key_count();
    fill_payload_padded(target->get_record_payload(0), payload, payload_count);
    ASSERT_ND(target->ltgt_key(0, key, key_length) > 0);
  }
  // DLOG(INFO) << "easdasdf " << *original;
  // DLOG(INFO) << "dsdfsdf " << *target;

  // and modify the record to be a next layer pointer
  parent->replace_next_layer_snapshot(page_id);
  // DLOG(INFO) << "Fdsddss " << *parent;
}

inline bool MasstreeComposeContext::does_need_to_adjust_path(
  const char* key,
  uint16_t key_length) const {
  if (cur_path_levels_ == 0) {
    return true;  // the path is empty. need to initialize path
  } else if (!cur_path_[0].contains_key(key, key_length)) {
    // first slice does not match
    return true;
  } else if (get_last_level()->layer_ == 0) {
    return false;  // first slice check already done.
  }

  // Does the slices of the next_key match the current path? if not we have to close them
  uint8_t cur_path_layer = get_last_layer();
  if (cur_path_layer > 0) {
    uint16_t max_key_layer = (key_length - 1U) / kSliceLen;
    if (max_key_layer < cur_path_layer) {
      return true;  // definitely not in this layer
    }
    uint16_t common_layers = count_common_slices(cur_prefix_be_, key, cur_path_layer);
    if (common_layers < cur_path_layer) {
      return true;  // prefix doesn't match. we have to close the layer(s)
    }
  }

  // Does the in-layer slice of the key match the current page? if not we have to close them
  // key is 8-bytes padded, so we can simply use normalize_be_bytes_full_aligned
  uint16_t skip = cur_path_layer * kSliceLen;
  KeySlice next_slice = normalize_be_bytes_full_aligned(key + skip);
  return !get_last_level()->contains_slice(next_slice);
}

ErrorStack MasstreeComposeContext::adjust_path(const char* key, uint16_t key_length) {
  // Close levels/layers until prefix slice matches
  bool closed_something = false;
  if (cur_path_levels_ > 0) {
    PathLevel* last = get_last_level();
    if (last->layer_ > 0) {
      // Close layers until prefix slice matches
      uint16_t max_key_layer = (key_length - 1U) / kSliceLen;
      uint16_t cmp_layer = std::min<uint16_t>(last->layer_, max_key_layer);
      uint16_t common_layers = count_common_slices(cur_prefix_be_, key, cmp_layer);
      if (common_layers < last->layer_) {
        CHECK_ERROR(close_path_layer(common_layers));
        ASSERT_ND(common_layers == get_last_layer());
        closed_something = true;
      }
    }

    last = get_last_level();
    // Close levels upto fence matches
    uint16_t skip = last->layer_ * kSliceLen;
    KeySlice next_slice = normalize_be_bytes_full_aligned(key + skip);
    while (cur_path_levels_ > 0 && !last->contains_slice(next_slice)) {
      // the next key doesn't fit in the current path. need to close.
      if (cur_path_levels_ == 1U) {
        CHECK_ERROR(close_first_level());
        ASSERT_ND(cur_path_levels_ == 0);
      } else {
        CHECK_ERROR(close_last_level());
        closed_something = true;
        last = get_last_level();
        ASSERT_ND(get_last_level()->layer_ * kSliceLen == skip);  // this shouldn't affect layer.
      }
    }
  }

  if (cur_path_levels_ == 0) {
    CHECK_ERROR(open_first_level(key, key_length));
  }
  ASSERT_ND(cur_path_levels_ > 0);

  // now, if the last level is an intermediate page, we definitely have to go deeper.
  // if it's a border page, we may have to go down, but at this point we are not sure.
  // so, just handle intermediate page case. after doing this, last level should be a border page.
  PathLevel* last = get_last_level();
  uint16_t skip = last->layer_ * kSliceLen;
  KeySlice next_slice = normalize_be_bytes_full_aligned(key + skip);
  ASSERT_ND(last->contains_slice(next_slice));
  MasstreePage* page = get_page(last->tail_);
  if (!page->is_border()) {
    ASSERT_ND(closed_something);  // otherwise current path should have been already border page
    MasstreeIntermediatePage* casted = as_intermdiate(page);

    // as searching keys are sorted, the slice should be always larger than everything in the page
    ASSERT_ND(casted->find_minipage(next_slice) == casted->get_key_count());
    ASSERT_ND(casted->get_minipage(casted->get_key_count()).find_pointer(next_slice)
      == casted->get_minipage(casted->get_key_count()).key_count_);
    if (last->has_next_original() && last->next_original_slice_ <= next_slice) {
      ASSERT_ND(last->next_original_slice_ != next_slice);
      CHECK_ERROR(consume_original_upto_intermediate(next_slice, last));
    }
    ASSERT_ND(casted->find_minipage(next_slice) == casted->get_key_count());
    ASSERT_ND(casted->get_minipage(casted->get_key_count()).find_pointer(next_slice)
      == casted->get_minipage(casted->get_key_count()).key_count_);
    uint8_t index = casted->get_key_count();
    MasstreeIntermediatePage::MiniPage& minipage = casted->get_minipage(index);
    uint8_t index_mini = minipage.key_count_;
#ifndef NDEBUG
    KeySlice existing_low;
    KeySlice existing_high;
    casted->extract_separators_snapshot(index, index_mini, &existing_low, &existing_high);
    ASSERT_ND(existing_low <= next_slice);
    ASSERT_ND(existing_high == casted->get_high_fence());
#endif  // NDEBUG
    SnapshotPagePointer* next_pointer = &minipage.pointers_[index_mini].snapshot_pointer_;
    CHECK_ERROR(open_next_level(key, key_length, page, 0, next_pointer));
  }
  ASSERT_ND(get_last_level()->contains_slice(next_slice));
  ASSERT_ND(get_page(get_last_level()->tail_)->is_border());
  return kRetOk;
}

inline void MasstreeComposeContext::append_border(
  KeySlice slice,
  xct::XctId xct_id,
  KeyLength remaining_length,
  const char* suffix,
  PayloadLength payload_count,
  const char* payload,
  PathLevel* level) {
  ASSERT_ND(remaining_length != kNextLayerKeyLength);
  MasstreeBorderPage* target = as_border(get_page(level->tail_));
  SlotIndex key_count = target->get_key_count();
  SlotIndex new_index = key_count;
  ASSERT_ND(key_count == 0 || !target->will_conflict(key_count - 1, slice, remaining_length));
  ASSERT_ND(key_count == 0
    || !target->will_contain_next_layer(key_count - 1, slice, remaining_length));
  ASSERT_ND(key_count == 0 || target->ltgt_key(key_count - 1, slice, suffix, remaining_length) > 0);
  if (UNLIKELY(!target->can_accomodate(new_index, remaining_length, payload_count))) {
    append_border_newpage(slice, level);
    MasstreeBorderPage* new_target = as_border(get_page(level->tail_));
    ASSERT_ND(target != new_target);
    target = new_target;
    new_index = 0;
  }

  target->reserve_record_space(new_index, xct_id, slice, suffix, remaining_length, payload_count);
  target->increment_key_count();
  fill_payload_padded(target->get_record_payload(new_index), payload, payload_count);
}

inline void MasstreeComposeContext::append_border_next_layer(
  KeySlice slice,
  xct::XctId xct_id,
  SnapshotPagePointer pointer,
  PathLevel* level) {
  MasstreeBorderPage* target = as_border(get_page(level->tail_));
  uint16_t key_count = target->get_key_count();
  uint16_t new_index = key_count;
  ASSERT_ND(key_count == 0 || !target->will_conflict(key_count - 1, slice, 0xFF));
  ASSERT_ND(key_count == 0 || target->ltgt_key(key_count - 1, slice, nullptr, kSliceLen) > 0);
  if (UNLIKELY(!target->can_accomodate(new_index, 0, sizeof(DualPagePointer)))) {
    append_border_newpage(slice, level);
    MasstreeBorderPage* new_target = as_border(get_page(level->tail_));
    ASSERT_ND(target != new_target);
    target = new_target;
    new_index = 0;
  }

  target->append_next_layer_snapshot(xct_id, slice, pointer);
}

void MasstreeComposeContext::append_border_newpage(KeySlice slice, PathLevel* level) {
  MasstreeBorderPage* target = as_border(get_page(level->tail_));
  uint16_t key_count = target->get_key_count();

  memory::PagePoolOffset new_offset = allocate_page();
  SnapshotPagePointer new_page_id = page_id_base_ + new_offset;
  MasstreeBorderPage* new_target = reinterpret_cast<MasstreeBorderPage*>(get_page(new_offset));

  KeySlice middle = slice;
  KeySlice high_fence = target->get_high_fence();
  new_target->initialize_snapshot_page(id_, new_page_id, level->layer_, middle, high_fence);
  target->set_foster_major_offset_unsafe(new_offset);  // set next link
  target->set_high_fence_unsafe(middle);
  level->tail_ = new_offset;
  ++level->page_count_;

  // We have to make sure the same slice does not span two pages with only length different
  // (masstree protocol). So, we might have to migrate a few more records in rare cases.
  uint16_t migrate_from;
  for (migrate_from = key_count; target->get_slice(migrate_from - 1U) == slice; --migrate_from) {
    continue;
  }
  if (migrate_from != key_count) {
    LOG(INFO) << "Interesting, migrate records to avoid key slice spanning two pages";
    for (uint16_t old_index = migrate_from; old_index < key_count; ++old_index) {
      ASSERT_ND(target->get_slice(old_index) == slice);
      xct::XctId xct_id = target->get_owner_id(old_index)->xct_id_;
      uint16_t new_index = new_target->get_key_count();
      if (target->does_point_to_layer(old_index)) {
        const DualPagePointer* pointer = target->get_next_layer(old_index);
        ASSERT_ND(pointer->volatile_pointer_.is_null());
        new_target->append_next_layer_snapshot(xct_id, slice, pointer->snapshot_pointer_);
      } else {
        uint16_t payload_count = target->get_payload_length(old_index);
        uint16_t remaining = target->get_remaining_key_length(old_index);
        new_target->reserve_record_space(
          new_index,
          xct_id,
          slice,
          target->get_record(old_index),
          remaining,
          payload_count);
        new_target->increment_key_count();
        fill_payload_padded(
            new_target->get_record_payload(new_index),
            target->get_record_payload(old_index),
            payload_count);
      }
    }

    target->header().set_key_count(migrate_from);
    ASSERT_ND(new_target->get_key_count() == key_count - migrate_from);
  }
}


inline void MasstreeComposeContext::append_intermediate(
  KeySlice low_fence,
  SnapshotPagePointer pointer,
  PathLevel* level) {
  ASSERT_ND(low_fence != kInfimumSlice);
  ASSERT_ND(low_fence != kSupremumSlice);
  MasstreeIntermediatePage* target = as_intermdiate(get_page(level->tail_));
  if (UNLIKELY(target->is_full_snapshot())) {
    append_intermediate_newpage_and_pointer(low_fence, pointer, level);
  } else {
    target->append_pointer_snapshot(low_fence, pointer);
  }
}

void MasstreeComposeContext::append_intermediate_newpage_and_pointer(
  KeySlice low_fence,
  SnapshotPagePointer pointer,
  PathLevel* level) {
  MasstreeIntermediatePage* target = as_intermdiate(get_page(level->tail_));

  memory::PagePoolOffset new_offset = allocate_page();
  SnapshotPagePointer new_page_id = page_id_base_ + new_offset;
  MasstreeIntermediatePage* new_target
    = reinterpret_cast<MasstreeIntermediatePage*>(get_page(new_offset));

  KeySlice middle = low_fence;
  KeySlice high_fence = target->get_high_fence();
  uint8_t btree_level = target->get_btree_level();
  uint8_t layer = level->layer_;
  new_target->initialize_snapshot_page(id_, new_page_id, layer, btree_level, middle, high_fence);
  target->set_foster_major_offset_unsafe(new_offset);  // set next link
  target->set_high_fence_unsafe(middle);
  level->tail_ = new_offset;
  ++level->page_count_;

  // Unlike border page, slices are unique in intermediate page, so nothing to migrate.
  // Instead, we also complete setting the installed pointer because the first pointer
  // is a bit special in intermediate pages.
  new_target->get_minipage(0).pointers_[0].volatile_pointer_.clear();
  new_target->get_minipage(0).pointers_[0].snapshot_pointer_ = pointer;
}

ErrorStack MasstreeComposeContext::consume_original_upto_border(
  KeySlice slice,
  uint16_t key_length,
  PathLevel* level) {
  ASSERT_ND(level == get_last_level());  // so far this is the only usecase
  ASSERT_ND(key_length > level->layer_ * kSliceLen);
  ASSERT_ND(level->needs_to_consume_original(slice, key_length));
  uint16_t level_index = level - cur_path_;
  MasstreeBorderPage* original = as_border(get_original(level_index));
  while (level->needs_to_consume_original(slice, key_length)) {
    uint16_t index = level->next_original_;
    ASSERT_ND(level->next_original_slice_ == original->get_slice(index));
    ASSERT_ND(level->next_original_remaining_ == original->get_remaining_key_length(index));
    ASSERT_ND(level->has_next_original());
    ASSERT_ND(level->next_original_slice_ <= slice);
    KeySlice original_slice = original->get_slice(index);
    uint16_t original_remaining = original->get_remaining_key_length(index);
    xct::XctId xct_id = original->get_owner_id(index)->xct_id_;
    if (original->does_point_to_layer(index)) {
      const DualPagePointer* pointer = original->get_next_layer(index);
      ASSERT_ND(pointer->volatile_pointer_.is_null());
      append_border_next_layer(original_slice, xct_id, pointer->snapshot_pointer_, level);
    } else {
      append_border(
        original_slice,
        xct_id,
        original_remaining,
        original->get_record(index),
        original->get_payload_length(index),
        original->get_record_payload(index),
        level);
    }
    ++level->next_original_;
    if (level->next_original_ >= original->get_key_count()) {
      level->set_no_more_next_original();
    } else {
      level->next_original_slice_ = original->get_slice(level->next_original_);
      level->next_original_remaining_ = original->get_remaining_key_length(level->next_original_);
    }
  }
  return kRetOk;
}

ErrorStack MasstreeComposeContext::consume_original_upto_intermediate(
  KeySlice slice,
  PathLevel* level) {
  ASSERT_ND(level->has_next_original());
  ASSERT_ND(level->next_original_slice_ < slice);
  uint16_t level_index = level - cur_path_;
  MasstreeIntermediatePage* original = as_intermdiate(get_original(level_index));
  while (level->has_next_original() && level->next_original_slice_ < slice) {
    MasstreeIntermediatePointerIterator it(original);
    it.index_ = level->next_original_;
    it.index_mini_ = level->next_original_mini_;
    ASSERT_ND(it.is_valid());
    append_intermediate(it.get_low_key(), it.get_pointer().snapshot_pointer_, level);
    it.next();
    if (it.is_valid()) {
      level->next_original_ = it.index_;
      level->next_original_mini_ = it.index_mini_;
      level->next_original_slice_ = it.get_low_key();
    } else {
      level->set_no_more_next_original();
    }
  }
  return kRetOk;
}

ErrorStack MasstreeComposeContext::consume_original_all() {
  PathLevel* last = get_last_level();
  if (!last->has_next_original()) {
    return kRetOk;  // nothing to consume
  }

  MasstreePage* page = get_original(get_last_level_index());
  if (page->is_border()) {
    MasstreeBorderPage* original = as_border(page);
    while (last->has_next_original()) {
      uint16_t index = last->next_original_;
      KeySlice slice = original->get_slice(index);
      xct::XctId xct_id = original->get_owner_id(index)->xct_id_;
      if (original->does_point_to_layer(index)) {
        const DualPagePointer* pointer = original->get_next_layer(index);
        ASSERT_ND(pointer->volatile_pointer_.is_null());
        append_border_next_layer(slice, xct_id, pointer->snapshot_pointer_, last);
      } else {
        append_border(
          slice,
          xct_id,
          original->get_remaining_key_length(index),
          original->get_record(index),
          original->get_payload_length(index),
          original->get_record_payload(index),
          last);
      }
      ++last->next_original_;
      if (last->next_original_ >= original->get_key_count()) {
        break;
      }
    }
  } else {
    MasstreeIntermediatePage* original = as_intermdiate(page);
    MasstreeIntermediatePointerIterator it(original);
    it.index_ = last->next_original_;
    it.index_mini_ = last->next_original_mini_;
    for (; it.is_valid(); it.next()) {
      append_intermediate(it.get_low_key(), it.get_pointer().snapshot_pointer_, last);
    }
  }
  last->set_no_more_next_original();
  return kRetOk;
}

ErrorStack MasstreeComposeContext::close_level_grow_subtree(
  SnapshotPagePointer* root_pointer,
  KeySlice subtree_low,
  KeySlice subtree_high) {
  PathLevel* last = get_last_level();
  ASSERT_ND(last->low_fence_ == subtree_low);
  ASSERT_ND(last->high_fence_ == subtree_high);
  ASSERT_ND(last->page_count_ > 1U);
  ASSERT_ND(!last->has_next_original());

  std::vector<FenceAndPointer> children;
  children.reserve(last->page_count_);
  uint8_t cur_btree_level = 0;
  for (memory::PagePoolOffset cur = last->head_; cur != 0;) {
    MasstreePage* page = get_page(cur);
    ASSERT_ND(page->get_foster_minor().is_null());
    FenceAndPointer child;
    child.pointer_ = page_id_base_ + cur;
    child.low_fence_ = page->get_low_fence();
    children.emplace_back(child);
    cur = page->get_foster_major().components.offset;
    page->set_foster_major_offset_unsafe(0);  // no longer needed
    cur_btree_level = page->get_btree_level();
  }
  ASSERT_ND(children.size() == last->page_count_);

  // The level is now nullified
  // Now, replace the last level with a newly created level.
  memory::PagePoolOffset new_offset = allocate_page();
  SnapshotPagePointer new_page_id = page_id_base_ + new_offset;
  MasstreeIntermediatePage* new_page
    = reinterpret_cast<MasstreeIntermediatePage*>(get_page(new_offset));
  new_page->initialize_snapshot_page(
    id_,
    new_page_id,
    last->layer_,
    cur_btree_level + 1U,  // add a new B-tree level
    last->low_fence_,
    last->high_fence_);
  last->head_ = new_offset;
  last->tail_ = new_offset;
  last->page_count_ = 1;
  ASSERT_ND(children.size() > 0);
  ASSERT_ND(children[0].low_fence_ == subtree_low);
  new_page->get_minipage(0).pointers_[0].volatile_pointer_.clear();
  new_page->get_minipage(0).pointers_[0].snapshot_pointer_ = children[0].pointer_;
  for (uint32_t i = 1; i < children.size(); ++i) {
    const FenceAndPointer& child = children[i];
    append_intermediate(child.low_fence_, child.pointer_, last);
  }


  // also register the pages in the newly created level.
  WRAP_ERROR_CODE(close_level_register_page_boundaries());

  if (last->page_count_ > 1U) {
    // recurse until we get just one page. this also means, when there are skewed inserts,
    // we might have un-balanced masstree (some subtree is deeper). not a big issue.
    CHECK_ERROR(close_level_grow_subtree(root_pointer, subtree_low, subtree_high));
  } else {
    *root_pointer = new_page_id;
  }
  return kRetOk;
}

ErrorStack MasstreeComposeContext::pushup_non_root() {
  ASSERT_ND(cur_path_levels_ > 1U);  // there is a parent level
  PathLevel* last = get_last_level();
  PathLevel* parent = get_second_last_level();
  ASSERT_ND(last->low_fence_ != kInfimumSlice || last->high_fence_ != kSupremumSlice);
  ASSERT_ND(last->page_count_ > 1U);
  ASSERT_ND(!last->has_next_original());

  bool is_head = true;
  for (memory::PagePoolOffset cur = last->head_; cur != 0;) {
    MasstreePage* page = get_page(cur);
    ASSERT_ND(page->get_foster_minor().is_null());
    SnapshotPagePointer pointer = page_id_base_ + cur;
    KeySlice low_fence = page->get_low_fence();
    ASSERT_ND(cur != last->head_ || low_fence == last->low_fence_);  // first entry's low_fence
    ASSERT_ND(cur != last->tail_ || page->get_high_fence() == last->high_fence_);  // tail's high
    ASSERT_ND(cur != last->tail_ || page->get_foster_major().is_null());  // iff tail, has no next
    ASSERT_ND(cur == last->tail_ || !page->get_foster_major().is_null());
    cur = page->get_foster_major().components.offset;
    page->set_foster_major_offset_unsafe(0);  // no longer needed

    // before pushing up the pointer, we might have to consume original pointers
    if (parent->has_next_original() && parent->next_original_slice_ <= low_fence) {
      ASSERT_ND(parent->next_original_slice_ != low_fence);
      CHECK_ERROR(consume_original_upto_intermediate(low_fence, parent));
    }

    // if this is a re-opened existing page (which is always the head), we already have
    // a pointer to this page in the parent page, so appending it will cause a duplicate.
    // let's check if that's the case.
    bool already_appended = false;
    if (is_head) {
      const MasstreeIntermediatePage* parent_page = as_intermdiate(get_page(parent->tail_));
      uint8_t index = parent_page->get_key_count();
      const MasstreeIntermediatePage::MiniPage& mini = parent_page->get_minipage(index);
      uint16_t index_mini = mini.key_count_;
      if (mini.pointers_[index_mini].snapshot_pointer_ == pointer) {
        already_appended = true;
#ifndef NDEBUG
        // If that's the case, the low-fence should match. let's check.
        KeySlice check_low, check_high;
        parent_page->extract_separators_snapshot(index, index_mini, &check_low, &check_high);
        ASSERT_ND(check_low == low_fence);
        // high-fence should be same as tail's high if this level has split. let's check it, too.
        MasstreePage* tail = get_page(last->tail_);
        ASSERT_ND(check_high == tail->get_high_fence());
#endif  // NDEBUG
      }
    }
    if (!already_appended) {
      append_intermediate(low_fence, pointer, parent);
    }
    is_head = false;
  }

  return kRetOk;
}

ErrorStack MasstreeComposeContext::close_path_layer(uint16_t max_layer) {
  DVLOG(1) << "Closing path up to layer-" << max_layer;
  ASSERT_ND(get_last_layer() > max_layer);
  while (true) {
    PathLevel* last = get_last_level();
    if (last->layer_ <= max_layer) {
      break;
    }
    CHECK_ERROR(close_last_level());
  }
  ASSERT_ND(get_last_layer() == max_layer);
  return kRetOk;
}

ErrorStack MasstreeComposeContext::close_last_level() {
  ASSERT_ND(cur_path_levels_ > 1U);  // do not use this method to close the first one.
  DVLOG(1) << "Closing the last level " << *get_last_level();
  PathLevel* last = get_last_level();
  PathLevel* parent = get_second_last_level();

  // before closing, consume all records in original page.
  CHECK_ERROR(consume_original_all());
  WRAP_ERROR_CODE(close_level_register_page_boundaries());

#ifndef NDEBUG
  {
    // some sanity checks.
    KeySlice prev = last->low_fence_;
    uint32_t counted = 0;
    for (memory::PagePoolOffset cur = last->head_; cur != 0;) {
      const MasstreePage* page = get_page(cur);
      ++counted;
      ASSERT_ND(page->get_low_fence() == prev);
      ASSERT_ND(page->get_high_fence() > prev);
      ASSERT_ND(page->get_layer() == last->layer_);
      prev = page->get_high_fence();
      cur = page->get_foster_major().components.offset;
      if (page->is_border()) {
        ASSERT_ND(page->get_key_count() > 0);
      }
    }
    ASSERT_ND(prev == last->high_fence_);
    ASSERT_ND(counted == last->page_count_);
  }
#endif  // NDEBUG

  // Closing this level means we might have to push up the last level's chain to previous.
  if (last->page_count_ > 1U) {
    ASSERT_ND(parent->layer_ <= last->layer_);
    bool last_is_root = (parent->layer_ != last->layer_);
    if (!last_is_root) {
      // 1) last is not layer-root, then we append child pointers to parent. simpler
      CHECK_ERROR(pushup_non_root());
    } else {
      // 2) last is layer-root, then we add another level (intermediate page) as a new root
      MasstreeBorderPage* parent_tail = as_border(get_page(parent->tail_));
      ASSERT_ND(parent_tail->get_key_count() > 0);
      ASSERT_ND(parent_tail->does_point_to_layer(parent_tail->get_key_count() - 1));
      SnapshotPagePointer* parent_pointer
        = &parent_tail->get_next_layer(parent_tail->get_key_count() - 1)->snapshot_pointer_;
      CHECK_ERROR(close_level_grow_subtree(parent_pointer, kInfimumSlice, kSupremumSlice));
    }
  }

  ASSERT_ND(!last->has_next_original());
  --cur_path_levels_;
  ASSERT_ND(cur_path_levels_ >= 1U);
  return kRetOk;
}
ErrorStack MasstreeComposeContext::close_first_level() {
  ASSERT_ND(cur_path_levels_ == 1U);
  ASSERT_ND(get_last_layer() == 0U);
  PathLevel* first = cur_path_;

  // Do we have to make another level?
  CHECK_ERROR(consume_original_all());
  WRAP_ERROR_CODE(close_level_register_page_boundaries());

  SnapshotPagePointer current_pointer = first->head_ + page_id_base_;
  DualPagePointer& root_pointer = root_->get_minipage(root_index_).pointers_[root_index_mini_];
  ASSERT_ND(root_pointer.volatile_pointer_.is_null());
  ASSERT_ND(root_pointer.snapshot_pointer_ == current_pointer);
  KeySlice root_low, root_high;
  root_->extract_separators_snapshot(root_index_, root_index_mini_, &root_low, &root_high);
  if (first->page_count_ > 1U) {
    CHECK_ERROR(close_level_grow_subtree(&root_pointer.snapshot_pointer_, root_low, root_high));
  }

  // adjust root page's btree-level if needed. as an exceptional rule, btree-level of 1st layer root
  // is max(child's level)+1.
  ASSERT_ND(first->page_count_ == 1U);
  MasstreePage* child = get_page(first->head_);
  if (child->get_btree_level() + 1U > root_->get_btree_level()) {
    VLOG(0) << "Masstree-" << id_ << " subtree of first layer root has grown.";
    root_->header().masstree_in_layer_level_ = child->get_btree_level() + 1U;
  }

  cur_path_levels_ = 0;
  root_index_ = 0xDA;  // just some
  root_index_mini_= 0xDA;  // random number
  return kRetOk;
}

ErrorStack MasstreeComposeContext::close_all_levels() {
  LOG(INFO) << "Closing all levels except root_";
  while (cur_path_levels_ > 1U) {
    CHECK_ERROR(close_last_level());
  }
  ASSERT_ND(cur_path_levels_ <= 1U);
  if (cur_path_levels_ > 0) {
    CHECK_ERROR(close_first_level());
  }
  ASSERT_ND(cur_path_levels_ == 0);
  LOG(INFO) << "Closed all levels. now buffered pages=" << allocated_pages_;
  return kRetOk;
}

ErrorStack MasstreeComposeContext::flush_buffer() {
  LOG(INFO) << "Flushing buffer. buffered pages=" << allocated_pages_;
  // 1) Close all levels. Otherwise we might still have many 'active' pages.
  close_all_levels();

  ASSERT_ND(allocated_pages_ <= max_pages_);

  WRAP_ERROR_CODE(get_writer()->dump_pages(0, allocated_pages_));
  allocated_pages_ = 0;
  page_id_base_ = get_writer()->get_next_page_id();
  write_dummy_page_zero();
  return kRetOk;
}

void MasstreeComposeContext::store_cur_prefix(uint8_t layer, KeySlice prefix_slice) {
  assorted::write_bigendian<KeySlice>(prefix_slice, cur_prefix_be_ + layer * kSliceLen);
  cur_prefix_slices_[layer] = prefix_slice;
}

///////////////////////////////////////////////////////////////////////
///
///  MasstreeComposeContext page_boundary_info/install_pointers related
///
///////////////////////////////////////////////////////////////////////
void MasstreeComposeContext::remove_old_page_boundary_info(
  SnapshotPagePointer page_id,
  MasstreePage* page) {
  snapshot::SnapshotId snapshot_id = extract_snapshot_id_from_snapshot_pointer(page_id);
  ASSERT_ND(snapshot_id == snapshot_id_);
  ASSERT_ND(extract_numa_node_from_snapshot_pointer(page_id) == numa_node_);
  ASSERT_ND(page->header().page_id_ == page_id);
  ASSERT_ND(page_boundary_info_cur_pos_ > 0);
  ASSERT_ND(page_boundary_elements_ > 0);
  ASSERT_ND(cur_path_levels_ > 0);

  // this should happen very rarely (after flush_buffer), so the performance doesn't matter.
  // hence we do somewhat sloppy thing here for simpler code.
  LOG(INFO) << "Removing a page_boundary_info entry for a re-opened page. This should happen"
    << " very infrequently, or the buffer size is too small.";
  const uint8_t btree_level = page->get_btree_level();
  const uint8_t layer = page->get_layer();
  const KeySlice* prefixes = cur_prefix_slices_;
  const KeySlice low = page->get_low_fence();
  const KeySlice high = page->get_high_fence();
  const uint32_t hash = PageBoundaryInfo::calculate_hash(btree_level, layer, prefixes, low, high);

  // check them all! again, this method is called very occasionally
  bool found = false;
  for (uint32_t i = 0; LIKELY(i < page_boundary_elements_); ++i) {
    if (LIKELY(page_boundary_sort_[i].hash_ != hash)) {
      continue;
    }
    ASSERT_ND(page_boundary_sort_[i].info_pos_ < page_boundary_info_cur_pos_);
    PageBoundaryInfo* info = get_page_boundary_info(page_boundary_sort_[i].info_pos_);
    if (info->exact_match(btree_level, layer, prefixes, low, high)) {
      ASSERT_ND(!info->removed_);
      // mark the entry as removed. later exact_match() will ignore this entry, although
      // hash value is still there. but it only causes a bit more false positives.
      info->removed_ = true;
      found = true;
      break;
    }
  }

  if (!found) {
    LOG(ERROR) << "WTF. The old page_boundary_info entry was not found. page_id="
      << assorted::Hex(page_id);
  }
}

inline void MasstreeComposeContext::assert_page_boundary_not_exists(
  uint8_t btree_level,
  uint8_t layer,
  const KeySlice* prefixes,
  KeySlice low,
  KeySlice high) const {
#ifndef NDEBUG
  uint32_t hash = PageBoundaryInfo::calculate_hash(btree_level, layer, prefixes, low, high);
  for (uint32_t i = 0; LIKELY(i < page_boundary_elements_); ++i) {
    if (LIKELY(page_boundary_sort_[i].hash_ != hash)) {
      continue;
    }
    ASSERT_ND(page_boundary_sort_[i].info_pos_ < page_boundary_info_cur_pos_);
    const PageBoundaryInfo* info = get_page_boundary_info(page_boundary_sort_[i].info_pos_);
    bool exists = info->exact_match(btree_level, layer, prefixes, low, high);
    if (exists) {  // just for debugging.
      // I think I figured this out. See comment of PageBoundaryInfo::btree_level_.
      info->exact_match(btree_level, layer, prefixes, low, high);
    }
    ASSERT_ND(!exists);
  }
#else  // NDEBUG
  UNUSED_ND(btree_level);
  UNUSED_ND(layer);
  UNUSED_ND(prefixes);
  UNUSED_ND(low);
  UNUSED_ND(high);
#endif  // NDEBUG
}

ErrorCode MasstreeComposeContext::close_level_register_page_boundaries() {
  // this method must be called *after* the pages in the last level are finalized because
  // we need to know the page fences. this is thus called at the end of close_xxx_level
  ASSERT_ND(cur_path_levels_ > 0U);
  PathLevel* last = get_last_level();
  ASSERT_ND(!last->has_next_original());  // otherwise tail's page boundary is not finalized yet

#ifndef NDEBUG
  // let's check that the last level is finalized.
  KeySlice prev = last->low_fence_;
  uint32_t counted = 0;
  for (memory::PagePoolOffset cur = last->head_; cur != 0;) {
    const MasstreePage* page = get_page(cur);
    ++counted;
    ASSERT_ND(page->get_low_fence() == prev);
    ASSERT_ND(page->get_high_fence() > prev);
    ASSERT_ND(page->get_layer() == last->layer_);
    prev = page->get_high_fence();
    cur = page->get_foster_major().components.offset;
  }
  ASSERT_ND(prev == last->high_fence_);
  ASSERT_ND(counted == last->page_count_);
#endif  // NDEBUG


  KeySlice prev_high = 0;  // only for assertion
  for (memory::PagePoolOffset cur = last->head_; cur != 0;) {
    const MasstreePage* page = get_page(cur);
    const SnapshotPagePointer page_id = page->header().page_id_;
    const uint8_t btree_level = page->get_btree_level();
    const uint8_t layer = last->layer_;
    const KeySlice low = page->get_low_fence();
    const KeySlice high = page->get_high_fence();
    ASSERT_ND(low < high);
    ASSERT_ND(cur == last->head_ || low == prev_high);
    ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(page_id) == snapshot_id_);

    // we might have to expand memory for page_boundary_info.
    uint16_t info_size = (layer + 3U) * sizeof(KeySlice);  // see dynamic_sizeof()
    if (UNLIKELY(page_boundary_elements_ == max_page_boundary_elements_
      || page_boundary_info_cur_pos_ + (info_size / 8U) >= page_boundary_info_capacity_)) {
      LOG(INFO) << "Automatically expanding memory for page_boundary_info. This is super-rare";
      CHECK_ERROR_CODE(get_writer()->expand_intermediate_memory(
        get_writer()->get_intermediate_size() * 2U,
        true));  // we need to retain the current content
      refresh_page_boundary_info_variables();
    }
    ASSERT_ND(page_boundary_elements_ < max_page_boundary_elements_);
    ASSERT_ND(page_boundary_info_cur_pos_ + (info_size / 8U) < page_boundary_info_capacity_);
    assert_page_boundary_not_exists(btree_level, layer, cur_prefix_slices_, low, high);

    PageBoundaryInfo* info = get_page_boundary_info(page_boundary_info_cur_pos_);
    info->btree_level_ = btree_level;
    info->removed_ = false;
    info->layer_ = layer;
    ASSERT_ND(info->dynamic_sizeof() == info_size);
    SnapshotLocalPageId local_id = extract_local_page_id_from_snapshot_pointer(page_id);
    ASSERT_ND(local_id > 0);
    ASSERT_ND(local_id < (1ULL << 40));
    info->local_snapshot_pointer_high_ = local_id >> 32;
    info->local_snapshot_pointer_low_ = static_cast<uint32_t>(local_id);
    for (uint8_t i = 0; i < layer; ++i) {
      info->slices_[i] = cur_prefix_slices_[i];
    }
    info->slices_[layer] = low;
    info->slices_[layer + 1] = high;

    page_boundary_sort_[page_boundary_elements_].hash_
      = PageBoundaryInfo::calculate_hash(btree_level, layer, cur_prefix_slices_, low, high);
    page_boundary_sort_[page_boundary_elements_].info_pos_ = page_boundary_info_cur_pos_;
    ++page_boundary_elements_;
    page_boundary_info_cur_pos_ += info->dynamic_sizeof() / 8U;
    ASSERT_ND(page->get_foster_major().components.offset || cur == last->tail_);
    cur = page->get_foster_major().components.offset;
    prev_high = high;
  }

  return kErrorCodeOk;
}

void MasstreeComposeContext::sort_page_boundary_info() {
  ASSERT_ND(page_boundary_info_cur_pos_ <= page_boundary_info_capacity_);
  ASSERT_ND(page_boundary_elements_ <= max_page_boundary_elements_);
  debugging::StopWatch watch;
  std::sort(page_boundary_sort_, page_boundary_sort_ + page_boundary_elements_);
  watch.stop();
  VLOG(0) << "MasstreeStorage-" << id_ << " sorted " << page_boundary_elements_ << " entries"
    << " to track page_boundary_info in " << watch.elapsed_ms() << "ms";
}

inline SnapshotPagePointer MasstreeComposeContext::lookup_page_boundary_info(
  uint8_t btree_level,
  uint8_t layer,
  const KeySlice* prefixes,
  KeySlice low,
  KeySlice high) const {
  PageBoundarySort dummy;
  dummy.hash_ = PageBoundaryInfo::calculate_hash(btree_level, layer, prefixes, low, high);
  const PageBoundarySort* begin_entry = std::lower_bound(
    page_boundary_sort_,
    page_boundary_sort_ + page_boundary_elements_,
    dummy,
    PageBoundarySort::static_less_than);
  uint32_t begin = begin_entry - page_boundary_sort_;
  for (uint32_t i = begin; i < page_boundary_elements_; ++i) {
    if (page_boundary_sort_[i].hash_ != dummy.hash_) {
      break;
    }
    const PageBoundaryInfo* info = get_page_boundary_info(page_boundary_sort_[i].info_pos_);
    if (!info->exact_match(btree_level, layer, prefixes, low, high)) {
      continue;
    }
    SnapshotLocalPageId local_page_id = info->get_local_page_id();
    ASSERT_ND(local_page_id > 0);
    SnapshotPagePointer page_id = to_snapshot_page_pointer(snapshot_id_, numa_node_, local_page_id);
    ASSERT_ND(page_id < get_writer()->get_next_page_id());  // otherwise not yet flushed
    return page_id;
  }
  DVLOG(1) << "exactly corresponding page boundaries not found. cannot install a snapshot"
    << " pointer";
  return 0;
}

ErrorStack MasstreeComposeContext::install_snapshot_pointers(uint64_t* installed_count) const {
  ASSERT_ND(allocated_pages_ == 1U);  // this method must be called at the end, after flush_buffer
  *installed_count = 0;
  VolatilePagePointer pointer = storage_.get_control_block()->root_page_pointer_.volatile_pointer_;
  if (pointer.is_null()) {
    VLOG(0) << "No volatile pages.. maybe while restart?";
    return kRetOk;
  }

  const memory::GlobalVolatilePageResolver& resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  MasstreeIntermediatePage* volatile_root
    = reinterpret_cast<MasstreeIntermediatePage*>(resolver.resolve_offset(pointer));

  KeySlice prefixes[kMaxLayers];
  std::memset(prefixes, 0, sizeof(prefixes));

  // then, at least this snapshot contains no snapshot page in next layers, so we don't have to
  // check any border pages (although the volatile world might now contain next layer).
  const bool no_next_layer = merge_sort_->are_all_single_layer_logs();

  debugging::StopWatch watch;
  // Install pointers. same as install_snapshot_pointers_recurse EXCEPT we skip other partition.
  // Because the volatle world only splits pages (no merge), no child spans two partitions.
  // So, it's easy to determine whether this composer should follow the pointer.
  // If there is such a pointer as a rare case for whatever reason, we skip it in this snapshot.

  // We don't even need partitioning information to figure this out. We just check the
  // new pointers in the root page and find a volatile pointer that equals or contains the range.
  struct InstallationInfo {
    KeySlice low_;
    KeySlice high_;
    SnapshotPagePointer pointer_;
  };
  std::vector<InstallationInfo> infos;  // only one root page, so vector wouldn't hurt
  infos.reserve(kMaxIntermediatePointers);
  for (MasstreeIntermediatePointerIterator it(root_); it.is_valid(); it.next()) {
    SnapshotPagePointer pointer = it.get_pointer().snapshot_pointer_;
    if (pointer == 0) {
      continue;
    }

    snapshot::SnapshotId snapshot_id = extract_snapshot_id_from_snapshot_pointer(pointer);
    ASSERT_ND(snapshot_id != snapshot::kNullSnapshotId);
    if (snapshot_id == snapshot_id_) {
      ASSERT_ND(extract_numa_node_from_snapshot_pointer(pointer) == numa_node_);
    }
    InstallationInfo info = {it.get_low_key(), it.get_high_key(), pointer};
    infos.emplace_back(info);
  }

  // vector, same above. in the recursed functions, we use stack array to avoid frequent heap alloc.
  std::vector<VolatilePagePointer> recursion_targets;
  recursion_targets.reserve(kMaxIntermediatePointers);
  {
    // first, install pointers. this step is quickly and safely done after taking a page lock.
    // recursion is done after releasing this lock because it might take long time.
    assorted::memory_fence_acq_rel();
    // TODO(Hideaki) PAGE LOCK HERE
    uint16_t cur = 0;  // upto infos[cur] is the first that might fully contain the range
    for (MasstreeIntermediatePointerIterator it(volatile_root); it.is_valid(); it.next()) {
      KeySlice low = it.get_low_key();
      KeySlice high = it.get_high_key();
      // the snapshot's page range is more coarse or same,
      //  1) page range exactly matches -> install pointer and recurse
      //  2) volatile range is subset of snapshot's -> just recurse
      // in either case, we are interested only when
      // snapshot's low <= volatile's low && snapshot's high >= volatile's high
      while (cur < infos.size() && (infos[cur].low_ > low || infos[cur].high_ < high)) {
        ++cur;
      }
      if (cur == infos.size()) {
        break;
      }
      ASSERT_ND(infos[cur].low_ <= low && infos[cur].high_ >= high);
      DualPagePointer& pointer = volatile_root->get_minipage(it.index_).pointers_[it.index_mini_];
      if (infos[cur].low_ == low && infos[cur].high_ == high) {
        pointer.snapshot_pointer_ = infos[cur].pointer_;
        ++cur;  // in this case we know next volatile pointer is beyond this snapshot range
      }
      // will recurse on it.
      if (!pointer.volatile_pointer_.is_null()) {
        recursion_targets.emplace_back(pointer.volatile_pointer_);
      }
    }
    assorted::memory_fence_acq_rel();
  }

  // The recursion is not transactionally protected. That's fine; even if we skip over
  // something, it's just that we can't drop the page this time.
  for (VolatilePagePointer pointer : recursion_targets) {
    MasstreePage* child = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(pointer));
    if (no_next_layer && child->is_border()) {
      // unlike install_snapshot_pointers_recurse_intermediate, we check it here.
      // because btree-levels might be unbalanced in first layer's root, we have to follow
      // the pointer to figure this out.
      continue;
    }
    WRAP_ERROR_CODE(install_snapshot_pointers_recurse(
      resolver,
      0,
      prefixes,
      child,
      installed_count));
  }
  watch.stop();
  LOG(INFO) << "MasstreeStorage-" << id_ << " installed " << *installed_count << " pointers"
    << " in " << watch.elapsed_ms() << "ms";
  return kRetOk;
}

inline ErrorCode MasstreeComposeContext::install_snapshot_pointers_recurse(
  const memory::GlobalVolatilePageResolver& resolver,
  uint8_t layer,
  KeySlice* prefixes,
  MasstreePage* volatile_page,
  uint64_t* installed_count) const {
  if (volatile_page->is_border()) {
    return install_snapshot_pointers_recurse_border(
      resolver,
      layer,
      prefixes,
      reinterpret_cast<MasstreeBorderPage*>(volatile_page),
      installed_count);
  } else {
    return install_snapshot_pointers_recurse_intermediate(
      resolver,
      layer,
      prefixes,
      reinterpret_cast<MasstreeIntermediatePage*>(volatile_page),
      installed_count);
  }
}

ErrorCode MasstreeComposeContext::install_snapshot_pointers_recurse_intermediate(
  const memory::GlobalVolatilePageResolver& resolver,
  uint8_t layer,
  KeySlice* prefixes,
  MasstreeIntermediatePage* volatile_page,
  uint64_t* installed_count) const {
  ASSERT_ND(volatile_page->get_layer() == layer);

  // we do not track down to foster-children. most pages are reachable without it anyways.
  // we have to recurse to children, but we _might_ know that we can ignore next layers (borders)
  const bool no_next_layer = (merge_sort_->get_longest_key_length() <= (layer + 1U) * 8U);
  const bool is_child_intermediate = volatile_page->get_btree_level() >= 2U;
  const bool follow_children = !no_next_layer || is_child_intermediate;
  const uint8_t btree_level  = volatile_page->get_btree_level() - 1U;

  // at this point we don't have to worry about partitioning. this subtree is solely ours.
  // we just follow every volatile pointer. this might mean a wasted recursion if this snapshot
  // modified only a part of the storage. However, the checks in root level have already cut most
  // un-touched subtrees. So, it won't be a big issue.

  // 160*8 bytes in each stack frame... hopefully not too much. nowadays default stack size is 2MB.
  VolatilePagePointer recursion_targets[kMaxIntermediatePointers];
  uint16_t recursion_target_count = 0;

  {
    // look for exactly matching page boundaries. we install pointer only in exact match case
    // while we recurse on every volatile page.
    assorted::memory_fence_acq_rel();
    // TODO(Hideaki) PAGE LOCK HERE
    for (MasstreeIntermediatePointerIterator it(volatile_page); it.is_valid(); it.next()) {
      KeySlice low = it.get_low_key();
      KeySlice high = it.get_high_key();
      DualPagePointer& pointer = volatile_page->get_minipage(it.index_).pointers_[it.index_mini_];
      SnapshotPagePointer snapshot_pointer
        = lookup_page_boundary_info(btree_level, layer, prefixes, low, high);
      if (snapshot_pointer != 0) {
        pointer.snapshot_pointer_ = snapshot_pointer;
        ++(*installed_count);
      } else {
        DVLOG(3) << "Oops, no matching boundary. low=" << low << ", high=" << high;
      }
      if (follow_children && !pointer.volatile_pointer_.is_null()) {
        ASSERT_ND(recursion_target_count < kMaxIntermediatePointers);
        recursion_targets[recursion_target_count] = pointer.volatile_pointer_;
        ++recursion_target_count;
      }
    }
    assorted::memory_fence_acq_rel();
  }

  for (uint16_t i = 0; i < recursion_target_count; ++i) {
    MasstreePage* child
      = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(recursion_targets[i]));
    if (no_next_layer && child->is_border()) {
      continue;
    }
    CHECK_ERROR_CODE(install_snapshot_pointers_recurse(
      resolver,
      layer,
      prefixes,
      child,
      installed_count));
  }

  return kErrorCodeOk;
}

ErrorCode MasstreeComposeContext::install_snapshot_pointers_recurse_border(
  const memory::GlobalVolatilePageResolver& resolver,
  uint8_t layer,
  KeySlice* prefixes,
  MasstreeBorderPage* volatile_page,
  uint64_t* installed_count) const {
  ASSERT_ND(volatile_page->get_layer() == layer);
  const bool no_next_layer = (merge_sort_->get_longest_key_length() <= (layer + 1U) * 8U);
  ASSERT_ND(!no_next_layer);
  const bool no_next_next_layer = (merge_sort_->get_longest_key_length() <= (layer + 2U) * 8U);

  // unlike intermediate pages, we don't need a page lock.
  // a record in border page is never physically deleted.
  // also once a record becomes next-layer pointer, it never gets reverted to a usual record.
  // just make sure we put consume (no need to be acquire) fence in a few places.
  uint8_t key_count = volatile_page->get_key_count();
  assorted::memory_fence_consume();
  for (uint8_t i = 0; i < key_count; ++i) {
    if (!volatile_page->does_point_to_layer(i)) {
      continue;
    }
    assorted::memory_fence_consume();
    DualPagePointer* next_pointer = volatile_page->get_next_layer(i);
    if (next_pointer->volatile_pointer_.is_null()) {
      // this also means that there was no change since the last snapshot.
      continue;
    }

    MasstreePage* child
      = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(next_pointer->volatile_pointer_));
    ASSERT_ND(child->get_low_fence() == kInfimumSlice);
    ASSERT_ND(child->is_high_fence_supremum());
    uint8_t btree_level = child->get_btree_level();
    KeySlice slice = volatile_page->get_slice(i);
    prefixes[layer] = slice;
    SnapshotPagePointer snapshot_pointer
      = lookup_page_boundary_info(btree_level, layer + 1U, prefixes, kInfimumSlice, kSupremumSlice);
    if (snapshot_pointer != 0) {
      // okay, exactly corresponding snapshot page found
      next_pointer->snapshot_pointer_ = snapshot_pointer;
      ++(*installed_count);
    } else {
      DVLOG(2) << "Oops, no matching boundary. border next layer";
    }

    if (child->is_border() && no_next_next_layer) {
      // the root page of next layer is a border page and we don't have that long keys.
      continue;
    }
    CHECK_ERROR_CODE(install_snapshot_pointers_recurse(
      resolver,
      layer + 1U,
      prefixes,
      child,
      installed_count));
  }

  return kErrorCodeOk;
}

/////////////////////////////////////////////////////////////////////////////
///
///  drop_volatiles and related methods
///
/////////////////////////////////////////////////////////////////////////////
Composer::DropResult MasstreeComposer::drop_volatiles(
  const Composer::DropVolatilesArguments& args) {
  Composer::DropResult result(args);
  if (storage_.get_masstree_metadata()->keeps_all_volatile_pages()) {
    LOG(INFO) << "Keep-all-volatile: Storage-" << storage_.get_name()
      << " is configured to keep all volatile pages.";
    result.dropped_all_ = false;
    return result;
  }

  DualPagePointer* root_pointer = &storage_.get_control_block()->root_page_pointer_;
  MasstreeIntermediatePage* volatile_page = reinterpret_cast<MasstreeIntermediatePage*>(
    resolve_volatile(root_pointer->volatile_pointer_));
  if (volatile_page == nullptr) {
    LOG(INFO) << "No volatile root page. Probably while restart";
    return result;
  }

  // Unlike array-storage, each thread decides partititions to follow a bit differently
  // because the snapshot pointer might be not installed.
  // We check the partitioning assignment.
  PartitionerMetadata* metadata = PartitionerMetadata::get_metadata(engine_, storage_id_);
  ASSERT_ND(metadata->valid_);
  MasstreePartitionerData* data
    = reinterpret_cast<MasstreePartitionerData*>(metadata->locate_data(engine_));
  ASSERT_ND(data->partition_count_ > 0);
  ASSERT_ND(data->partition_count_ < kMaxIntermediatePointers);
  uint16_t cur = 0;
  for (MasstreeIntermediatePointerIterator it(volatile_page); it.is_valid(); it.next()) {
    DualPagePointer* pointer = &volatile_page->get_minipage(it.index_).pointers_[it.index_mini_];
    if (!args.partitioned_drop_) {
      result.combine(drop_volatiles_recurse(args, pointer));
      continue;
    }
    // the page boundaries as of partitioning might be different from the volatile page's,
    // but at least it's more coarse. Each pointer belongs to just one partition.
    // same trick as install_pointers.
    KeySlice low = it.get_low_key();
    KeySlice high = it.get_high_key();
    while (true) {
      KeySlice partition_high;
      if (cur + 1U == data->partition_count_) {
        partition_high = kSupremumSlice;
      } else {
        partition_high = data->low_keys_[cur + 1U];
      }
      if (high <= partition_high) {
        break;
      }
      ++cur;
    }
    ASSERT_ND(cur < data->partition_count_);
    if (data->partitions_[cur] == args.my_partition_) {
      if (data->low_keys_[cur] != low) {
        VLOG(0) << "Not exactly matching page boundary.";  // but not a big issue.
      }
      result.combine(drop_volatiles_recurse(args, pointer));
    }
  }

  // we so far always keep the volatile root of a masstree storage.
  return result;
}

void MasstreeComposer::drop_root_volatile(const Composer::DropVolatilesArguments& args) {
  if (storage_.get_masstree_metadata()->keeps_all_volatile_pages()) {
    LOG(INFO) << "Oh, but keep-all-volatile is on. Storage-" << storage_.get_name()
      << " is configured to keep all volatile pages.";
    return;
  }
  DualPagePointer* root_pointer = &storage_.get_control_block()->root_page_pointer_;
  MasstreeIntermediatePage* volatile_page = reinterpret_cast<MasstreeIntermediatePage*>(
    resolve_volatile(root_pointer->volatile_pointer_));
  if (volatile_page == nullptr) {
    LOG(INFO) << "Oh, but root volatile page already null";
    return;
  }

  if (is_to_keep_volatile(0, volatile_page->get_btree_level())) {
    LOG(INFO) << "Oh, but " << storage_ << " is configured to keep the root page.";
    return;
  }

  // yes, we can drop ALL volatile pages!
  LOG(INFO) << "Okay, drop em all!!";
  drop_all_recurse(args, root_pointer);
  root_pointer->volatile_pointer_.clear();
}

void MasstreeComposer::drop_all_recurse(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer) {
  MasstreePage* page = resolve_volatile(pointer->volatile_pointer_);
  if (page == nullptr) {
    return;
  }
  drop_all_recurse_page_only(args, page);
  args.drop(engine_, pointer->volatile_pointer_);
  pointer->volatile_pointer_.clear();  // not required, but to be clear.
}

void MasstreeComposer::drop_all_recurse_page_only(
  const Composer::DropVolatilesArguments& args,
  MasstreePage* page) {
  ASSERT_ND(page);
  if (page->has_foster_child()) {
    MasstreePage* minor = resolve_volatile(page->get_foster_minor());
    MasstreePage* major = resolve_volatile(page->get_foster_major());
    drop_all_recurse_page_only(args, minor);
    drop_all_recurse_page_only(args, major);
    return;
  }

  ASSERT_ND(!page->has_foster_child());
  if (page->is_border()) {
    MasstreeBorderPage* border = as_border(page);
    const uint8_t key_count = page->get_key_count();
    for (uint8_t i = 0; i < key_count; ++i) {
      if (border->does_point_to_layer(i)) {
        DualPagePointer* pointer = border->get_next_layer(i);
        drop_all_recurse(args, pointer);
      }
    }
  } else {
    MasstreeIntermediatePage* casted = as_intermediate(page);
    for (MasstreeIntermediatePointerIterator it(casted); it.is_valid(); it.next()) {
      DualPagePointer* pointer = const_cast<DualPagePointer*>(&it.get_pointer());
      drop_all_recurse(args, pointer);
    }
  }
}

inline MasstreePage* MasstreeComposer::resolve_volatile(VolatilePagePointer pointer) {
  if (pointer.is_null()) {
    return nullptr;
  }
  const memory::GlobalVolatilePageResolver& page_resolver
    = engine_->get_memory_manager()->get_global_volatile_page_resolver();
  return reinterpret_cast<MasstreePage*>(page_resolver.resolve_offset(pointer));
}

inline Composer::DropResult MasstreeComposer::drop_volatiles_recurse(
  const Composer::DropVolatilesArguments& args,
  DualPagePointer* pointer) {
  Composer::DropResult result(args);
  if (pointer->volatile_pointer_.is_null()) {
    return result;
  }
  // Unlike array-storage, even if this pointer itself has not updated the snapshot pointer,
  // we might have updated the descendants because of not-matching page boundaries.
  // so, anyway we recurse.
  MasstreePage* page = resolve_volatile(pointer->volatile_pointer_);

  // Also, masstree might have foster children.
  // they are kept as far as this page is kept.
  if (page->has_foster_child()) {
    MasstreePage* minor = resolve_volatile(page->get_foster_minor());
    MasstreePage* major = resolve_volatile(page->get_foster_major());
    if (page->is_border()) {
      result.combine(drop_volatiles_border(args, as_border(minor)));
      result.combine(drop_volatiles_border(args, as_border(major)));
    } else {
      result.combine(drop_volatiles_intermediate(args, as_intermediate(minor)));
      result.combine(drop_volatiles_intermediate(args, as_intermediate(major)));
    }
    ASSERT_ND(result.max_observed_ >= args.snapshot_.valid_until_epoch_);
  } else {
    if (page->is_border()) {
      result.combine(drop_volatiles_border(args, as_border(page)));
    } else {
      result.combine(drop_volatiles_intermediate(args, as_intermediate(page)));
    }
  }
  if (result.max_observed_ > args.snapshot_.valid_until_epoch_ || !result.dropped_all_) {
    DVLOG(1) << "Couldn't drop a volatile page that has a recent modification";
    result.dropped_all_ = false;
    return result;
  }
  ASSERT_ND(result.max_observed_ == args.snapshot_.valid_until_epoch_);
  ASSERT_ND(result.dropped_all_);
  bool updated_pointer = is_updated_pointer(args, pointer->snapshot_pointer_);
  if (updated_pointer) {
    if (is_to_keep_volatile(page->get_layer(), page->get_btree_level())) {
      DVLOG(2) << "Exempted";
      result.dropped_all_ = false;  // max_observed is satisfactory, but we chose to not drop.
    } else {
      if (page->has_foster_child()) {
        drop_foster_twins(args, page);
      }
      args.drop(engine_, pointer->volatile_pointer_);
      pointer->volatile_pointer_.clear();
    }
  } else {
    DVLOG(1) << "Couldn't drop a volatile page that has a non-matching boundary";
    result.dropped_all_ = false;
  }
  return result;
}

void MasstreeComposer::drop_foster_twins(
  const Composer::DropVolatilesArguments& args,
  MasstreePage* page) {
  ASSERT_ND(page->has_foster_child());
  MasstreePage* minor = resolve_volatile(page->get_foster_minor());
  MasstreePage* major = resolve_volatile(page->get_foster_major());
  if (minor->has_foster_child()) {
    drop_foster_twins(args, minor);
  }
  if (major->has_foster_child()) {
    drop_foster_twins(args, major);
  }
  args.drop(engine_, page->get_foster_minor());
  args.drop(engine_, page->get_foster_major());
}

Composer::DropResult MasstreeComposer::drop_volatiles_intermediate(
  const Composer::DropVolatilesArguments& args,
  MasstreeIntermediatePage* page) {
  ASSERT_ND(!page->header().snapshot_);
  ASSERT_ND(!page->is_border());

  Composer::DropResult result(args);
  if (page->has_foster_child()) {
    // iff both minor and major foster children dropped all descendants,
    // we drop this page and its foster children altogether.
    // otherwise, we keep all of them.
    MasstreeIntermediatePage* minor = as_intermediate(resolve_volatile(page->get_foster_minor()));
    MasstreeIntermediatePage* major = as_intermediate(resolve_volatile(page->get_foster_major()));
    result.combine(drop_volatiles_intermediate(args, minor));
    result.combine(drop_volatiles_intermediate(args, major));
    return result;
  }

  // Explore/replace children first because we need to know if there is new modification.
  // In that case, we must keep this volatile page, too.
  ASSERT_ND(!page->has_foster_child());
  for (MasstreeIntermediatePointerIterator it(page); it.is_valid(); it.next()) {
    DualPagePointer* pointer = &page->get_minipage(it.index_).pointers_[it.index_mini_];
    result.combine(drop_volatiles_recurse(args, pointer));
  }

  return result;
}

inline Composer::DropResult MasstreeComposer::drop_volatiles_border(
  const Composer::DropVolatilesArguments& args,
  MasstreeBorderPage* page) {
  ASSERT_ND(!page->header().snapshot_);
  ASSERT_ND(page->is_border());

  Composer::DropResult result(args);
  if (page->has_foster_child()) {
    MasstreeBorderPage* minor = as_border(resolve_volatile(page->get_foster_minor()));
    MasstreeBorderPage* major = as_border(resolve_volatile(page->get_foster_major()));
    result.combine(drop_volatiles_border(args, minor));
    result.combine(drop_volatiles_border(args, major));
    return result;
  }

  ASSERT_ND(!page->has_foster_child());
  const uint8_t key_count = page->get_key_count();
  for (uint8_t i = 0; i < key_count; ++i) {
    if (page->does_point_to_layer(i)) {
      DualPagePointer* pointer = page->get_next_layer(i);
      result.combine(drop_volatiles_recurse(args, pointer));
    } else {
      Epoch epoch = page->get_owner_id(i)->xct_id_.get_epoch();
      ASSERT_ND(epoch.is_valid());
      result.on_rec_observed(epoch);
    }
  }
  return result;
}
inline bool MasstreeComposer::is_updated_pointer(
  const Composer::DropVolatilesArguments& args,
  SnapshotPagePointer pointer) const {
  snapshot::SnapshotId snapshot_id = extract_snapshot_id_from_snapshot_pointer(pointer);
  return (pointer != 0 && args.snapshot_.id_ == snapshot_id);
}
inline bool MasstreeComposer::is_to_keep_volatile(uint8_t layer, uint16_t btree_level) const {
  const MasstreeMetadata* meta = storage_.get_masstree_metadata();
  // snapshot_drop_volatile_pages_layer_threshold_:
  // Number of B-trie layers of volatile pages to keep after each snapshotting.
  // Ex. 0 drops all (always false).
  if (layer < meta->snapshot_drop_volatile_pages_layer_threshold_) {
    return true;
  }
  // snapshot_drop_volatile_pages_btree_levels_:
  // Volatile pages of this B-tree level or higher are always kept after each snapshotting.
  // Ex. 0 keeps all. 0xFF drops all.
  return (btree_level >= meta->snapshot_drop_volatile_pages_btree_levels_);
}

std::ostream& operator<<(std::ostream& o, const MasstreeComposeContext::PathLevel& v) {
  o << "<PathLevel>"
    << "<layer_>" << static_cast<int>(v.layer_) << "</layer_>"
    << "<next_original_>" << static_cast<int>(v.next_original_) << "</next_original_>"
    << "<next_original_mini>" << static_cast<int>(v.next_original_mini_) << "</next_original_mini>"
    << "<head>" << v.head_ << "</head>"
    << "<tail_>" << v.tail_ << "</tail_>"
    << "<page_count_>" << v.page_count_ << "</page_count_>"
    << "<low_fence_>" << assorted::Hex(v.low_fence_, 16) << "</low_fence_>"
    << "<high_fence_>" << assorted::Hex(v.high_fence_, 16) << "</high_fence_>"
    << "<next_original_slice_>"
      << assorted::Hex(v.next_original_slice_, 16) << "</next_original_slice_>"
    << "</PathLevel>";
  return o;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
