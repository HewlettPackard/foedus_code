/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
  CHECK_ERROR(MasstreeComposeContext::assure_work_memory_size(args));
  MasstreeComposeContext context(engine_, storage_id_, args);
  CHECK_ERROR(context.execute());
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

    MasstreeIntermediatePage* base = reinterpret_cast<MasstreeIntermediatePage*>(new_root);
    ASSERT_ND(!base->is_border());
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

ErrorStack MasstreeComposer::replace_pointers(const Composer::ReplacePointersArguments& args) {
  return MasstreeStorage(engine_, storage_id_).replace_pointers(args);
}

///////////////////////////////////////////////////////////////////////
///
///  MasstreeStreamStatus methods
///
///////////////////////////////////////////////////////////////////////
void MasstreeStreamStatus::init(snapshot::SortedBuffer* stream) {
  stream_ = stream;
  stream_->assert_checks();
  buffer_ = stream->get_buffer();
  buffer_size_ = stream->get_buffer_size();
  cur_absolute_pos_ = stream->get_cur_block_abosulte_begin();
  // this is the initial read of this block, so we are sure cur_block_abosulte_begin is the window
  ASSERT_ND(cur_absolute_pos_ >= stream->get_offset());
  cur_relative_pos_ = cur_absolute_pos_ - stream->get_offset();
  end_absolute_pos_ = stream->get_cur_block_abosulte_end();
  ended_ = false;
  if (cur_absolute_pos_ >= end_absolute_pos_) {
    ended_ = true;
  }
}

inline ErrorCode MasstreeStreamStatus::next() {
  ASSERT_ND(!ended_);
  const MasstreeCommonLogType* entry = get_entry();
  cur_absolute_pos_ += entry->header_.log_length_;
  cur_relative_pos_ += entry->header_.log_length_;
  if (UNLIKELY(cur_absolute_pos_ >= end_absolute_pos_)) {
    ended_ = true;
    return kErrorCodeOk;
  } else if (UNLIKELY(cur_relative_pos_ >= buffer_size_)) {
    CHECK_ERROR_CODE(stream_->wind(cur_absolute_pos_));
    cur_relative_pos_ = stream_->to_relative_pos(cur_absolute_pos_);
  }
  return kErrorCodeOk;
}

inline const MasstreeCommonLogType* MasstreeStreamStatus::get_entry() const {
  const MasstreeCommonLogType* entry
    = reinterpret_cast<const MasstreeCommonLogType*>(buffer_ + cur_relative_pos_);
  ASSERT_ND(entry->header_.get_type() == log::kLogCodeMasstreeOverwrite
    || entry->header_.get_type() == log::kLogCodeMasstreeDelete
    || entry->header_.get_type() == log::kLogCodeMasstreeInsert);
  return entry;
}


///////////////////////////////////////////////////////////////////////
///
///  MasstreeComposeContext methods
///
///////////////////////////////////////////////////////////////////////
MasstreeComposeContext::MasstreeComposeContext(
  Engine* engine,
  StorageId id,
  const Composer::ComposeArguments& args)
  : engine_(engine),
    id_(id),
    storage_(engine, id),
    args_(args),
    numa_node_(get_writer()->get_numa_node()),
    max_pages_(get_writer()->get_page_size()),
    root_(reinterpret_cast<MasstreeIntermediatePage*>(args.root_info_page_)),
    // max_intermediates_(get_writer()->get_intermediate_size()),
    page_base_(reinterpret_cast<Page*>(get_writer()->get_page_base())),
    // intermediate_base_(reinterpret_cast<MasstreePage*>(get_writer()->get_intermediate_base())) {
    original_base_(reinterpret_cast<Page*>(args.work_memory_->get_block())),
    inputs_(reinterpret_cast<MasstreeStreamStatus*>(
      reinterpret_cast<char*>(args.work_memory_->get_block()) + kPageSize * (kMaxLevels + 1ULL))) {
  page_id_base_ = get_writer()->get_next_page_id();
  root_index_ = 0;
  root_index_mini_ = 0;
  allocated_pages_ = 0;
  ended_inputs_count_ = 0;
  // allocated_intermediates_ = 0;
  cur_path_levels_ = 0;
  std::memset(cur_path_, 0, sizeof(cur_path_));
  std::memset(cur_prefix_be_, 0, sizeof(cur_prefix_be_));
}

ErrorStack MasstreeComposeContext::assure_work_memory_size(const Composer::ComposeArguments& args) {
  uint64_t required_size = sizeof(Page) * (kMaxLevels + 1U);  // original pages
  required_size += sizeof(MasstreeStreamStatus) * args.log_streams_count_;
  WRAP_ERROR_CODE(args.work_memory_->assure_capacity(required_size));
  return kRetOk;
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

ErrorStack MasstreeComposeContext::execute() {
  write_dummy_page_zero();
  CHECK_ERROR(init_inputs());
  CHECK_ERROR(init_root());

#ifndef NDEBUG
  char debug_prev[8192];
  uint16_t debug_prev_length = 0;
#endif  // NDEBUG

  uint64_t flush_threshold = max_pages_ * 8ULL / 10ULL;
  uint64_t processed = 0;
  while (ended_inputs_count_ < args_.log_streams_count_) {
    read_inputs();
    const MasstreeCommonLogType* entry = next_entry_;
    const char* key = entry->get_key();
    uint16_t key_length = entry->key_length_;
    ASSERT_ND(key_length > 0);
    ASSERT_ND(cur_path_levels_ == 0 || get_page(get_last_level()->tail_)->is_border());
    ASSERT_ND(is_key_aligned_and_zero_padded(key, key_length));

#ifndef NDEBUG
    if (key_length < debug_prev_length) {
      ASSERT_ND(std::memcmp(debug_prev, key, key_length) <= 0);
    } else {
      ASSERT_ND(std::memcmp(debug_prev, key, debug_prev_length) <= 0);
    }
    std::memcpy(debug_prev, key, key_length);
    debug_prev_length = key_length;
#endif  // NDEBUG

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
      ASSERT_ND(page->compare_key(index, key, key_length));
      char* record = page->get_record(index);
      const MasstreeOverwriteLogType* casted
        = reinterpret_cast<const MasstreeOverwriteLogType*>(entry);
      casted->apply_record(nullptr, id_, page->get_owner_id(index), record);
    } else if (entry->header_.get_type() == log::kLogCodeMasstreeDelete) {
      // [Delete] Physically deletes the last record in this page.
      uint16_t index = key_count - 1;
      ASSERT_ND(!page->does_point_to_layer(index));
      ASSERT_ND(page->compare_key(index, key, key_length));
      page->set_key_count(index);
    } else {
      // [Insert] next-layer is already handled above, so just append it.
      ASSERT_ND(entry->header_.get_type() == log::kLogCodeMasstreeInsert);
      ASSERT_ND(key_count == 0 || !page->compare_key(key_count - 1, key, key_length));  // no dup
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
    WRAP_ERROR_CODE(advance());

    ++processed;
    if (UNLIKELY(allocated_pages_ >= flush_threshold)) {
      CHECK_ERROR(flush_buffer());
    }
  }

  VLOG(0) << storage_ << " processed " << processed << " logs. now pages=" << allocated_pages_;
  CHECK_ERROR(flush_buffer());
  return kRetOk;
}

ErrorStack MasstreeComposeContext::init_root() {
  SnapshotPagePointer snapshot_page_id
    = storage_.get_control_block()->root_page_pointer_.snapshot_pointer_;
  if (snapshot_page_id != 0) {
    // based on previous snapshot page.
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

    root_->initialize_snapshot_page(id_, 0, 0, kInfimumSlice, kSupremumSlice);
    ASSERT_ND(data->low_keys_[0] == kInfimumSlice);
    root_->get_minipage(0).pointers_[0].snapshot_pointer_ = 0;
    for (uint16_t i = 1; i < data->partition_count_; ++i) {
      root_->append_pointer_snapshot(data->low_keys_[i], 0);
    }
  }

  return kRetOk;
}

ErrorStack MasstreeComposeContext::init_inputs() {
  for (uint32_t i = 0; i < args_.log_streams_count_; ++i) {
    inputs_[i].init(args_.log_streams_[i]);
    if (inputs_[i].ended_) {
      ++ended_inputs_count_;
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

inline void fill_payload_padded(char* destination, const char* source, uint16_t count) {
  if (count > 0) {
    ASSERT_ND(is_key_aligned_and_zero_padded(source, count));
    std::memcpy(destination, source, assorted::align8(count));
  }
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
    store_cur_prefix_be(parent->get_layer(), prefix_slice);
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

  store_cur_prefix_be(parent->get_layer(), prefix_slice);
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

inline ErrorCode MasstreeComposeContext::advance() {
  ASSERT_ND(!inputs_[next_input_].ended_);
  // advance the chosen stream
  CHECK_ERROR_CODE(inputs_[next_input_].next());
  if (inputs_[next_input_].ended_) {
    ++ended_inputs_count_;
    if (ended_inputs_count_ >= args_.log_streams_count_) {
      ASSERT_ND(ended_inputs_count_ == args_.log_streams_count_);
    }
  }
  return kErrorCodeOk;
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

inline void MasstreeComposeContext::read_inputs() {
  ASSERT_ND(ended_inputs_count_ < args_.log_streams_count_);
  if (args_.log_streams_count_ == 1U) {
    ASSERT_ND(!inputs_[0].ended_);
    next_input_ = 0;
    next_entry_ = inputs_[0].get_entry();
  } else {
    next_input_ = args_.log_streams_count_;
    next_entry_ = nullptr;
    for (uint32_t i = 0; i < args_.log_streams_count_; ++i) {
      if (!inputs_[i].ended_) {
        const MasstreeCommonLogType* entry = inputs_[i].get_entry();
        if (next_entry_ == nullptr
          || MasstreeCommonLogType::compare_logs(entry, next_entry_) < 0) {
          next_input_ = i;
          next_entry_ = inputs_[i].get_entry();
        }
      }
    }
  }
  ASSERT_ND(next_input_ < args_.log_streams_count_);
  ASSERT_ND(next_entry_);
}

inline void MasstreeComposeContext::append_border(
  KeySlice slice,
  xct::XctId xct_id,
  uint16_t remaining_length,
  const char* suffix,
  uint16_t payload_count,
  const char* payload,
  PathLevel* level) {
  ASSERT_ND(remaining_length != MasstreeBorderPage::kKeyLengthNextLayer);
  MasstreeBorderPage* target = as_border(get_page(level->tail_));
  uint16_t key_count = target->get_key_count();
  uint16_t new_index = key_count;
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
  new_target->initialize_snapshot_page(id_, new_page_id, level->layer_, middle, high_fence);
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

ErrorStack MasstreeComposeContext::grow_subtree(
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
  for (memory::PagePoolOffset cur = last->head_; cur != 0;) {
    MasstreePage* page = get_page(cur);
    ASSERT_ND(page->get_foster_minor().is_null());
    FenceAndPointer child;
    child.pointer_ = page_id_base_ + cur;
    child.low_fence_ = page->get_low_fence();
    children.emplace_back(child);
    cur = page->get_foster_major().components.offset;
    page->set_foster_major_offset_unsafe(0);  // no longer needed
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

  if (last->page_count_ > 1U) {
    // recurse until we get just one page. this also means, when there are skewed inserts,
    // we might have un-balanced masstree (some subtree is deeper). not a big issue.
    CHECK_ERROR(grow_subtree(root_pointer, subtree_low, subtree_high));
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
      CHECK_ERROR(grow_subtree(parent_pointer, kInfimumSlice, kSupremumSlice));
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
  SnapshotPagePointer current_pointer = first->head_ + page_id_base_;
  DualPagePointer& root_pointer = root_->get_minipage(root_index_).pointers_[root_index_mini_];
  ASSERT_ND(root_pointer.volatile_pointer_.is_null());
  ASSERT_ND(root_pointer.snapshot_pointer_ == current_pointer);
  KeySlice root_low, root_high;
  root_->extract_separators_snapshot(root_index_, root_index_mini_, &root_low, &root_high);
  if (first->page_count_ > 1U) {
    CHECK_ERROR(grow_subtree(&root_pointer.snapshot_pointer_, root_low, root_high));
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

void MasstreeComposeContext::store_cur_prefix_be(uint8_t layer, KeySlice prefix_slice) {
  assorted::write_bigendian<KeySlice>(prefix_slice, cur_prefix_be_ + layer * kSliceLen);
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
