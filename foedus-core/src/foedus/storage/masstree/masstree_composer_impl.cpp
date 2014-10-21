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
  VLOG(0) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}

ErrorStack MasstreeComposer::construct_root(const Composer::ConstructRootArguments& args) {
  ASSERT_ND(args.root_info_pages_count_ > 0);
  VLOG(0) << to_string() << " combining " << args.root_info_pages_count_ << " root page info.";
  debugging::StopWatch stop_watch;
  stop_watch.stop();
  VLOG(0) << to_string() << " done in " << stop_watch.elapsed_ms() << "ms.";
  return kRetOk;
}


std::string MasstreeComposer::to_string() const {
  return std::string("MasstreeComposer-") + std::to_string(storage_id_);
}

ErrorStack MasstreeComposer::replace_pointers(const Composer::ReplacePointersArguments& args) {
  ASSERT_ND(args.new_root_page_pointer_ != 0);
  VLOG(0) << to_string() << " replacing pointers...";
  debugging::StopWatch stop_watch;
  stop_watch.stop();
  VLOG(0) << to_string() << " done in " << stop_watch.elapsed_us() << "us.";
  return kRetOk;
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
    // max_intermediates_(get_writer()->get_intermediate_size()),
    page_base_(reinterpret_cast<MasstreePage*>(get_writer()->get_page_base())),
    // intermediate_base_(reinterpret_cast<MasstreePage*>(get_writer()->get_intermediate_base())) {
    original_base_(reinterpret_cast<MasstreePage*>(args.work_memory_->get_block())),
    inputs_(reinterpret_cast<MasstreeStreamStatus*>(original_base_ + (kMaxLevels + 1U))) {
  page_id_base_ = get_writer()->get_next_page_id();
  root_page_id_ = storage_.get_masstree_metadata()->root_snapshot_page_id_;
  allocated_pages_ = 0;
  ended_inputs_count_ = 0;
  // allocated_intermediates_ = 0;
  cur_path_levels_ = 0;
  cur_path_layers_ = 0;
  std::memset(cur_path_, 0, sizeof(cur_path_));
  std::memset(cur_prefix_slices_, 0, sizeof(cur_prefix_slices_));
  std::memset(cur_prefix_be_, 0, sizeof(cur_prefix_be_));
}

ErrorStack MasstreeComposeContext::assure_work_memory_size(const Composer::ComposeArguments& args) {
  uint64_t required_size = sizeof(MasstreePage) * (kMaxLevels + 1U);  // original pages
  required_size += sizeof(MasstreeStreamStatus) * args.log_streams_count_;
  WRAP_ERROR_CODE(args.work_memory_->assure_capacity(required_size));
  return kRetOk;
}

void MasstreeComposeContext::write_dummy_page_zero() {
  ASSERT_ND(allocated_pages_ == 0);
  MasstreePage* dummy_page = page_base_;
  std::memset(dummy_page, 0xDA, sizeof(MasstreePage));
  dummy_page->header().page_id_ = get_writer()->get_next_page_id();
  dummy_page->header().storage_id_ = id_;
  dummy_page->header().page_type_ = kMasstreeBorderPageType;
  dummy_page->header().snapshot_ = true;
  allocated_pages_ = 1;
}

inline MasstreePage* MasstreeComposeContext::get_page(memory::PagePoolOffset offset) const {
  ASSERT_ND(offset > 0);
  ASSERT_ND(offset < allocated_pages_);
  ASSERT_ND(offset < max_pages_);
  return page_base_ + offset;
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
  return original_base_ + offset;
}


ErrorStack MasstreeComposeContext::execute() {
  write_dummy_page_zero();
  CHECK_ERROR(init_inputs());

  uint64_t flush_threshold = max_pages_ * 8ULL / 10ULL;
  while (ended_inputs_count_ < args_.log_streams_count_) {
    read_inputs();
    const MasstreeCommonLogType* entry = next_entry_;
    const char* key = entry->get_key();
    uint16_t key_length = entry->key_length_;
    if (UNLIKELY(does_need_to_adjust_path(key, key_length))) {
      CHECK_ERROR(adjust_path(key, key_length));
    }

    PathLevel* last = get_last_level();
    uint16_t index = last->tail_cur_index_;
    MasstreeBorderPage* page = as_border(get_page(last->tail_));
    ASSERT_ND(index + 1U == page->get_key_count());
    char* record = page->get_record(index);
    if (entry->header_.get_type() == log::kLogCodeMasstreeOverwrite) {
      ASSERT_ND(!page->does_point_to_layer(index));
      ASSERT_ND(page->compare_key(index, key, key_length));
      // [Overwrite] simply reuse log.apply
      const MasstreeOverwriteLogType* casted
        = reinterpret_cast<const MasstreeOverwriteLogType*>(entry);
      casted->apply_record(nullptr, id_, page->get_owner_id(index), record);
    } else if (entry->header_.get_type() == log::kLogCodeMasstreeDelete) {
      ASSERT_ND(!page->does_point_to_layer(index));
      ASSERT_ND(page->compare_key(index, key, key_length));
      // [Delete] Physically deletes the last record in this page.
      page->set_key_count(page->get_key_count() - 1U);
    } else {
      ASSERT_ND(entry->header_.get_type() == log::kLogCodeMasstreeInsert);
      // [Insert] a bit complex because of next_layer
      KeySlice slice = slice_layer(key, key_length, last->layer_);
      uint16_t skip = last->layer_ * kSliceLen;
      append_border(
        slice,
        entry->header_.xct_id_,
        key_length - skip,
        key + skip,
        entry->payload_count_,
        entry->get_payload(),
        last);
    }
    WRAP_ERROR_CODE(advance());

    if (UNLIKELY(allocated_pages_ >= flush_threshold)) {
      CHECK_ERROR(flush_buffer());
    }
  }

  CHECK_ERROR(finalize());
  return kRetOk;
}

ErrorStack MasstreeComposeContext::finalize() {
  CHECK_ERROR(flush_buffer());
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

ErrorCode MasstreeComposeContext::read_original_page(
  SnapshotPagePointer page_id,
  uint16_t path_level) {
  ASSERT_ND(page_id != 0);
  MasstreePage* original = get_original(path_level);
  CHECK_ERROR_CODE(args_.previous_snapshot_files_->read_page(page_id, original));
  PathLevel* level = cur_path_ + path_level;
  if (original->is_border()) {
    level->remaining_original_ = original->get_key_count();
  } else {
    // if it's an intermediate page, we convert the page image to something easier to handle.
    char buffer[sizeof(ConvertedIntermediatePage)];
    ConvertedIntermediatePage* converted = reinterpret_cast<ConvertedIntermediatePage*>(buffer);
    ASSERT_ND(sizeof(ConvertedIntermediatePage) <= kPageSize);
    // copy the header part.
    std::memcpy(converted, original, sizeof(MasstreePage));

    // copy pointers/fences
    MasstreeIntermediatePage* casted = reinterpret_cast<MasstreeIntermediatePage*>(original);
    converted->pointer_count_ = 0;
    for (uint16_t i = 0; i <= casted->get_key_count(); ++i) {
      MasstreeIntermediatePage::MiniPage& minipage = casted->get_minipage(i);
      for (uint16_t j = 0; j <= minipage.key_count_; ++j) {
        ASSERT_ND(minipage.pointers_[j].volatile_pointer_.is_null());
        FenceAndPointer* value = converted->pointers_ + converted->pointer_count_;
        value->pointer_ = minipage.pointers_[j].snapshot_pointer_;
        ASSERT_ND(value->pointer_ != 0);
        if (j < minipage.key_count_) {
          value->high_fence_ = minipage.separators_[j];
        } else if (i < casted->get_key_count()) {
          value->high_fence_ = casted->get_separator(i);
        } else {
          value->high_fence_ = casted->get_high_fence();
        }
        ++converted->pointer_count_;
      }
    }

    ASSERT_ND(converted->pointers_[converted->pointer_count_ - 1U].high_fence_
      == converted->get_high_fence());
    level->remaining_original_ = converted->pointer_count_;

    // copy back the converted image.
#ifndef NDEBUG
    std::memset(original, 0, kPageSize);
#endif  // NDEBUG
    std::memcpy(original, converted, sizeof(ConvertedIntermediatePage));
  }
  return kErrorCodeOk;
}
ErrorStack MasstreeComposeContext::init_first_level(const char* key, uint16_t key_length) {
  ASSERT_ND(cur_path_levels_ == 0);
  PathLevel* first = cur_path_;

  ASSERT_ND(allocated_pages_ == 1U);  // 0 is dummy page
  first->head_ = allocate_page();
  ASSERT_ND(allocated_pages_ == 2U);
  first->tail_ = first->head_;
  first->level_ = 0;

  bool root_border = true;
  first->remaining_original_ = 0;
  if (root_page_id_ != 0)  {
    WRAP_ERROR_CODE(read_original_page(root_page_id_, 0));
    MasstreePage* previous_root = get_original(0);
    ASSERT_ND(previous_root->get_layer() == 0);
    ASSERT_ND(previous_root->get_low_fence() == kInfimumSlice);
    ASSERT_ND(previous_root->get_high_fence() == kSupremumSlice);
    if (!previous_root->is_border()) {
      root_border = false;
    }
  }

  MasstreePage* page = get_page(first->head_);
  SnapshotPagePointer new_page_id = page_id_base_ + first->head_;
  if (root_border) {
    MasstreeBorderPage* casted = reinterpret_cast<MasstreeBorderPage*>(page);
    casted->initialize_snapshot_page(id_, new_page_id, 0, kInfimumSlice, kSupremumSlice);
  } else {
    MasstreeIntermediatePage* casted = reinterpret_cast<MasstreeIntermediatePage*>(page);
    casted->initialize_snapshot_page(id_, new_page_id, 0, kInfimumSlice, kSupremumSlice);
  }

  first->layer_ = 0;
  first->page_count_ = 1;
  first->low_fence_ = kInfimumSlice;
  first->high_fence_ = kSupremumSlice;
  ++cur_path_levels_;
  return kRetOk;
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
  }

  // Does the slices of the next_key match the current path? if not we have to close them
  if (cur_path_layers_ > 0) {
    uint16_t max_key_layer = (key_length - 1U) / kSliceLen;
    if (max_key_layer < cur_path_layers_) {
      return true;  // definitely not in this layer
    }
    uint16_t common_layers = count_common_slices(cur_prefix_be_, key, cur_path_layers_);
    if (common_layers < cur_path_layers_) {
      return true;  // prefix doesn't match. we have to close the layer(s)
    }
  }

  // Does the in-layer slice of the key match the current page? if not we have to close them
  // key is 8-bytes padded, so we can simply use normalize_be_bytes_full
  uint16_t skip = cur_path_layers_ * kSliceLen;
  KeySlice next_slice = normalize_be_bytes_full(key + skip);
  return !get_last_level()->contains_slice(next_slice);
}

ErrorStack MasstreeComposeContext::adjust_path(const char* key, uint16_t key_length) {
  if (cur_path_levels_ == 0) {
    CHECK_ERROR(init_first_level(key, key_length));
  }
  ASSERT_ND(cur_path_levels_ > 0);

  // Close layers until prefix slice matches
  uint16_t max_key_layer = (key_length - 1U) / kSliceLen;
  uint16_t cmp_layer = std::min<uint16_t>(cur_path_layers_, max_key_layer);
  uint16_t common_layers = count_common_slices(cur_prefix_be_, key, cmp_layer);
  if (common_layers < cur_path_layers_) {
    CHECK_ERROR(close_path_layer(common_layers));
    ASSERT_ND(common_layers == cur_path_layers_);
  }

  // Close levels upto fence matches
  uint16_t skip = cur_path_layers_ * kSliceLen;
  KeySlice next_slice = normalize_be_bytes_full(key + skip);
  while (!get_last_level()->contains_slice(next_slice)) {
    // the next key doesn't fit in the current path. need to close.
    CHECK_ERROR(close_last_level());
  }
  ASSERT_ND(get_last_level()->contains_slice(next_slice));
  ASSERT_ND(cur_path_layers_ * kSliceLen == skip);  // this shouldn't affect layer.

  // Does the current page has deeper pages to contain next_key?
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
          || MasstreeCommonLogType::compare_key_and_xct_id(entry, next_entry_) < 0) {
          next_input_ = i;
          next_entry_ = inputs_[i].get_entry();
        }
      }
    }
  }
  ASSERT_ND(next_input_ < args_.log_streams_count_);
  ASSERT_ND(next_entry_);
}


ErrorStack MasstreeComposeContext::close_path_layer(uint16_t max_layer) {
  DVLOG(1) << "Closing path up to layer-" << max_layer;
  ASSERT_ND(cur_path_layers_ > max_layer);
  while (true) {
    PathLevel* last = get_last_level();
    if (last->layer_ <= max_layer) {
      break;
    }
    CHECK_ERROR(close_last_level());
  }
  ASSERT_ND(cur_path_layers_ == max_layer);
  return kRetOk;
}

ErrorStack MasstreeComposeContext::close_last_level() {
  ASSERT_ND(cur_path_levels_ > 1U);  // do not use this method to close the first one.
  DVLOG(1) << "Closing the last level " << *get_last_level();
  PathLevel* last = get_last_level();
  PathLevel* parent = get_second_last_level();

  // Closing this level means we push up the last level's chain to previous.
  ASSERT_ND(parent->layer_ <= last->layer_);
  bool last_is_root = (parent->layer_ != last->layer_);
  if (!last_is_root) {
    // 1) last is not layer-root, then we append child pointers to parent
    MasstreeIntermediatePage* parent_tail = as_intermdiate(get_page(parent->tail_));
  } else {
    // 2) last is layer-root, then we add another level (intermediate page) as a new root
    SnapshotPagePointer new_pointer;
    CHECK_ERROR(grow_layer_root(&new_pointer));
  }

  ASSERT_ND(cur_path_levels_ >= 1U);
  return kRetOk;
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
  uint16_t new_index = target->get_key_count();
  if (UNLIKELY(!target->can_accomodate(new_index, remaining_length, payload_count))) {
    append_border_newpage(slice, level);
    MasstreeBorderPage* new_target = as_border(get_page(level->tail_));
    ASSERT_ND(target != new_target);
    target = new_target;
    new_index = 0;
  }

  target->reserve_record_space(new_index, xct_id, slice, suffix, remaining_length, payload_count);
  if (payload_count > 0) {
    std::memcpy(target->get_record_payload(new_index), payload, payload_count);
  }
}

inline void MasstreeComposeContext::append_border_next_layer(
  KeySlice slice,
  xct::XctId xct_id,
  SnapshotPagePointer pointer,
  PathLevel* level) {
  MasstreeBorderPage* target = as_border(get_page(level->tail_));
  uint16_t new_index = target->get_key_count();
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
        if (payload_count > 0) {
          std::memcpy(
            new_target->get_record_payload(new_index),
            target->get_record_payload(old_index),
            payload_count);
        }
      }
    }

    target->header().set_key_count(migrate_from);
    ASSERT_ND(new_target->get_key_count() == key_count - migrate_from);
  }
}

ErrorStack MasstreeComposeContext::consume_original_all() {
  PathLevel* last = get_last_level();
  if (last->remaining_original_ == 0) {
    return kRetOk;  // nothing to consume
  }

  MasstreePage* tail = get_page(last->tail_);
  if (tail->is_border()) {
    MasstreeBorderPage* original = as_border(get_original(last->level_));
    while (last->remaining_original_ > 0) {
      uint16_t index = original->get_key_count() - last->remaining_original_;
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
      --last->remaining_original_;
    }
  } else {
    ConvertedIntermediatePage* original = get_original_converted(last->level_);
    MasstreeIntermediatePage* tail_casted = as_intermdiate(tail);
    while (last->remaining_original_ > 0) {
      const FenceAndPointer& value = original->get(*last);
    }
  }
  return kRetOk;
}

ErrorStack MasstreeComposeContext::grow_layer_root(SnapshotPagePointer* root_pointer) {
  // the last level is layer-root, which we are growing.
  PathLevel* last = get_last_level();
  ASSERT_ND(last->low_fence_ == kInfimumSlice);
  ASSERT_ND(last->high_fence_ == kSupremumSlice);
  ASSERT_ND(last->page_count_ > 1U);

  CHECK_ERROR(consume_original_all());
  ASSERT_ND(last->remaining_original_ == 0);

  std::vector<FenceAndPointer> children;
  children.reserve(last->page_count_);
  for (memory::PagePoolOffset cur = last->head_; cur != 0;) {
    MasstreePage* page = get_page(cur);
    ASSERT_ND(page->get_foster_minor().is_null());
    FenceAndPointer child;
    child.pointer_ = page_id_base_ + cur;
    child.high_fence_ = page->get_high_fence();
    children.emplace_back(child);
    cur = page->get_foster_major().components.offset;
    page->set_foster_major_offset_unsafe(0);  // no longer needed
  }
  ASSERT_ND(children.size() == last->page_count_);
  ASSERT_ND(children.back().high_fence_ == kSupremumSlice);

  // The level is now nullified
  last->head_ = 0;
  last->tail_ = 0;

  // Now, replace the last level with a newly created level.
  uint32_t parent_count = assorted::int_div_ceil(last->page_count_, kMaxIntermediatePointers);
  memory::PagePoolOffset new_parent_offset = allocate_page();
  SnapshotPagePointer new_parent_page_id = page_id_base_ + new_parent_offset;
  MasstreeIntermediatePage* new_parent
    = reinterpret_cast<MasstreeIntermediatePage*>(get_page(new_parent_offset));

  last->page_count_ = parent_count;
  return kRetOk;
}

ErrorStack MasstreeComposeContext::close_first_level() {
  ASSERT_ND(cur_path_levels_ == 1U);
  ASSERT_ND(cur_path_layers_ == 0U);
  PathLevel* first = cur_path_;
  ASSERT_ND(first->low_fence_ == kInfimumSlice);
  ASSERT_ND(first->high_fence_ == kSupremumSlice);

  // Do we have to make another level?
  if (first->page_count_ > 1U) {
    CHECK_ERROR(grow_layer_root(&root_page_id_));
  }

  cur_path_levels_ = 0;
  return kRetOk;
}

ErrorStack MasstreeComposeContext::flush_buffer() {
  LOG(INFO) << "Flushing buffer. buffered pages=" << allocated_pages_;
  // 1) Close all levels. Otherwise we might still have many 'active' pages.
  while (cur_path_levels_ > 1U) {
    CHECK_ERROR(close_last_level());
  }
  CHECK_ERROR(close_first_level());
  ASSERT_ND(root_page_id_ != 0);
  ASSERT_ND(extract_numa_node_from_snapshot_pointer(root_page_id_) == numa_node_);
  ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(root_page_id_)
    == get_writer()->get_snapshot_id());

  LOG(INFO) << "Closed all levels. now buffered pages=" << allocated_pages_;
  ASSERT_ND(allocated_pages_ <= max_pages_);

  WRAP_ERROR_CODE(get_writer()->dump_pages(0, allocated_pages_));
  allocated_pages_ = 0;
  page_id_base_ = get_writer()->get_next_page_id();
  write_dummy_page_zero();
  return kRetOk;
}

std::ostream& operator<<(std::ostream& o, const MasstreeComposeContext::PathLevel& v) {
  o << "<PathLevel>"
    << "<level>" << static_cast<int>(v.level_) << "</level>"
    << "<layer_>" << static_cast<int>(v.layer_) << "</layer_>"
    << "<remaining_original_>" << v.remaining_original_ << "</remaining_original_>"
    << "<head>" << v.head_ << "</head>"
    << "<tail_>" << v.tail_ << "</tail_>"
    << "<page_count_>" << v.page_count_ << "</page_count_>"
    << "<low_fence_>" << assorted::Hex(v.low_fence_, 16) << "</low_fence_>"
    << "<high_fence_>" << assorted::Hex(v.high_fence_, 16) << "</high_fence_>"
    << "</PathLevel>";
  return o;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
