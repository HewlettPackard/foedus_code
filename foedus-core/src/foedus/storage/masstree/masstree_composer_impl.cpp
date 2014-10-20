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
    WRAP_ERROR_CODE(read_inputs());
    const MasstreeCommonLogType* entry = next_entry_;

    if (entry->header_.get_type() == log::kLogCodeMasstreeOverwrite) {
      const MasstreeOverwriteLogType* casted
        = reinterpret_cast<const MasstreeOverwriteLogType*>(entry);
      // casted->apply_record(nullptr, id_, &record->owner_id_, record->payload_);
    } else if (entry->header_.get_type() == log::kLogCodeMasstreeDelete) {
      const MasstreeDeleteLogType* casted = reinterpret_cast<const MasstreeDeleteLogType*>(entry);
    } else {
      ASSERT_ND(entry->header_.get_type() == log::kLogCodeMasstreeInsert);
      const MasstreeInsertLogType* casted = reinterpret_cast<const MasstreeInsertLogType*>(entry);
    }
    WRAP_ERROR_CODE(advance());

    if (UNLIKELY(allocated_pages_ >= flush_threshold)) {
      CHECK_ERROR(flush_buffer(true));
    }
  }

  CHECK_ERROR(finalize());
  return kRetOk;
}

ErrorStack MasstreeComposeContext::finalize() {
  CHECK_ERROR(flush_buffer(false));
  return kRetOk;
}

ErrorStack MasstreeComposeContext::init_inputs() {
  CHECK_ERROR(init_first_level());
  for (uint32_t i = 0; i < args_.log_streams_count_; ++i) {
    inputs_[i].init(args_.log_streams_[i]);
    if (inputs_[i].ended_) {
      ++ended_inputs_count_;
    }
  }
  return kRetOk;
}
ErrorStack MasstreeComposeContext::init_first_level() {
  ASSERT_ND(cur_path_levels_ == 0);
  PathLevel* first = cur_path_;

  ASSERT_ND(allocated_pages_ == 1U);  // 0 is dummy page
  first->head_ = allocate_page();
  ASSERT_ND(allocated_pages_ == 2U);
  first->tail_ = first->head_;
  first->level_ = 0;

  bool root_border = true;
  first->has_original_ = root_page_id_ != 0;
  if (first->has_original_)  {
    MasstreePage* previous_root = get_original(0);
    WRAP_ERROR_CODE(args_.previous_snapshot_files_->read_page(root_page_id_, previous_root));
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

inline ErrorCode MasstreeComposeContext::read_inputs() {
  ASSERT_ND(ended_inputs_count_ < args_.log_streams_count_);
  // 1) Find the log entry to apply next. so far this is a dumb sequential search. loser tree?
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

  const char* next_key = next_entry_->get_key();
  const uint16_t next_key_length = next_entry_->key_length_;

  // 2) Does the slices of the next_key match the current path? if not close them
  if (cur_path_layers_ > 0) {
    uint16_t cmp_layer = std::min<uint16_t>(cur_path_layers_, next_key_length / kSliceLen);
    ASSERT_ND(std::memcmp(cur_prefix_be_, next_key, cmp_layer * kSliceLen) <= 0);  // sorted inputs
    uint16_t common_layers = count_common_slices(cur_prefix_be_, next_key, cmp_layer);
    if (UNLIKELY(common_layers < cur_path_layers_)) {  // most cases should skip this
      CHECK_ERROR_CODE(close_path_layer(common_layers));
      ASSERT_ND(common_layers == cur_path_layers_);
    }
  }

  // 3) Does the in-layer slice  of the next_key match the current page? if not close them
  // next_key is 8-bytes padded, so we can simply use normalize_be_bytes_full
  uint16_t skip = cur_path_layers_ * kSliceLen;
  KeySlice next_slice = normalize_be_bytes_full(next_key + skip);
  while (UNLIKELY(!get_last_level()->contains_slice(next_slice))) {  // most cases should skip this
    // the next key doesn't fit in the current path. need to close.
    CHECK_ERROR_CODE(close_last_level());
  }
  ASSERT_ND(get_last_level()->contains_slice(next_slice));
  ASSERT_ND(cur_path_layers_ * kSliceLen == skip);  // this shouldn't affect layer.

  // 4) Does the current page has deeper pages to contain next_key?

  return kErrorCodeOk;
}


ErrorCode MasstreeComposeContext::close_path_layer(uint16_t max_layer) {
  DVLOG(1) << "Closing path up to layer-" << max_layer;
  ASSERT_ND(cur_path_layers_ > max_layer);
  while (true) {
    PathLevel* last = get_last_level();
    if (last->layer_ <= max_layer) {
      break;
    }
    CHECK_ERROR_CODE(close_last_level());
  }
  ASSERT_ND(cur_path_layers_ == max_layer);
  return kErrorCodeOk;
}

ErrorCode MasstreeComposeContext::close_last_level() {
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
  }

  ASSERT_ND(cur_path_levels_ >= 1U);
  return kErrorCodeOk;
}

ErrorStack MasstreeComposeContext::close_first_level() {
  ASSERT_ND(cur_path_levels_ == 1U);
  ASSERT_ND(cur_path_layers_ == 0U);
  PathLevel* first = cur_path_;
  cur_path_levels_ = 0;

  // Do we have to make another level?
  if (first->page_count_ > 1U) {
    memory::PagePoolOffset new_parent_offset = allocate_page();
    SnapshotPagePointer new_parent_page_id = page_id_base_ + new_parent_offset;
    MasstreeIntermediatePage* new_parent
      = reinterpret_cast<MasstreeIntermediatePage*>(get_page(new_parent_offset));
    struct Child {
      memory::PagePoolOffset offset_;
      KeySlice high_fence_;
    };
    std::vector<Child> children;
    children.reserve(first->page_count_);
    for (memory::PagePoolOffset cur = first->head_; cur != 0;) {
      MasstreePage* page = get_page(cur);
      ASSERT_ND(page->get_foster_minor().is_null());
      Child child;
      child.offset_ = cur;
      child.high_fence_ = page->get_high_fence();
      children.emplace_back(child);
      cur = page->get_foster_major().components.offset;
      page->get_foster_major().clear();  // no longer needed
    }
  }

  return kRetOk;
}

ErrorStack MasstreeComposeContext::flush_buffer(bool more_logs) {
  LOG(INFO) << "Flushing buffer. buffered pages=" << allocated_pages_;
  // 1) Close all levels. Otherwise we might still have many 'active' pages.
  while (cur_path_levels_ > 1U) {
    WRAP_ERROR_CODE(close_last_level());
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
  if (more_logs) {
    write_dummy_page_zero();
    CHECK_ERROR(init_first_level());
  }
  return kRetOk;
}

std::ostream& operator<<(std::ostream& o, const MasstreeComposeContext::PathLevel& v) {
  o << "<PathLevel>"
    << "<level>" << static_cast<int>(v.level_) << "</level>"
    << "<layer_>" << static_cast<int>(v.layer_) << "</layer_>"
    << "<has_original_>" << v.has_original_ << "</has_original_>"
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
