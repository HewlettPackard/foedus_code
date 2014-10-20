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
    max_pages_(get_writer()->get_page_size()),
    // max_intermediates_(get_writer()->get_intermediate_size()),
    page_base_(reinterpret_cast<MasstreePage*>(get_writer()->get_page_base())),
    // intermediate_base_(reinterpret_cast<MasstreePage*>(get_writer()->get_intermediate_base())) {
    original_base_(reinterpret_cast<MasstreePage*>(args.work_memory_->get_block())),
    inputs_(reinterpret_cast<MasstreeStreamStatus*>(original_base_ + (kMaxLevels + 1U))) {
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
inline MasstreePage* MasstreeComposeContext::get_original(memory::PagePoolOffset offset) const {
  ASSERT_ND(offset < kMaxLevels);
  ASSERT_ND(offset < cur_path_levels_);
  return original_base_ + offset;
}


ErrorStack MasstreeComposeContext::execute() {
  write_dummy_page_zero();
  CHECK_ERROR(init_inputs());

  while (ended_inputs_count_ < args_.log_streams_count_) {
    const MasstreeCommonLogType* entry = next_entry_;

    if (entry->header_.get_type() == log::kLogCodeMasstreeOverwrite) {
      const MasstreeOverwriteLogType* casted
        = reinterpret_cast<const MasstreeOverwriteLogType*>(entry);
      // casted->apply_record(nullptr, storage_id_, &record->owner_id_, record->payload_);
    } else if (entry->header_.get_type() == log::kLogCodeMasstreeDelete) {
      const MasstreeDeleteLogType* casted = reinterpret_cast<const MasstreeDeleteLogType*>(entry);
    } else {
      ASSERT_ND(entry->header_.get_type() == log::kLogCodeMasstreeInsert);
      const MasstreeInsertLogType* casted = reinterpret_cast<const MasstreeInsertLogType*>(entry);
    }
    WRAP_ERROR_CODE(advance());
  }

  CHECK_ERROR(finalize());
  return kRetOk;
}

ErrorStack MasstreeComposeContext::finalize() {
  return kRetOk;
}

ErrorStack MasstreeComposeContext::init_inputs() {
  for (uint32_t i = 0; i < args_.log_streams_count_; ++i) {
    inputs_[i].init(args_.log_streams_[i]);
    if (inputs_[i].ended_) {
      ++ended_inputs_count_;
    }
  }
  if (ended_inputs_count_ >= args_.log_streams_count_) {
    return kRetOk;
  }
  WRAP_ERROR_CODE(read_inputs());
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
      return kErrorCodeOk;
    }
  }

  return read_inputs();
}

inline ErrorCode MasstreeComposeContext::read_inputs() {
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

  const char* next_key = next_entry_->get_key();
  const uint16_t next_key_length = next_entry_->key_length_;
  const uint16_t cur_prefix_length = get_cur_prefix_length();
  if (next_key_length >= cur_prefix_length) {
    int prefix_cmp = std::memcmp(cur_prefix_be_, next_key, cur_prefix_length);
    ASSERT_ND(prefix_cmp >= 0);  // inputs are sorted
    if (prefix_cmp == 0) {
      // prefix matched!
    }
  }
  return kErrorCodeOk;
}


}  // namespace masstree
}  // namespace storage
}  // namespace foedus
