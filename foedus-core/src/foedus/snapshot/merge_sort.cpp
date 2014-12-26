/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/snapshot/merge_sort.hpp"

#include <glog/logging.h>

#include "foedus/epoch.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/log_buffer.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"

namespace foedus {
namespace snapshot {

bool is_array_log_type(uint16_t log_type) {
  return log_type == log::kLogCodeArrayOverwrite || log_type == log::kLogCodeArrayIncrement;
}
bool is_masstree_log_type(uint16_t log_type) {
  return
    log_type == log::kLogCodeMasstreeInsert
    || log_type == log::kLogCodeMasstreeOverwrite
    || log_type == log::kLogCodeMasstreeOverwrite;
}

MergeSort::MergeSort(
  storage::StorageId id,
  storage::StorageType type,
  Epoch base_epoch,
  uint16_t shortest_key_length,
  uint16_t longest_key_length,
  SortedBuffer* const* inputs,
  uint16_t inputs_count,
  memory::AlignedMemory* const work_memory)
  : DefaultInitializable(),
    id_(id),
    type_(type),
    base_epoch_(base_epoch),
    shortest_key_length_(shortest_key_length),
    longest_key_length_(longest_key_length),
    inputs_(inputs),
    inputs_count_(inputs_count),
    work_memory_(work_memory) {
  current_count_ = 0;
  sort_entries_ = nullptr;
  position_entries_ = nullptr;
  original_pages_ = nullptr;
  inputs_status_ = nullptr;
}

ErrorStack MergeSort::initialize_once() {
  // in each batch, we might include tuples from an input even if we didn't fully pick a chunk from
  // it (at most kLogChunk-1 such tuples). so, conservatively kChunkBatch + inputs_count_.
  uint32_t buffer_capacity = kLogChunk * (kChunkBatch + inputs_count_);
  buffer_capacity_ = assorted::align<uint32_t, 512U>(buffer_capacity);
  uint64_t byte_size = buffer_capacity_ * (sizeof(SortEntry) + sizeof(PositionEntry));
  ASSERT_ND(byte_size % 4096U == 0);
  byte_size += storage::kPageSize * (kMaxLevels + 1U);
  byte_size += sizeof(InputStatus) * inputs_count_;
  WRAP_ERROR_CODE(work_memory_->assure_capacity(byte_size));

  // assign pointers
  char* block = reinterpret_cast<char*>(work_memory_->get_block());
#ifndef NDEBUG
  std::memset(block, 0xDA, work_memory_->get_size());
#endif  // NDEBUG
  uint64_t offset = 0;
  sort_entries_ = reinterpret_cast<SortEntry*>(block + offset);
  offset += sizeof(SortEntry) * buffer_capacity;
  position_entries_ = reinterpret_cast<PositionEntry*>(block + offset);
  offset += sizeof(PositionEntry) * buffer_capacity;
  original_pages_ = reinterpret_cast<storage::Page*>(block + offset);
  offset += sizeof(storage::Page) * (kMaxLevels + 1U);
  inputs_status_ = reinterpret_cast<InputStatus*>(block + offset);
  offset += sizeof(InputStatus) * inputs_count_;
  ASSERT_ND(offset == byte_size);

  // initialize inputs_status_
  for (InputIndex i = 0; i < inputs_count_; ++i) {
    InputStatus* status = inputs_status_ + i;
    SortedBuffer* input = inputs_[i];
    input->assert_checks();
    status->window_ = input->get_buffer();
    status->window_size_ = input->get_buffer_size();
    uint64_t cur_abs = input->get_cur_block_abosulte_begin();
    // this is the initial read of this block, so we are sure cur_block_abosulte_begin is the window
    ASSERT_ND(cur_abs >= input->get_offset());
    status->cur_relative_pos_ = cur_abs - input->get_offset();
    status->chunk_relative_pos_ = status->cur_relative_pos_;  // hence the chunk has only 1 log
    status->previous_chunk_relative_pos_ = status->chunk_relative_pos_;

    uint64_t end_abs = input->get_cur_block_abosulte_end();
    status->end_absolute_pos_ = end_abs;
    status->ended_ = (cur_abs >= end_abs);

    if (status->ended_) {
      status->last_chunk_ = true;
    } else {
      uint64_t pos = status->get_cur_log()->header_.log_length_ + status->cur_relative_pos_;
      ASSERT_ND(pos + input->get_offset() <= end_abs);
      status->last_chunk_ = pos + input->get_offset() >= end_abs;
    }
    status->assert_consistent();
  }
  return kRetOk;
}

bool MergeSort::next_chunk(InputIndex input_index) {
  InputStatus* status = inputs_status_ + input_index;
  ASSERT_ND(!status->ended_);
  ASSERT_ND(!status->last_chunk_);
  status->assert_consistent();
  uint64_t pos = status->chunk_relative_pos_;

  bool had_any = false;
  for (uint32_t i = 0; i < kLogChunk; ++i) {
    ASSERT_ND(pos < status->window_size_);
    const log::RecordLogType* the_log = status->from_byte_pos(pos);
    uint16_t log_length = the_log->header_.log_length_;
    if (pos + log_length >= status->window_size_) {
      break;
    }
    had_any = true;
  }
  status->previous_chunk_relative_pos_ = status->chunk_relative_pos_;
  status->chunk_relative_pos_ = pos;

  uint64_t chunk_end = status->get_chunk_log()->header_.log_length_ + pos;
  ASSERT_ND(chunk_end + inputs_[input_index]->get_offset() <= status->end_absolute_pos_);
  if (chunk_end + inputs_[input_index]->get_offset() >= status->end_absolute_pos_) {
    status->last_chunk_ = true;
  }

  status->assert_consistent();
  return had_any;
}

MergeSort::InputIndex MergeSort::determine_min_input() const {
  InputIndex min_input = kInvalidInput;
  for (InputIndex i = 0; i < inputs_count_; ++i) {
    InputStatus* status = inputs_status_ + i;
    if (status->ended_  || status->last_chunk_) {
      continue;
    }
    if (min_input == kInvalidInput) {
      min_input = i;
    } else {
      ASSERT_ND(!inputs_status_[min_input].ended_);
      ASSERT_ND(!inputs_status_[min_input].last_chunk_);
      if (compare_logs(status->get_chunk_log(), inputs_status_[min_input].get_chunk_log()) < 0) {
        min_input = i;
      }
    }
  }
  return min_input;
}

MergeSort::InputIndex MergeSort::pick_chunks() {
  uint32_t chunks;
  for (chunks = 0; chunks < kChunkBatch; ++chunks) {
    InputIndex min_input = determine_min_input();
    if (min_input == kInvalidInput) {
      // now all inputs are in the last chunks, we can simply merge them all in one shot!
      return kInvalidInput;
    }

    bool retrieved = next_chunk(min_input);
    if (!retrieved) {
      VLOG(0) << "Input-" << min_input << " needs to shift window. chunks=" << chunks;
      break;
    }
  }

  VLOG(0) << "Now determining batch-threshold... chunks=" << chunks;
  return determine_min_input();
}

void MergeSort::batch_sort(MergeSort::InputIndex min_input) {
  batch_sort_prepare(min_input);
  ASSERT_ND(current_count_ <= buffer_capacity_);

  assert_sorted();
}

void MergeSort::batch_sort_prepare(MergeSort::InputIndex min_input) {
  current_count_ = 0;
  if (min_input == kInvalidInput) {
    // this is the last iteration! get all remaining logs from all inputs
    for (InputIndex i = 0; i < inputs_count_; ++i) {
      InputStatus* status = inputs_status_ + i;
      ASSERT_ND(status->last_chunk_);
      if (status->ended_) {
        continue;
      }
      append_logs(i, status->chunk_relative_pos_ + status->get_chunk_log()->header_.log_length_);
      status->ended_ = true;
      status->assert_consistent();
    }
  } else {
    // merge-sort upto batch-threshold
    const log::RecordLogType* threshold = inputs_status_[min_input].get_chunk_log();
    for (InputIndex i = 0; i < inputs_count_; ++i) {
      InputStatus* status = inputs_status_ + i;
      if (status->ended_) {
        continue;
      }

      if (i == min_input) {
        // the input that provides threshold itself. Hence, all logs before the last log are
        // guaranteed to be strictly smaller than the threshold.
        append_logs(i, status->chunk_relative_pos_);
      } else {
        // otherwise, we have to add logs that are smaller than the threshold.
        // to avoid key comparison in most cases, we use "previous chunk" hint.
        if (status->previous_chunk_relative_pos_ != status->chunk_relative_pos_) {
          append_logs(i, status->previous_chunk_relative_pos_);
          ASSERT_ND(status->previous_chunk_relative_pos_ == status->chunk_relative_pos_);
        }

        // and then we have to check one by one. we could do binary search here, but >90%
        // of logs should be already appended by the previous-chunk optimization. not worth it.
        uint64_t cur = status->cur_relative_pos_;
        uint64_t end = status->chunk_relative_pos_ + status->get_chunk_log()->header_.log_length_;
        ASSERT_ND(cur < end);
        while (cur < end) {
          const log::RecordLogType* the_log = status->from_byte_pos(cur);
          // It must be _strictly_ smaller than the threshold
          if (compare_logs(the_log, threshold) >= 0) {
            break;
          }
          cur += the_log->header_.log_length_;
        }
        ASSERT_ND(cur <= end);
        append_logs(i, cur);
        if (cur == end) {
          // this means we added even the last log. This can happen only at the last chunk
          ASSERT_ND(status->last_chunk_);
          status->ended_ = true;
        }
      }

      status->assert_consistent();
    }
  }
}



template <typename T>
int compare_logs_as(const log::RecordLogType* lhs, const log::RecordLogType* rhs) {
  const T* lhs_log = reinterpret_cast<const T*>(lhs);
  const T* rhs_log = reinterpret_cast<const T*>(rhs);
  return T::compare_logs(lhs_log, rhs_log);
}

int MergeSort::compare_logs(const log::RecordLogType* lhs, const log::RecordLogType* rhs) const {
  ASSERT_ND(lhs->header_.storage_id_ == id_);
  ASSERT_ND(rhs->header_.storage_id_ == id_);
  if (type_ == storage::kArrayStorage) {
    ASSERT_ND(is_array_log_type(lhs->header_.log_type_code_));
    ASSERT_ND(is_array_log_type(rhs->header_.log_type_code_));
    return compare_logs_as< storage::array::ArrayCommonUpdateLogType >(lhs, rhs);
  } else {
    ASSERT_ND(type_ == storage::kMasstreeStorage);
    ASSERT_ND(is_masstree_log_type(lhs->header_.log_type_code_));
    ASSERT_ND(is_masstree_log_type(rhs->header_.log_type_code_));
    return compare_logs_as< storage::masstree::MasstreeCommonLogType >(lhs, rhs);
  }
}

void MergeSort::append_logs(MergeSort::InputIndex input_index, uint64_t upto_relative_pos) {
  if (type_ == storage::kArrayStorage) {
    append_logs_array(input_index, upto_relative_pos);
  } else {
    ASSERT_ND(type_ == storage::kMasstreeStorage);
    append_logs_masstree(input_index, upto_relative_pos);
  }
  InputStatus* status = inputs_status_ + input_index;
  status->cur_relative_pos_ = upto_relative_pos;
  if (upto_relative_pos > status->chunk_relative_pos_) {
    // we appeneded even the last log of this chunk! this should happen only at the last chunk.
    ASSERT_ND(status->last_chunk_);
    status->chunk_relative_pos_ = upto_relative_pos;
    status->previous_chunk_relative_pos_ = upto_relative_pos;
  }
  status->assert_consistent();
}

void MergeSort::append_logs_array(InputIndex input_index, uint64_t upto_relative_pos) {
  InputStatus* status = inputs_status_ + input_index;
  uint64_t relative_pos = status->cur_relative_pos_;
  while (relative_pos < upto_relative_pos) {
    ASSERT_ND(relative_pos < status->window_size_);
    ASSERT_ND(relative_pos % 8U == 0);
    const storage::array::ArrayCommonUpdateLogType* the_log
      = reinterpret_cast<const storage::array::ArrayCommonUpdateLogType*>(
          status->window_ + relative_pos);
    ASSERT_ND(is_array_log_type(the_log->header_.log_type_code_));
    the_log->assert_valid_generic();

    Epoch epoch = the_log->header_.xct_id_.get_epoch();
    ASSERT_ND(epoch.subtract(base_epoch_) < (1U << 16));
    uint16_t compressed_epoch = epoch.subtract(base_epoch_);
    sort_entries_[current_count_].set(
      the_log->offset_,
      compressed_epoch,
      the_log->header_.xct_id_.get_ordinal(),
      false,
      current_count_);
    position_entries_[current_count_].input_index_ = input_index;
    position_entries_[current_count_].input_position_ = to_buffer_position(relative_pos);
    ++current_count_;

    relative_pos += the_log->header_.log_length_;
  }
  ASSERT_ND(relative_pos == upto_relative_pos);
}

void MergeSort::append_logs_masstree(InputIndex input_index, uint64_t upto_relative_pos) {
  InputStatus* status = inputs_status_ + input_index;
  uint64_t relative_pos = status->cur_relative_pos_;
  while (relative_pos < upto_relative_pos) {
    ASSERT_ND(relative_pos < status->window_size_);
    ASSERT_ND(relative_pos % 8U == 0);
    const storage::masstree::MasstreeCommonLogType* the_log
      = reinterpret_cast<const storage::masstree::MasstreeCommonLogType*>(
          status->window_ + relative_pos);
    ASSERT_ND(is_masstree_log_type(the_log->header_.log_type_code_));
    the_log->assert_valid_generic();

    Epoch epoch = the_log->header_.xct_id_.get_epoch();
    ASSERT_ND(epoch.subtract(base_epoch_) < (1U << 16));
    uint16_t compressed_epoch = epoch.subtract(base_epoch_);
    uint16_t key_length = the_log->key_length_;
    ASSERT_ND(key_length >= shortest_key_length_);
    ASSERT_ND(key_length <= longest_key_length_);
    sort_entries_[current_count_].set(
      the_log->get_first_slice(),
      compressed_epoch,
      the_log->header_.xct_id_.get_ordinal(),
      key_length != sizeof(storage::masstree::KeySlice),
      current_count_);
    position_entries_[current_count_].input_index_ = input_index;
    position_entries_[current_count_].key_length_ = key_length;
    position_entries_[current_count_].input_position_ = to_buffer_position(relative_pos);
    ++current_count_;

    relative_pos += the_log->header_.log_length_;
  }
  ASSERT_ND(relative_pos == upto_relative_pos);
}


void MergeSort::assert_sorted() {
#ifndef NDEBUG
  for (MergedPosition i = 0; i < current_count_; ++i) {
    MergedPosition cur_pos = sort_entries_[i].get_position();
    const log::RecordLogType* cur = inputs_status_[position_entries_[cur_pos].input_index_].
      from_compact_pos(position_entries_[cur_pos].input_position_);

    // does it point to a correct log?
    Epoch epoch = cur->header_.xct_id_.get_epoch();
    uint16_t compressed_epoch = epoch.subtract(base_epoch_);
    SortEntry dummy;
    if (type_ == storage::kArrayStorage) {
      const auto* casted = reinterpret_cast<const storage::array::ArrayCommonUpdateLogType*>(cur);
      dummy.set(
        casted->offset_,
        compressed_epoch,
        cur->header_.xct_id_.get_ordinal(),
        false,
        cur_pos);
    } else {
      const auto* casted = reinterpret_cast<const storage::masstree::MasstreeCommonLogType*>(cur);
      dummy.set(
        casted->get_first_slice(),
        compressed_epoch,
        cur->header_.xct_id_.get_ordinal(),
        casted->key_length_ != sizeof(storage::masstree::KeySlice),
        cur_pos);
    }
    ASSERT_ND(dummy.data_ == sort_entries_[i].data_);
    if (i == 0) {
      continue;
    }

    // compare with previous
    MergedPosition prev_pos = sort_entries_[i - 1].get_position();
    ASSERT_ND(prev_pos != cur_pos);
    const log::RecordLogType* prev = inputs_status_[position_entries_[prev_pos].input_index_].
      from_compact_pos(position_entries_[prev_pos].input_position_);
    int cmp = compare_logs(cur, prev);
    ASSERT_ND(cmp <= 0);
    if (cmp == 0) {
      // the last of sort order is position.
      ASSERT_ND(prev_pos < cur_pos);
    }
  }
#endif  // NDEBUG
}

}  // namespace snapshot
}  // namespace foedus
