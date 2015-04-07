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
#include "foedus/snapshot/merge_sort.hpp"

#include <glog/logging.h>

#include <algorithm>

#include "foedus/epoch.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/log_buffer.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"

namespace foedus {
namespace snapshot {

uint16_t extract_shortest_key_length(SortedBuffer* const* inputs, uint16_t inputs_count) {
  uint16_t ret = inputs[0]->get_cur_block_shortest_key_length();
  for (uint16_t i = 1; i < inputs_count; ++i) {
    ret = std::min<uint16_t>(ret, inputs[i]->get_cur_block_shortest_key_length());
  }
  return ret;
}
uint16_t extract_longest_key_length(SortedBuffer* const* inputs, uint16_t inputs_count) {
  uint16_t ret = inputs[0]->get_cur_block_longest_key_length();
  for (uint16_t i = 1; i < inputs_count; ++i) {
    ret = std::max<uint16_t>(ret, inputs[i]->get_cur_block_longest_key_length());
  }
  return ret;
}

MergeSort::MergeSort(
  storage::StorageId id,
  storage::StorageType type,
  Epoch base_epoch,
  SortedBuffer* const* inputs,
  uint16_t inputs_count,
  uint16_t max_original_pages,
  memory::AlignedMemory* const work_memory,
  uint16_t chunk_batch_size)
  : DefaultInitializable(),
    id_(id),
    type_(type),
    base_epoch_(base_epoch),
    shortest_key_length_(extract_shortest_key_length(inputs, inputs_count)),
    longest_key_length_(extract_longest_key_length(inputs, inputs_count)),
    inputs_(inputs),
    inputs_count_(inputs_count),
    max_original_pages_(max_original_pages),
    chunk_batch_size_(chunk_batch_size),
    work_memory_(work_memory) {
  ASSERT_ND(shortest_key_length_ <= longest_key_length_);
  ASSERT_ND(shortest_key_length_ > 0);
  ASSERT_ND(chunk_batch_size_ > 0);
  current_count_ = 0;
  sort_entries_ = nullptr;
  position_entries_ = nullptr;
  original_pages_ = nullptr;
  inputs_status_ = nullptr;
}

ErrorStack MergeSort::initialize_once() {
  // in each batch, we might include tuples from an input even if we didn't fully pick a chunk from
  // it (at most kLogChunk-1 such tuples). so, conservatively chunk_batch_size_ + inputs_count_.
  uint32_t buffer_capacity = kLogChunk * (chunk_batch_size_ + inputs_count_);
  buffer_capacity_ = assorted::align<uint32_t, 512U>(buffer_capacity);
  uint64_t byte_size = buffer_capacity_ * (sizeof(SortEntry) + sizeof(PositionEntry));
  ASSERT_ND(byte_size % 4096U == 0);
  byte_size += storage::kPageSize * (max_original_pages_ + 1U);
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
  offset += sizeof(storage::Page) * (max_original_pages_ + 1U);
  inputs_status_ = reinterpret_cast<InputStatus*>(block + offset);
  offset += sizeof(InputStatus) * inputs_count_;
  ASSERT_ND(offset == byte_size);

  // initialize inputs_status_
  for (InputIndex i = 0; i < inputs_count_; ++i) {
    InputStatus* status = inputs_status_ + i;
    SortedBuffer* input = inputs_[i];
    input->assert_checks();
    status->window_ = input->get_buffer();
    status->window_offset_ = input->get_offset();
    status->window_size_ = input->get_buffer_size();
    uint64_t cur_abs = input->get_cur_block_abosulte_begin();
    // this is the initial read of this block, so we are sure cur_block_abosulte_begin is the window
    ASSERT_ND(cur_abs >= status->window_offset_);
    status->cur_relative_pos_ = cur_abs - status->window_offset_;
    status->chunk_relative_pos_ = status->cur_relative_pos_;  // hence the chunk has only 1 log
    status->previous_chunk_relative_pos_ = status->chunk_relative_pos_;

    uint64_t end_abs = input->get_cur_block_abosulte_end();
    status->end_absolute_pos_ = end_abs;

    status->assert_consistent();
  }
  return kRetOk;
}

ErrorStack MergeSort::next_batch() {
  ASSERT_ND(is_initialized());
  current_count_ = 0;
  CHECK_ERROR(advance_window());

  if (is_ended_all()) {
    return kRetOk;
  }

  if (is_no_merging()) {
    next_batch_one_input();
  } else {
    InputIndex min_input = pick_chunks();
    batch_sort(min_input);
  }
  return kRetOk;
}

void MergeSort::next_batch_one_input() {
  // In this case, we could even skip setting sort_entries_. However, composer benefits from the
  // concise array that tells the most significant 8 bytes key, so we populate it even in this case.
  ASSERT_ND(is_no_merging());
  // Note, even in this case, inputs_[0] might NOT be InMemorySortedBuffer.
  // Example:
  //   in-memory: blocks for storage-1, storage-3
  //   dump-0: blocks for storage-1, storage-2, storage-3
  //   dump-1: blocks for storage-3
  // For storage-2, dump-0 is the only input. Had a bug to overlook this case...
  InputStatus* status = inputs_status_;
  uint64_t relative_pos = status->cur_relative_pos_;
  uint64_t end_pos = status->end_absolute_pos_ - status->window_offset_;
  const uint32_t kLongestLog = 1U << 16;
  if (dynamic_cast<InMemorySortedBuffer*>(inputs_[0])) {
    VLOG(0) << "1-input in-memory case.";
    ASSERT_ND(status->is_last_window());  // then it's always the last window.
    ASSERT_ND(status->window_offset_ == 0);
    ASSERT_ND(!status->is_ended());
    ASSERT_ND(end_pos <= status->window_size_);
  } else {
    VLOG(0) << "1-input dump-file case.";
    ASSERT_ND(dynamic_cast<DumpFileSortedBuffer*>(inputs_[0]));
    // In this case, end_pos might be careful on a log spanning the end.
    // rather than checking the length each time, we conservatively close the current window.
    if (end_pos + kLongestLog > status->window_size_) {
      end_pos = status->window_size_ - kLongestLog;
    }
  }

  ASSERT_ND(relative_pos <= end_pos + kLongestLog);
  uint64_t processed = 0;
  debugging::StopWatch watch;
  if (type_ == storage::kArrayStorage) {
    for (; LIKELY(relative_pos < end_pos && processed < buffer_capacity_); ++processed) {
      relative_pos += populate_entry_array(0, relative_pos);
    }
  } else {
    ASSERT_ND(type_ == storage::kMasstreeStorage);
    for (; LIKELY(relative_pos < end_pos && processed < buffer_capacity_); ++processed) {
      relative_pos += populate_entry_masstree(0, relative_pos);
    }
  }
  ASSERT_ND(relative_pos <= end_pos + kLongestLog);
  ASSERT_ND(processed <= buffer_capacity_);
  ASSERT_ND(current_count_ == processed);

  watch.stop();
  VLOG(0) << "1-input case. from=" << status->cur_relative_pos_ << "b. processed " << processed
    << " logs in " << watch.elapsed_ms() << "ms";
  status->cur_relative_pos_ = relative_pos;
  status->chunk_relative_pos_ = relative_pos;
  status->previous_chunk_relative_pos_ = relative_pos;
  status->assert_consistent();
  assert_sorted();
}

ErrorStack MergeSort::advance_window() {
  // this method is called while we do not grab anything from the input yet.
  // otherwise we can't move window here.
  ASSERT_ND(current_count_ == 0);
  for (InputIndex i = 0; i < inputs_count_; ++i) {
    InputStatus* status = inputs_status_ + i;
    if (status->is_ended() || status->is_last_window()) {
      continue;
    }
    ASSERT_ND(status->cur_relative_pos_ <= status->chunk_relative_pos_);
    ASSERT_ND(status->cur_relative_pos_ == status->previous_chunk_relative_pos_);
    if (status->cur_relative_pos_
        >= static_cast<uint64_t>(status->window_size_ * kWindowMoveThreshold)
       || status->cur_relative_pos_ + kWindowChunkReserveBytes >= status->window_size_) {
      uint64_t cur_abs_pos = status->to_absolute_pos(status->cur_relative_pos_);

      SortedBuffer* input = inputs_[i];
      WRAP_ERROR_CODE(input->wind(cur_abs_pos));
      status->window_offset_ = input->get_offset();
      ASSERT_ND(status->window_size_ == input->get_buffer_size());
      ASSERT_ND(status->window_ == input->get_buffer());

      ASSERT_ND(cur_abs_pos >= status->window_offset_);
      status->cur_relative_pos_ = cur_abs_pos - status->window_offset_;
      status->chunk_relative_pos_ = status->cur_relative_pos_;
      status->previous_chunk_relative_pos_ = status->cur_relative_pos_;
      status->assert_consistent();
    }
    ASSERT_ND(status->chunk_relative_pos_ + status->get_chunk_log()->header_.log_length_
        <= status->window_size_);
  }

#ifndef NDEBUG
  // after the conservative move above, all inputs should be either
  // 1) in last window, including already ended
  // 2) in non-last window that has at least kWindowChunkReserveBytes to be consumed
  // let's confirm.
  for (InputIndex i = 0; i < inputs_count_; ++i) {
    InputStatus* status = inputs_status_ + i;
    status->assert_consistent();
    if (status->is_ended() || status->is_last_window()) {
      continue;
    }
    ASSERT_ND(status->cur_relative_pos_ + kWindowChunkReserveBytes <= status->window_size_);
    ASSERT_ND(status->chunk_relative_pos_ + status->get_chunk_log()->header_.log_length_
        <= status->window_size_);
  }
#endif  // NDEBUG
  return kRetOk;
}

uint32_t MergeSort::fetch_logs(
  uint32_t sort_pos,
  uint32_t count,
  log::RecordLogType const** out) const {
  ASSERT_ND(sort_pos <= current_count_);
  uint32_t fetched_count = count;
  if (sort_pos + count > current_count_) {
    fetched_count = current_count_ - sort_pos;
  }

  if (is_no_merging()) {
    // no merge sort.
#ifndef NDEBUG
    for (uint32_t i = 0; i < fetched_count; ++i) {
      ASSERT_ND(sort_entries_[sort_pos + i].get_position() == sort_pos + i);
    }
#endif  // NDEBUG
    // in this case, the pointed logs are also contiguous. no point to do prefetching.
    for (uint32_t i = 0; i < fetched_count; ++i) {
      MergedPosition pos = sort_pos + i;
      ASSERT_ND(position_entries_[pos].input_index_ == 0);
      out[i] = inputs_status_[0].from_compact_pos(position_entries_[pos].input_position_);
    }
    return fetched_count;
  }

  // prefetch position entries
  for (uint32_t i = 0; i < fetched_count; ++i) {
    MergedPosition pos = sort_entries_[sort_pos + i].get_position();
    assorted::prefetch_cacheline(position_entries_ + pos);
  }
  // prefetch and fetch logs
  for (uint32_t i = 0; i < fetched_count; ++i) {
    MergedPosition pos = sort_entries_[sort_pos + i].get_position();
    InputIndex input = position_entries_[pos].input_index_;
    out[i] = inputs_status_[input].from_compact_pos(position_entries_[pos].input_position_);
    assorted::prefetch_cacheline(out[i]);
  }
  return fetched_count;
}

void MergeSort::next_chunk(InputIndex input_index) {
  InputStatus* status = inputs_status_ + input_index;
  ASSERT_ND(!status->is_ended());
  ASSERT_ND(!status->is_last_chunk_in_window());
  status->assert_consistent();

  uint64_t pos = status->chunk_relative_pos_;
  uint64_t relative_end = status->end_absolute_pos_ - status->window_offset_;
  if (relative_end >= status->window_size_) {
    relative_end = status->window_size_;
  }
  ASSERT_ND(pos + status->from_byte_pos(pos)->header_.log_length_ <= status->window_size_);

  // Be careful on advancing "too much". If the next log spans to next window,
  // we can't use it as a chunk-log. But, we don't know the log length until we advance.
  // We thus maintain two values:
  //   pos : we are sure log entry at this position completely fits in the window.
  //   next_pos : we are considering to set this value to pos
  if (status->is_last_window()) {
    // separately handle last-window, where there is no concern on spanning to next window,
    // but instead we have to include the last log in last chunk.
    for (uint32_t i = 0; i < kLogChunk; ++i) {
      ASSERT_ND(pos < status->window_size_);
      const log::RecordLogType* the_log = status->from_byte_pos(pos);
      uint16_t log_length = the_log->header_.log_length_;
      ASSERT_ND(pos + log_length <= relative_end);  // because it's last window
      if (pos + log_length >= relative_end) {
        break;
      }
      pos += log_length;
    }
  } else {
    uint64_t next_pos = pos;
    for (uint32_t i = 0; i < kLogChunk; ++i) {
      ASSERT_ND(next_pos < status->window_size_);
      const log::RecordLogType* the_log = status->from_byte_pos(next_pos);
      uint16_t log_length = the_log->header_.log_length_;
      if (next_pos + log_length >= relative_end) {
        break;
      }
      pos = next_pos;
      next_pos += log_length;
    }
  }
  ASSERT_ND(pos < relative_end);
  ASSERT_ND(pos + status->from_byte_pos(pos)->header_.log_length_ <= status->window_size_);
  status->previous_chunk_relative_pos_ = status->chunk_relative_pos_;
  status->chunk_relative_pos_ = pos;

  status->assert_consistent();
  // ALWAYS, the chunk-log is fully contained in the window.
  ASSERT_ND(status->chunk_relative_pos_ + status->get_chunk_log()->header_.log_length_
      <= status->window_size_);
}

MergeSort::InputIndex MergeSort::determine_min_input() const {
  InputIndex min_input = kInvalidInput;
  for (InputIndex i = 0; i < inputs_count_; ++i) {
    InputStatus* status = inputs_status_ + i;
    status->assert_consistent();
    if (status->is_ended() || status->is_last_chunk_overall()) {
      continue;
    }
    if (min_input == kInvalidInput) {
      min_input = i;
    } else {
      ASSERT_ND(!inputs_status_[min_input].is_ended());
      ASSERT_ND(!inputs_status_[min_input].is_last_chunk_overall());
      if (compare_logs(status->get_chunk_log(), inputs_status_[min_input].get_chunk_log()) < 0) {
        min_input = i;
      }
    }
  }
  return min_input;
}

MergeSort::InputIndex MergeSort::pick_chunks() {
  uint32_t chunks;
  for (chunks = 0; chunks < chunk_batch_size_; ++chunks) {
    InputIndex min_input = determine_min_input();
    if (min_input == kInvalidInput) {
      // now all inputs are in the last chunks, we can simply merge them all in one shot!
      return kInvalidInput;
    }

    if (inputs_status_[min_input].is_last_chunk_in_window()) {
      VLOG(1) << "Min Input-" << min_input << " needs to shift window. chunks=" << chunks;
      break;
    }
    next_chunk(min_input);

    inputs_status_[min_input].assert_consistent();
  }

  VLOG(1) << "Now determining batch-threshold... chunks=" << chunks;
  return determine_min_input();
}

void MergeSort::batch_sort(MergeSort::InputIndex min_input) {
  batch_sort_prepare(min_input);
  ASSERT_ND(current_count_ <= buffer_capacity_);

  // First, sort it with std::sort, which is (*) smart enough to switch to heap sort for this case.
  // (*) at least gcc's does.
  debugging::StopWatch sort_watch;
  std::sort(&(sort_entries_->data_), &(sort_entries_[current_count_].data_));
  sort_watch.stop();
  VLOG(1) << "Storage-" << id_ << ", merge sort (main) of " << current_count_ << " logs in "
    << sort_watch.elapsed_ms() << "ms";

  if (type_ != storage::kArrayStorage
    && (shortest_key_length_ != 8U || longest_key_length_ != 8U)) {
    // the sorting above has to be adjusted if we need additional logic for key comparison
    batch_sort_adjust_sort();
  }

  assert_sorted();
}

void MergeSort::batch_sort_prepare(MergeSort::InputIndex min_input) {
  current_count_ = 0;
  if (min_input == kInvalidInput) {
    // this is the last iteration! get all remaining logs from all inputs
    for (InputIndex i = 0; i < inputs_count_; ++i) {
      InputStatus* status = inputs_status_ + i;
      ASSERT_ND(status->is_last_chunk_overall());
      if (status->is_ended()) {
        continue;
      }
      append_logs(i, status->chunk_relative_pos_ + status->get_chunk_log()->header_.log_length_);
      status->assert_consistent();
    }
  } else {
    // merge-sort upto batch-threshold
    const log::RecordLogType* threshold = inputs_status_[min_input].get_chunk_log();
    for (InputIndex i = 0; i < inputs_count_; ++i) {
      InputStatus* status = inputs_status_ + i;
      if (status->is_ended()) {
        continue;
      }

      if (i == min_input) {
        // the input that provides threshold itself. Hence, all logs before the last log are
        // guaranteed to be strictly smaller than the threshold.
        append_logs(i, status->chunk_relative_pos_);
        ASSERT_ND(status->chunk_relative_pos_ == status->cur_relative_pos_);
      } else {
        // otherwise, we have to add logs that are smaller than the threshold.
        // to avoid key comparison in most cases, we use "previous chunk" hint.
        if (status->previous_chunk_relative_pos_ != status->chunk_relative_pos_) {
          append_logs(i, status->previous_chunk_relative_pos_);
          ASSERT_ND(status->previous_chunk_relative_pos_ == status->cur_relative_pos_);
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
          // this means we added even the last log. This can happen only at the overall last chunk
          // because we pick batch threshold that is the smallest chunk-last-key.
          ASSERT_ND(status->is_last_chunk_overall());
        }
      }

      status->assert_consistent();
    }
  }
}

void MergeSort::batch_sort_adjust_sort() {
  debugging::StopWatch sort_watch;
  uint32_t cur = 0;
  uint32_t debug_stat_run_count = 0;
  uint32_t debug_stat_longest_run = 0;
  uint32_t debug_stat_runs_total = 0;
  while (LIKELY(cur + 1U < current_count_)) {
    // if the 8-bytes key is strictly smaller, we don't need additional check.
    // and it should be the vast majority of cases.
    uint64_t short_key = sort_entries_[cur].get_key();
    ASSERT_ND(short_key <= sort_entries_[cur + 1U].get_key());
    if (LIKELY(short_key < sort_entries_[cur + 1U].get_key())) {
      ++cur;
      continue;
    }

    // figure out how long the run goes on.
    uint32_t next = cur + 2U;
    bool needs_to_check =
      sort_entries_[cur].needs_additional_check()
      || sort_entries_[cur + 1U].needs_additional_check();
    for (next = cur + 2U;
        next < current_count_ && short_key == sort_entries_[next].get_key();
        ++next) {
      ASSERT_ND(short_key <= sort_entries_[next].get_key());
      needs_to_check |= sort_entries_[next].needs_additional_check();
    }
    // now, next points to the first entry that has a different key (or current_count_). thus:
    uint32_t run_length = next - cur;
    debug_stat_runs_total += run_length;
    debug_stat_longest_run = std::max<uint32_t>(debug_stat_longest_run, run_length);
    ++debug_stat_run_count;

    // so far only masstree. hash should be added
    ASSERT_ND(type_ == storage::kMasstreeStorage);
    if (needs_to_check) {  // if all entries in this range are 8-bytes keys, no need.
      AdjustComparatorMasstree comparator(position_entries_, inputs_status_);
      std::sort(sort_entries_ + cur, sort_entries_ + next, comparator);
    }
    cur = next;
  }
  sort_watch.stop();
  VLOG(1) << "Storage-" << id_ << ", merge sort (adjust) of " << current_count_ << " logs in "
    << sort_watch.elapsed_ms() << "ms. run_count=" << debug_stat_run_count << ", "
      << "longest_run=" << debug_stat_longest_run << ", total_runs=" << debug_stat_runs_total;
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
  InputStatus* status = inputs_status_ + input_index;
  ASSERT_ND(status->to_absolute_pos(upto_relative_pos) <= status->end_absolute_pos_);
  uint64_t relative_pos = status->cur_relative_pos_;
  if (type_ == storage::kArrayStorage) {
    while (LIKELY(relative_pos < upto_relative_pos)) {
      relative_pos += populate_entry_array(input_index, relative_pos);
    }
  } else {
    ASSERT_ND(type_ == storage::kMasstreeStorage);
    while (LIKELY(relative_pos < upto_relative_pos)) {
      relative_pos += populate_entry_masstree(input_index, relative_pos);
    }
  }
  ASSERT_ND(relative_pos == upto_relative_pos);

  if (upto_relative_pos > status->chunk_relative_pos_) {
    // we appeneded even the last log of this chunk! this should happen only at the last chunk.
    ASSERT_ND(status->is_last_chunk_overall());
    status->chunk_relative_pos_ = upto_relative_pos;
  }
  status->cur_relative_pos_ = upto_relative_pos;
  status->previous_chunk_relative_pos_ = upto_relative_pos;
  status->assert_consistent();
}

inline uint16_t MergeSort::populate_entry_array(InputIndex input_index, uint64_t relative_pos) {
  InputStatus* status = inputs_status_ + input_index;
  ASSERT_ND(current_count_ < buffer_capacity_);
  ASSERT_ND(relative_pos < status->window_size_);
  ASSERT_ND(relative_pos % 8U == 0);
  const storage::array::ArrayCommonUpdateLogType* the_log
    = reinterpret_cast<const storage::array::ArrayCommonUpdateLogType*>(
        status->from_byte_pos(relative_pos));
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
  position_entries_[current_count_].log_type_ = the_log->header_.log_type_code_;
  position_entries_[current_count_].input_position_ = to_buffer_position(relative_pos);
  ++current_count_;

  return the_log->header_.log_length_;
}

inline uint16_t MergeSort::populate_entry_masstree(InputIndex input_index, uint64_t relative_pos) {
  InputStatus* status = inputs_status_ + input_index;
  ASSERT_ND(current_count_ < buffer_capacity_);
  ASSERT_ND(relative_pos < status->window_size_);
  ASSERT_ND(relative_pos % 8U == 0);
  const storage::masstree::MasstreeCommonLogType* the_log
    = reinterpret_cast<const storage::masstree::MasstreeCommonLogType*>(
        status->from_byte_pos(relative_pos));
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
  position_entries_[current_count_].log_type_ = the_log->header_.log_type_code_;
  position_entries_[current_count_].input_position_ = to_buffer_position(relative_pos);
  ++current_count_;

  return the_log->header_.log_length_;
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
    int cmp = compare_logs(prev, cur);
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
