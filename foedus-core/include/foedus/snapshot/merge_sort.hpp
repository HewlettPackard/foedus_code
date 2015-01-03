/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_MERGE_SORT_HPP_
#define FOEDUS_SNAPSHOT_MERGE_SORT_HPP_

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/initializable.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief Receives an arbitrary number of sorted buffers and emits one fully sorted stream of logs.
 * @ingroup SNAPSHOT
 * @details
 * @par Where this is used, and why
 * During snapshot, log reducer merges the current in-memory buffer with zero or more dumped
 * sorted runs on disk. We initially used a tuple-based algorithm to select a stream that has the
 * smallest key for each iteration, but it was not desirable for overheads and code complexity.
 * Batching the sorting is much faster than even tournament-tree, and also separates sorting
 * logic from the composer. Hence this class.
 *
 * @par Batching, Batching, Batching
 * All frequently used code path must be batched, at least in 1000s.
 * An exception is the code that is invoked only occasionally, eg once per block.
 * There are a few logics that must be customized for each storage type (eg handling of keys).
 * Such customization is done in granularity of batch, too. No switch for individual logs.
 *
 * @par Algorithm at a glance
 * \li a) Pick a \e chunk (1000s of logs) from an input whose last key (called \e chunk-last-key)
 * is the smallest among inputs.
 * \li b) Repeat a) until some input must move on to next window or we have a large number of logs.
 * \li c) The smallest chunk key among all inputs is the \e batch-threshold. All tuples in all
 * input's chunks whose key is strictly smaller than the threshold are sorted.
 * \li d) Composer consumes the sorted inputs.
 * \li e) Advance each input's current place, move their windows if needed (step a-d never moves
 * windows), and repeat a) until all inputs are done.
 * \li Some special condition: chunk-last-key survives to next iteration except when the chunk is
 * the last chunk of last window. In that case, the batch-threshold is chosen ignoring the input.
 * The logs are merged if key < threshold, marking the input as ended if all logs are merged.
 * If all inputs are in the last chunk (last iteration), we include all remainings from all inputs.
 * \li Another special condition: Obviously, this class does nothing when input_count is 1,
 * skipping all the overheads. This should hopefully happen often if reducers have large buffers.
 *
 * @par Modularity
 * This class has no dependency to other modules. It receives buffers of logs, that's it.
 * We must keep this class in that way for easier testing and tuning.
 */
class MergeSort CXX11_FINAL : public DefaultInitializable {
 public:
  /**
   * Position in MergeSort's buffer. We never sort more than 2^23 entries at once
   * because we merge by chunks.
   */
  typedef uint32_t MergedPosition;
  /** Index in input streams */
  typedef uint16_t InputIndex;
  /** Represents null. */
  const InputIndex kInvalidInput = static_cast<InputIndex>(-1U);

  enum Constants {
    /** This is a theoretical max. additionally it must be less than buffer_capacity_ */
    kMaxMergedPosition = 1 << 23,
    /** 1024 logs per chunk */
    kLogChunk = 1 << 10,
    /** Suppose each log is 50 bytes: 1k*256*50b=12.5 MB worth logs to sort per batch. */
    kDefaultChunkBatch = 1 << 8,
    /** We assume the path wouldn't be this deep. */
    kMaxLevels = 32,
    /**
     * To avoid handling the case where a log spans an end of window, chunks leave at least
     * this many bytes in each window. No single log can be more than this size, so it simplifies
     * the logic. If the input is in the last window, this value has no effects.
     */
    kWindowChunkReserveBytes = 1 << 16,
  };
  /**
   * Also, when the input consumed more than this fraction of current window, we move the window.
   * This means we have to memmove 5% everytime, but instead we can avoid many-small batches.
   */
  const float kWindowMoveThreshold = 0.95;

  /**
   * Entries we actually sort.
   * \li 0-7 bytes: the key. ArrayOffset, or the most significant 8 bytes of other storages' key.
   * \li 8-9 bytes: compressed epoch (difference from base_epoch)
   * \li 10-12 bytes: in-epoch-ordinal
   * \li 13 byte's 1st bit: Whether the key needs additional comparison if the 8-bytes are the same.
   * This is set to true only in masstree/hash and the key length is not 8 bytes.
   * \li 13 byte's other bits -15 bytes: Position in MergeSort's buffer.
   *
   * It's quite important to keep this 16 bytes. But, in some cases, we need more than 16 bytes to
   * compare logs. In that case, we have to addtionally invoke batch_sort_adjust_sort().
   */
  struct SortEntry {
    inline void set(
      uint64_t  key,
      uint16_t  compressed_epoch,
      uint32_t  in_epoch_ordinal,
      bool      needs_additional_check,
      MergedPosition position) ALWAYS_INLINE {
      ASSERT_ND(in_epoch_ordinal < (1U << 24));
      ASSERT_ND(position < kMaxMergedPosition);
      data_
        = static_cast<__uint128_t>(key) << 64
          | static_cast<__uint128_t>(compressed_epoch) << 48
          | static_cast<__uint128_t>(in_epoch_ordinal) << 24
          | static_cast<__uint128_t>(needs_additional_check ? 1U << 23 : 0)
          | static_cast<__uint128_t>(position);
    }
    inline uint64_t get_key() const ALWAYS_INLINE {
      return static_cast<uint64_t>(data_ >> 64);
    }
    inline bool needs_additional_check() const ALWAYS_INLINE {
      return data_ & (1U << 23);
    }
    inline MergedPosition get_position() const ALWAYS_INLINE {
      return static_cast<MergedPosition>(data_) & 0x7FFFFFU;
    }

    __uint128_t data_;
  };

  /**
   * Provides additional information for each entry we are sorting.
   */
  struct PositionEntry {
    uint16_t        input_index_;     // +2 -> 2
    /** not the enum itself for explicit size. use the getter for type safety. */
    uint16_t        log_type_;        // +2 -> 4
    BufferPosition  input_position_;  // +4 -> 8

    inline log::LogCode get_log_type() const ALWAYS_INLINE {
      return static_cast<log::LogCode>(log_type_);
    }
  };

  /**
   * Current status of each input.
   * "Current" means the first log that is not yet processed.
   * "Chunk" means the last log in the current chunk to be batch-processed.
   * "Previous chunk" is the previous chunk of this input in the same batch, thus all logs in
   * the previous chunk are guaranteed to be smaller than the batch-threshold.
   * "End" means the end of input.
   */
  struct InputStatus CXX11_FINAL {
    const char*     window_;
    /** @invariant cur_relative_pos_ < window_size_ */
    uint64_t        cur_relative_pos_;
    /** @invariant cur_relative_pos_ <= chunk_relative_pos_ < window_size_ */
    uint64_t        chunk_relative_pos_;
    char            padding_[6];
    /** @invariant previous_chunk_relative_pos_ <= chunk_relative_pos_ */
    uint64_t        previous_chunk_relative_pos_;
    /** relative pos counts from this offset */
    uint64_t        window_offset_;
    uint64_t        window_size_;
    uint64_t        end_absolute_pos_;

    inline void assert_consistent() const ALWAYS_INLINE {
#ifndef NDEBUG
      ASSERT_ND(window_);
      ASSERT_ND(cur_relative_pos_ <= window_size_);
      ASSERT_ND(chunk_relative_pos_ <= window_size_);
      ASSERT_ND(previous_chunk_relative_pos_ <= window_size_);
      ASSERT_ND(window_offset_ <= end_absolute_pos_);
      ASSERT_ND(cur_relative_pos_ + window_offset_ <= end_absolute_pos_);
      ASSERT_ND(chunk_relative_pos_ + window_offset_ <= end_absolute_pos_);
      ASSERT_ND(previous_chunk_relative_pos_ + window_offset_ <= end_absolute_pos_);
      ASSERT_ND(cur_relative_pos_ <= chunk_relative_pos_);
      ASSERT_ND(previous_chunk_relative_pos_ <= chunk_relative_pos_);
#endif  // NDEBUG
    }
    inline const log::RecordLogType* get_cur_log() const ALWAYS_INLINE {
      assert_consistent();
      return reinterpret_cast<const log::RecordLogType*>(window_ + cur_relative_pos_);
    }
    inline const log::RecordLogType* get_chunk_log() const ALWAYS_INLINE {
      assert_consistent();
      return reinterpret_cast<const log::RecordLogType*>(window_ + chunk_relative_pos_);
    }
    inline const log::RecordLogType* from_byte_pos(uint64_t pos) const ALWAYS_INLINE {
      assert_consistent();
      ASSERT_ND(pos < window_size_);
      return reinterpret_cast<const log::RecordLogType*>(window_ + pos);
    }
    inline const log::RecordLogType* from_compact_pos(BufferPosition pos) const ALWAYS_INLINE {
      assert_consistent();
      ASSERT_ND(pos * 8ULL < window_size_);
      return reinterpret_cast<const log::RecordLogType*>(window_ + pos * 8ULL);
    }
    inline uint64_t to_absolute_pos(uint64_t relative_pos) const ALWAYS_INLINE {
      assert_consistent();
      return window_offset_ + relative_pos;
    }
    inline bool is_last_window() const ALWAYS_INLINE {
      assert_consistent();
      return to_absolute_pos(window_size_) >= end_absolute_pos_;
    }
    inline bool is_ended() const ALWAYS_INLINE {
      assert_consistent();
      return to_absolute_pos(cur_relative_pos_) == end_absolute_pos_;
    }
    inline bool is_last_chunk_in_window() const ALWAYS_INLINE {
      if (is_last_window()) {
        // in last window, we accurately determines the last chunk
        uint64_t chunk_abs_pos = to_absolute_pos(chunk_relative_pos_);
        ASSERT_ND(chunk_abs_pos <= end_absolute_pos_);
        if (chunk_abs_pos >= end_absolute_pos_) {
          return true;
        }
        uint16_t length = get_chunk_log()->header_.log_length_;
        ASSERT_ND(length > 0);
        ASSERT_ND(chunk_abs_pos + length <= end_absolute_pos_);
        return chunk_abs_pos + length >= end_absolute_pos_;
      } else {
        // in this case, we conservatively determines the last chunk
        return chunk_relative_pos_ + kWindowChunkReserveBytes >= window_size_;
      }
    }
    inline bool is_last_chunk_overall() const ALWAYS_INLINE {
      return is_last_window() && is_last_chunk_in_window();
    }
  };

  /**
   * Used in batch_sort_adjust_sort if the storage is a masstree storage.
   * This comparator is actually a valid comparator for any case. But, it's slower, so used
   * only where we need to.
   */
  struct AdjustComparatorMasstree CXX11_FINAL {
    AdjustComparatorMasstree(PositionEntry* position_entries, InputStatus* inputs_status)
      : position_entries_(position_entries), inputs_status_(inputs_status) {}
    inline bool operator() (const SortEntry& left, const SortEntry& right) const ALWAYS_INLINE {
      ASSERT_ND(left.get_key() <= right.get_key());
      if (left.get_key() < right.get_key()) {
        return true;
      }
      MergedPosition left_index = left.get_position();
      MergedPosition right_index = right.get_position();
      const PositionEntry& left_pos = position_entries_[left_index];
      const PositionEntry& right_pos = position_entries_[right_index];
      const storage::masstree::MasstreeCommonLogType* left_casted
        = reinterpret_cast<const storage::masstree::MasstreeCommonLogType*>(
          inputs_status_[left_pos.input_index_].from_compact_pos(left_pos.input_position_));
      const storage::masstree::MasstreeCommonLogType* right_casted
        = reinterpret_cast<const storage::masstree::MasstreeCommonLogType*>(
          inputs_status_[right_pos.input_index_].from_compact_pos(right_pos.input_position_));
      int cmp = storage::masstree::MasstreeCommonLogType::compare_logs(left_casted, right_casted);
      return cmp < 0 || (cmp == 0 && left_index < right_index);
    }
    PositionEntry* position_entries_;
    InputStatus* inputs_status_;
  };

  /**
   * Represents a group of consecutive logs in the current batch.
   * Priority is 1) common-key (at least 2 logs), 2) common log code.
   */
  struct GroupifyResult {
    uint32_t count_;            // +4 -> 4
    bool has_common_key_;       // +1 -> 5
    bool has_common_log_code_;  // +1 -> 6
    uint16_t log_code_;         // +2 -> 8
  };

  MergeSort(
    storage::StorageId id,
    storage::StorageType type,
    Epoch base_epoch,
    SortedBuffer* const* inputs,
    uint16_t inputs_count,
    uint16_t max_original_pages,
    memory::AlignedMemory* const work_memory,
    uint16_t chunk_batch_size = kDefaultChunkBatch);

  /**
   * @brief Executes merge-sort on several thousands of logs and provides the result as a batch.
   * @pre is_initialized (call initialize() first!)
   * @details
   * Composer repeatedly invokes this method until it returns false.
   * For each invokation (batch), composer consumes the entries in sorted order, in other words
   * sort_entries_[0] to sort_entries_[current_count_ - 1], which points to position_entries_.
   * This level of indirection might cause more L1 cache misses (only if we could fit everything
   * into 16 bytes!), so we do prefetching to ameriolate it.
   * @see fetch_logs(), which does the prefetching
   * @see get_current_count(), which will return the number of logs in the generated batch.
   */
  ErrorStack next_batch();

  /**
   * To reduce L1 cache miss stall, we prefetch some number of position entries and
   * the pointed log entries in parallel.
   * @param[in] sort_pos we prefetch from this sort_entries_ -> position_entries_ -> logs
   * @param[in] count we prefetch upto sort_entries_[sort_pos + count - 1]. Should be at least 2
   * or 4 to make parallel prefetch effective.
   * @param[out] out Fetched logs.
   * @pre sort_pos <= current_count_
   * @return fetched count. unless sort_pos+count>current_count, same as count.
   */
  uint32_t fetch_logs(uint32_t sort_pos, uint32_t count, log::RecordLogType const** out) const;

  /**
   * @brief Find a group of consecutive logs from the given position that have either a common
   * log type or a common key.
   * @param[in] begin starting position in the sorted array.
   * @return the group found in the current batch.
   * @details
   * Some composer uses this optional method to batch-process the logs.
   * In an ideal case, there always is a big group, and composer calls this method per
   * hundreds of logs. But, it's also quite possible that there isn't a good group. Hence,
   * these methods are called very frequently (potentially for each log). Worth explicit inlining.
   */
  GroupifyResult groupify(uint32_t begin) const ALWAYS_INLINE;

  /**
   * For debug/test only. Checks if sort_entries_ are indeed fully sorted.
   * This method is wiped out in release mode.
   */
  void assert_sorted();

  ErrorStack initialize_once() CXX11_OVERRIDE;
  ErrorStack uninitialize_once() CXX11_OVERRIDE { return kRetOk; }

  inline bool is_no_merging() const ALWAYS_INLINE { return inputs_count_ == 1U; }

  inline bool is_ended_all() const {
    for (InputIndex i = 0; i < inputs_count_; ++i) {
      if (!inputs_status_[i].is_ended()) {
        return false;
      }
    }
    return true;
  }

  inline storage::StorageId get_storage_id() const ALWAYS_INLINE { return id_; }
  inline MergedPosition get_current_count() const ALWAYS_INLINE { return current_count_; }
  inline InputIndex     get_inputs_count() const ALWAYS_INLINE { return inputs_count_; }
  inline const PositionEntry* get_position_entries() const ALWAYS_INLINE {
    return position_entries_;
  }
  inline const SortEntry* get_sort_entries() const ALWAYS_INLINE { return sort_entries_; }
  inline const log::RecordLogType* resolve_merged_position(MergedPosition pos) const ALWAYS_INLINE {
    ASSERT_ND(pos < current_count_);
    const PositionEntry& entry = position_entries_[pos];
    return inputs_status_[entry.input_index_].from_compact_pos(entry.input_position_);
  }
  inline const log::RecordLogType* resolve_sort_position(uint32_t sort_pos) const ALWAYS_INLINE {
    ASSERT_ND(sort_pos < current_count_);
    MergedPosition pos = sort_entries_[sort_pos].get_position();
    return resolve_merged_position(pos);
  }
  inline storage::Page* get_original_pages() const ALWAYS_INLINE { return original_pages_; }

 private:
  const storage::StorageId      id_;
  const storage::StorageType    type_;
  const Epoch                   base_epoch_;
  const uint16_t                shortest_key_length_;
  const uint16_t                longest_key_length_;
  /** Sorted runs. */
  SortedBuffer* const*          inputs_;
  /** Number of sorted runs. */
  const InputIndex              inputs_count_;
  /** Number of pages to allocate for get_original_pages() */
  const uint16_t                max_original_pages_;
  /** how many chunks one batch has */
  const uint16_t                chunk_batch_size_;
  /** Working memory to be used in this class. Automatically expanded if needed. */
  memory::AlignedMemory* const  work_memory_;

  /**
   * count of sort_entries_ and position_entries_.
   * @invariant current_count_ <= buffer_capacity_
   */
  MergedPosition                current_count_;
  /** Allocated size of each xxx_entry. It's ceil-ed to make each xxx_enty 4kb aligned. */
  uint32_t                      buffer_capacity_;

  // followings are backed by work_memory_, and allocatd at initialize_once.
  /** Index has no meaning after sorting (before sorting, MergedPosition) */
  SortEntry*                    sort_entries_;
  /** Index is MergedPosition */
  PositionEntry*                position_entries_;
  /** kMaxLevels + 1 of original pages. some storage type needs fewer pages. */
  storage::Page*                original_pages_;
  /** index is 0 to inputs_count_ - 1 */
  InputStatus*                  inputs_status_;

  /** trivial case of next_batch(). */
  void next_batch_one_input();

  /**
   * subroutine of next_batch() to move window (Step e) if there is any input close
   * to the end of window.
   */
  ErrorStack advance_window();

  /**
   * Advance chunk_relative_pos_ for one-chunk of logs within the current window (this method
   * does not move window).
   * @pre !ended_
   * @pre !is_last_chunk_in_window
   */
  void next_chunk(InputIndex input_index);

  /**
   * @return index of the input whose chunk-last-key will be the current batch-threshold.
   * -1U if all inputs are either ended or in last chunk. In other word,
   * return value is -1 or !inputs_status_[returned]->last_chunk_.
   * When there is a tie, we pick an input with smaller index (input-0 is most preferred).
   */
  InputIndex determine_min_input() const;

  /**
   * subroutine of next_batch for Step a and b.
   * Pick up to kChunkBatch chunks from inputs whose chunk-last-key is the smallest among inputs.
   * @return same as determine_min_input().
   */
  InputIndex pick_chunks();

  /**
   * Step c. Sorts all entries up to the threshold.
   */
  void batch_sort(InputIndex min_input);
  /**
   * Subroutine of batch_sort to populate sort_entries_ and position_entries_.
   */
  void batch_sort_prepare(InputIndex min_input);
  /**
   * Subroutine of batch_sort to adjust the sorting results if the storage needs additional logic
   * for key comparison. This is used only when the key length might not be 8 bytes.
   */
  void batch_sort_adjust_sort();

  /**
   * Returns -1, 0, 1 when left is less than, same, larger than right in terms of key and xct_id.
   * This method itself is NOT optimized, so it must be called only a few times per 1000s of logs.
   * Instead, this method works for any kinds of logs.
   */
  int compare_logs(const log::RecordLogType* lhs, const log::RecordLogType* rhs) const;

  void append_logs(InputIndex input_index, uint64_t upto_relative_pos);

  uint16_t populate_entry_array(InputIndex input_index, uint64_t relative_pos) ALWAYS_INLINE;
  uint16_t populate_entry_masstree(InputIndex input_index, uint64_t relative_pos) ALWAYS_INLINE;

  // these methods are called very frequently (potentially for each log). worth explicit inlining.
  // all of them return the position upto (inclusive) which the logs have the common feature.
  uint32_t groupify_find_common_keys_8b(uint32_t begin) const ALWAYS_INLINE;
  uint32_t groupify_find_common_keys_general(uint32_t begin) const ALWAYS_INLINE;
  uint32_t groupify_find_common_log_type(uint32_t begin) const ALWAYS_INLINE;
};


inline bool is_array_log_type(uint16_t log_type) {
  return log_type == log::kLogCodeArrayOverwrite || log_type == log::kLogCodeArrayIncrement;
}
inline bool is_masstree_log_type(uint16_t log_type) {
  return
    log_type == log::kLogCodeMasstreeInsert
    || log_type == log::kLogCodeMasstreeDelete
    || log_type == log::kLogCodeMasstreeOverwrite;
}

inline MergeSort::GroupifyResult MergeSort::groupify(uint32_t begin) const {
  ASSERT_ND(begin < current_count_);
  GroupifyResult result;
  result.count_ = 1;
  result.has_common_key_ = false;
  result.has_common_log_code_ = false;
  result.log_code_ = log::kLogCodeInvalid;
  if (UNLIKELY(begin + 1U == current_count_)) {
    return result;
  }

  // first, check common keys
  uint32_t cur;
  if (shortest_key_length_ == 8U && longest_key_length_ == 8U) {
    cur = groupify_find_common_keys_8b(begin);
  } else {
    cur = groupify_find_common_keys_general(begin);
  }

  ASSERT_ND(cur >= begin);
  ASSERT_ND(cur < current_count_);
  if (UNLIKELY(cur != begin)) {
    result.has_common_key_ = true;
    result.count_ = cur - begin + 1U;
    return result;
  }

  // if keys are different, check common log types.
  cur = groupify_find_common_log_type(begin);
  ASSERT_ND(cur >= begin);
  ASSERT_ND(cur < current_count_);
  if (UNLIKELY(cur != begin)) {
    result.has_common_log_code_ = true;
    result.count_ = cur - begin + 1U;
    return result;
  } else {
    return result;
  }
}

inline uint32_t MergeSort::groupify_find_common_keys_8b(uint32_t begin) const {
  ASSERT_ND(shortest_key_length_ == 8U && longest_key_length_ == 8U);
  ASSERT_ND(begin + 1U < current_count_);
  uint32_t cur = begin;
  // 8 byte key is enough to determine equality.
  while (sort_entries_[cur].get_key() == sort_entries_[cur + 1U].get_key()
      && LIKELY(cur + 1U < current_count_)) {
    ++cur;
  }
  return cur;
}

inline uint32_t MergeSort::groupify_find_common_keys_general(uint32_t begin) const {
  ASSERT_ND(type_ != storage::kArrayStorage);
  ASSERT_ND(begin + 1U < current_count_);
  uint32_t cur = begin;
  // we might need more. a bit slower.
  while (sort_entries_[cur].get_key() == sort_entries_[cur + 1U].get_key()
      && LIKELY(cur + 1U < current_count_)) {
    if (sort_entries_[cur].needs_additional_check()
      || sort_entries_[cur + 1U].needs_additional_check()) {
      MergedPosition cur_pos = sort_entries_[cur].get_position();
      MergedPosition next_pos = sort_entries_[cur + 1U].get_position();
      ASSERT_ND(is_masstree_log_type(position_entries_[cur_pos].get_log_type()));
      ASSERT_ND(is_masstree_log_type(position_entries_[next_pos].get_log_type()));
      const storage::masstree::MasstreeCommonLogType* cur_log
        = reinterpret_cast<const storage::masstree::MasstreeCommonLogType*>(
            resolve_merged_position(cur_pos));
      const storage::masstree::MasstreeCommonLogType* next_log
        = reinterpret_cast<const storage::masstree::MasstreeCommonLogType*>(
            resolve_merged_position(next_pos));
      uint16_t key_len = cur_log->key_length_;
      ASSERT_ND(key_len != 8U || next_log->key_length_ != 8U);
      if (key_len != next_log->key_length_) {
        break;
      } else if (key_len > 8U) {
        if (std::memcmp(cur_log->get_key() + 8, next_log->get_key() + 8, key_len - 8U) != 0) {
          break;
        }
      }
    }
    ++cur;
  }
  return cur;
}

inline uint32_t MergeSort::groupify_find_common_log_type(uint32_t begin) const {
  ASSERT_ND(begin + 1U < current_count_);

  // First, let's avoid (relatively) expensive checks if there is no common log type,
  // and assume the worst case; we are calling this method for each log.
  uint32_t cur = begin;
  PositionEntry cur_pos = position_entries_[sort_entries_[cur].get_position()];
  PositionEntry next_pos = position_entries_[sort_entries_[cur + 1U].get_position()];
  if (LIKELY(cur_pos.log_type_ != next_pos.log_type_)) {
    return cur;
  }

  // the LIKELY above will be false if there is a group, but then the cost of branch misdetection
  // is amortized by the (hopefully) large number of logs processed together below.

  // okay, from now on, there likely is a number of logs with same log type.
  // this method has to read potentially a large number of position entries, which might be
  // not contiguous. thus, let's do parallel prefetching.
  const uint16_t kFetchSize = 8;
  cur = begin + 1U;
  while (LIKELY(cur + kFetchSize < current_count_)) {
    for (uint16_t i = 0; i < kFetchSize; ++i) {
      assorted::prefetch_cacheline(position_entries_ + sort_entries_[cur + i].get_position());
    }
    for (uint16_t i = 0; i < kFetchSize; ++i) {
      PositionEntry cur_pos = position_entries_[sort_entries_[cur + i].get_position()];
      PositionEntry next_pos = position_entries_[sort_entries_[cur + i + 1U].get_position()];
      // now that we assume there is a large group, this is UNLIKELY.
      if (UNLIKELY(cur_pos.log_type_ != next_pos.log_type_)) {
        return cur + i;
      }
    }
    cur += kFetchSize;
  }

  // last bits. no optimization needed.
  while (cur + 1U < current_count_) {
    PositionEntry cur_pos = position_entries_[sort_entries_[cur].get_position()];
    PositionEntry next_pos = position_entries_[sort_entries_[cur + 1U].get_position()];
    if (cur_pos.log_type_ != next_pos.log_type_) {
      return cur;
    }
    ++cur;
  }
  return cur;
}

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_MERGE_SORT_HPP_
