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
#include "foedus/log/common_log_types.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief Receives an arbitrary number of sorted buffers and emits one fully sorted stream of logs.
 * @ingroup SNAPSHOT STORAGE
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
 * @par Algorithm in a glance
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
 * The logs are merged if key >= threshold, marking the input as ended if all logs are merged.
 * If all inputs are in the last chunk (last iteration), we include all remainings from all inputs.
 * \li Another special condition: Obviously, this class does nothing when input_count is 1,
 * skipping all the overheads. This should hopefully happen often if reducers have large buffers.
 */
class MergeSort CXX11_FINAL : public DefaultInitializable {
 public:
  MergeSort(
    storage::StorageId id,
    storage::StorageType type,
    Epoch base_epoch,
    uint16_t shortest_key_length,
    uint16_t longest_key_length,
    SortedBuffer* const* inputs,
    uint16_t inputs_count,
    memory::AlignedMemory* const work_memory);

  /**
   * For debug/test only. Checks if sort_entries_ are indeed fully sorted.
   * This method is wiped out in release mode.
   */
  void assert_sorted();

  ErrorStack initialize_once() CXX11_OVERRIDE;
  ErrorStack uninitialize_once() CXX11_OVERRIDE { return kRetOk; }

 private:
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
    kMaxMergedPosition = 1 << 23,
    /** 1024 logs per chunk */
    kLogChunk = 1 << 10,
    /** Suppose each log is 50 bytes: 1k*256*50b=12.5 MB worth logs to sort per batch. */
    kChunkBatch = 1 << 8,
    /** We assume the path wouldn't be this deep. */
    kMaxLevels = 32,
  };

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
   * compare logs. In that case, we use the position to determine the order.
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
    uint16_t        key_length_;      // +2 -> 4
    BufferPosition  input_position_;  // +4 -> 8
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
    /** @invariant previous_chunk_relative_pos_ <= chunk_relative_pos_ */
    uint64_t        previous_chunk_relative_pos_;
    uint64_t        window_size_;
    uint64_t        end_absolute_pos_;

    bool            ended_;
    bool            last_chunk_;
    char            pad_[6];

    inline void assert_consistent() const ALWAYS_INLINE {
      ASSERT_ND(window_);
      ASSERT_ND(cur_relative_pos_ < window_size_);
      ASSERT_ND(cur_relative_pos_ <= chunk_relative_pos_);
      ASSERT_ND(previous_chunk_relative_pos_ <= chunk_relative_pos_);
      ASSERT_ND(chunk_relative_pos_ < window_size_);
      ASSERT_ND(!ended_ || last_chunk_);  // if it's ended, should have been in last chunk
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
  };

  const storage::StorageId      id_;
  const storage::StorageType    type_;
  const Epoch                   base_epoch_;
  const uint16_t                shortest_key_length_;
  const uint16_t                longest_key_length_;
  /** Sorted runs. */
  SortedBuffer* const*          inputs_;
  /** Number of sorted runs. */
  const InputIndex              inputs_count_;
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

  /**
   * Advance chunk_relative_pos_ for one-chunk of logs within the current window (this method
   * does not move window).
   * @pre !ended_
   * @pre !last_chunk_
   * @return whether it could get more chunk in current window. In other words, it returns false iff
   * chunk_relative_pos_ already points to the last log in the current window.
   */
  bool next_chunk(InputIndex input_index);

  /**
   * @return index of the input whose chunk-last-key will be the current batch-threshold.
   * -1U if all inputs are either ended or in last chunk. In other word,
   * return value is -1 or !inputs_status_[returned]->last_chunk_.
   */
  InputIndex determine_min_input() const;

  /**
   * Step a and b.
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
   * Returns -1, 0, 1 when left is less than, same, larger than right in terms of key and xct_id.
   * This method itself is NOT optimized, so it must be called only a few times per 1000s of logs.
   * Instead, this method works for any kinds of logs.
   */
  int compare_logs(const log::RecordLogType* lhs, const log::RecordLogType* rhs) const;

  void append_logs(InputIndex input_index, uint64_t upto_relative_pos);
  void append_logs_array(InputIndex input_index, uint64_t upto_relative_pos);
  void append_logs_masstree(InputIndex input_index, uint64_t upto_relative_pos);
};

}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_MERGE_SORT_HPP_
