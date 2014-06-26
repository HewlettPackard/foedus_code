/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_LOG_REDUCER_IMPL_HPP_
#define FOEDUS_SNAPSHOT_LOG_REDUCER_IMPL_HPP_
#include <stdint.h>

#include <atomic>
#include <iosfwd>
#include <map>
#include <string>
#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/mapreduce_base_impl.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/condition_variable_impl.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief A log reducer, which receives log entries sent from mappers
 * and applies them to construct new snapshot files.
 * @ingroup SNAPSHOT
 * @details
 * @section REDUCER_OVERVIEW Overview
 * Reducers receive log entries from mappers and apply them to new snapshot files.
 *
 * @section SORTING Sorting
 * The log entries are sorted in a few steps to be processed efficiently and simply.
 * Sorting starts when one of the reducer's buffer becomes full.
 * Reducer never starts sorting until that to maximize the benefits of batch-processing
 * (this design might be revisited later, though).
 * Reducers maintain two buffers to let mappers keep sending data while reducers are sorting
 * and dumping to temporary files.
 *
 * @subsection STORAGE-SORT Storage Sorting
 * The first step is to sort log entries by storage, which is done in mappers.
 * We process all log entries of one storage together.
 * This has a benefit of code simplicity and less D-cache misses.
 * We don't actually sort in this case because we don't care the order between
 * storages. Thus, we use hashmap-like structure in mappers to sort based on storage-id.
 *
 * Upon receiving a chunk of data from mappers, the reducer has to collect all of them
 * to do the following (otherwise the sorting is incomplete). This is done by simply reading
 * all block headers utilizing the block_length_ property.
 * Assuming each block is sufficiently large, this jumping cost on DRAM should be negligible.
 * If each block is not sufficiently large, there are anyway other performance issues.
 *
 * @subsection KEY-ORDINAL-SORT Key and Ordinal Sorting
 * Then, in each storage, we sort logs by keys and then by ordinal (*).
 * The algorithm to do this sorting depends on the storage type (eg Array, Masstree)
 * because some storage has a VERY efficient way to do this.
 * We exploit the fact that this sorting occurs only per storage, just passing the whole
 * log entries for the storage to storage-specific logic defined in foedus::storage::Partitioner.
 * This is another reason to sort by storage first.
 *
 * (*) We do need to sort by ordinal. Otherwise correct result is not guaranteed.
 * For example, imagine the following case:
 *  \li UPDATE rec-1 to A. Log-ordinal 1.
 *  \li UPDATE rec-1 to B. Log-ordinal 2.
 * Ordinal-1 must be processed before ordinal 2.
 *
 * For more details, see foedus::storage::Partitioner.
 *
 * @subsection DUMP-MERGE Dumping Logs and and Merging
 * After the sorting, the reducer dumps the buffer to a file.
 * When all logs are received, the reducer does merge-sort on top of the sorted run files.
 *
 * @subsection COMPACTING Compacting Logs
 * In some cases, we can delete log entries for the same keys.
 * For example, when we have two logs for the same key like the example above, we can safely
 * omit the first log with ordinal 1 AS FAR AS both logs appear in the same reducer buffer
 * and updated byte positions in the record are the same.
 * Another example is updates followed by a deletion.
 *
 * This compaction is especially useful for a record that is repeatedly updated/inserted/deleted,
 * such as TPC-C's WAREHOUSE/DISTRICT records, where several thousands of overwrite-logs
 * in each reducer buffer will be compacted into just one log.
 *
 * See foedus::storage::Partitioner::sort_batch() for more details.
 *
 * @section DATAPAGES Data Pages
 * One tricky thing in reducer is how it manages data pages to read previous snapshot pages
 * and apply the new logs. So far, we assume each reducer allocates a sufficient amount of
 * DRAM to hold all pages it read/write during one snapshotting.
 * If this doesn't hold, we might directly allocate pages on NVRAM and read/write there.
 *
 * @note
 * This is a private implementation-details of \ref SNAPSHOT, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class LogReducer final : public MapReduceBase {
 public:
  LogReducer(Engine* engine, LogGleaner* parent, thread::ThreadGroupId numa_node)
    : MapReduceBase(engine, parent, numa_node, numa_node), sorted_runs_(0), current_buffer_(0) {}

  /** One LogReducer corresponds to one NUMA node (partition). */
  thread::ThreadGroupId   get_id() const { return id_; }
  std::string             to_string() const override {
    return std::string("LogReducer-") + std::to_string(id_);
  }
  friend std::ostream&    operator<<(std::ostream& o, const LogReducer& v);

  /**
   * @brief Append the log entries of one storage in the given buffer to this reducer's buffer.
   * @param[in] storage_id all log entries are of this storage
   * @param[in] send_buffer contains log entries to copy
   * @param[in] log_count number of log entries to copy
   * @param[in] send_buffer_size byte count to copy
   * @details
   * This is the interface via which mappers send log entries to reducers.
   * Internally, this atomically changes the status of the current reducer buffer to reserve
   * a contiguous space and then copy without blocking other mappers.
   * If this methods hits a situation where the current buffer becomes full, this methods
   * wakes up the reducer and lets it switch the current buffer.
   * All log entries are contiguously copied. One block doesn't span two buffers.
   */
  void append_log_chunk(
    storage::StorageId storage_id,
    const char* send_buffer,
    uint32_t log_count,
    uint64_t send_buffer_size);

 protected:
  ErrorStack  handle_initialize() override;
  ErrorStack  handle_uninitialize() override;
  ErrorStack  handle_process() override;

 private:
  enum Constants {
    /**
     * A bit-wise flag in BufferStatus's flags_.
     * If this bit is on, no more mappers can enter the buffer as a new writer.
     */
    kFlagNoMoreWriters = 0x0001,
    /** @see BlockHeader::magic_word_ */
    kBlockHeaderMagicWord = 0xDEADBEEF,
  };
  /**
   * Compactly represents important status informations of a reducer buffer.
   * Concurrent threads use atomic CAS to change any of these information.
   * Last 32 bits are tail position of the buffer in bytes divided by 8, so at most 32 GB buffer.
   */
  union BufferStatus {
    uint64_t word;
    struct Components {
      uint16_t        active_writers_;
      uint16_t        flags_;
      BufferPosition  tail_position_;
    } components;
  };

  /**
   * All buffer blocks sent via append_log_chunk() put this header at first.
   */
  struct BlockHeader {
    storage::StorageId  storage_id_;
    uint32_t            log_count_;
    BufferPosition      block_length_;
    /** just for sanity check. */
    uint32_t            magic_word_;
  };

  struct ReducerBuffer {
    memory::AlignedMemorySlice  buffer_slice_;

    std::atomic<uint64_t>       status_;  // actually of type BufferStatus

    char filler_to_avoid_false_sharing_[
      assorted::kCachelineSize
      - sizeof(memory::AlignedMemorySlice)
      - sizeof(std::atomic<uint64_t>)];


    BufferStatus get_status() const {
      BufferStatus ret;
      ret.word = status_.load();
      return ret;
    }
    BufferStatus get_status_weak() const {
      BufferStatus ret;
      ret.word = status_.load(std::memory_order_relaxed);
      return ret;
    }
    bool        is_no_more_writers() const {
      return (get_status().components.flags_ & kFlagNoMoreWriters) != 0;
    }
    bool        is_no_more_writers_weak() const {
      return (get_status_weak().components.flags_ & kFlagNoMoreWriters) != 0;
    }
  };

  /**
   * Underlying memory of reducer buffer.
   */
  memory::AlignedMemory   buffer_memory_;

  /**
   * Used to sort log entries in each storage.
   * This is automatically extended when needed.
   */
  memory::AlignedMemory   sort_buffer_;

  /**
   * Used to temporarily store input/output positions of all log entries for one storage.
   * This is automatically extended when needed.
   * Note that this contains two slices, input_positions_slice_ and output_positions_slice_.
   */
  memory::AlignedMemory   positions_buffers_;

  /** Half of positions_buffers_ used for input buffer for batch-sorting method. */
  memory::AlignedMemorySlice input_positions_slice_;
  /** Half of positions_buffers_ used for output buffer for batch-sorting method. */
  memory::AlignedMemorySlice output_positions_slice_;

  /**
   * The reducer buffer is split into two so that reducers can always work on completely filled
   * buffer while mappers keep appending to another buffer.
   * @see current_buffer_
   */
  ReducerBuffer buffers_[2];

  /**
   * How many buffers written out as a temporary file.
   * If this number is zero when all mappers complete, the reducer does not bother writing out
   * the last and only buffer to file.
   * For now, this value should be always same as current_buffer_.
   */
  uint32_t      sorted_runs_;

  /**
   * buffers_[current_buffer_ % 2] is the buffer mappers should append to.
   * This value increases for every buffer switch.
   */
  std::atomic<uint32_t>   current_buffer_;

  /**
   * Fired (notify_all) whenever current_buffer_ is switched.
   * Used by mappers to wait for available buffer.
   */
  thread::ConditionVariable current_buffer_changed_;

  ReducerBuffer* get_current_buffer() { return &buffers_[current_buffer_ % 2]; }

  /**
   * Sorts and dumps another buffer (buffers_[sorted_runs_ % 2]).
   * @pre buffers_[sorted_runs_ % 2] is closed for new writers
   * (but doesn't have to be active_writers_==0. this method waits for it)
   * @details
   * When it is completed, this method increments sorted_runs_.
   * So, the next target of sort/dump is another buffer.
   */
  ErrorStack dump_buffer();
  /**
   * First sub routine of dump_buffer.
   * Wait for all mappers to finish writing (active_writers==0).
   * because each mapper might be writing up to 1MB, and might be from remote NUMA node,
   * this chould take hundred microseconds.
   * a bit unclear whether spinning is better or not in this case.
   * however, buffer dumping happens only occasionally, so the difference is not that significant.
   * thus, we simply spin here.
   */
  ErrorStack dump_buffer_wait_for_writers(const ReducerBuffer& buffer) const;

  /**
   * Second sub routine of dump_buffer().
   * We list up all blocks for each storage to sort them by key.
   * assuming that there aren't a huge number of blocks (each block should be several hundred KB),
   * we simply use a vector and a map. if this becomes the bottleneck, let's tune it later.
   */
  void dump_buffer_scan_block_headers(
    char* buffer_base,
    BufferPosition tail_position,
    std::map<storage::StorageId, std::vector<BufferPosition> > *blocks) const;

  void expand_sort_buffer_if_needed(uint64_t required_size);
  void expand_positions_buffers_if_needed(uint64_t required_size_per_buffer);
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_REDUCER_IMPL_HPP_
