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
#include <memory>
#include <string>
#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/fs/fwd.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/mapreduce_base_impl.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/storage/fwd.hpp"
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
 *
 * @todo This class got a bit bloated and hard to do a whitebox test because of dependencies
 * to other modules. Dump-part and merge-part should be separated into its own classes in a way
 * testcases can independently test them. Maybe reducer should be its own package?
 */
class LogReducer final : public MapReduceBase {
 public:
  LogReducer(Engine* engine, LogGleaner* parent, thread::ThreadGroupId numa_node)
    : MapReduceBase(engine, parent, numa_node, numa_node),
      snapshot_writer_(engine_, this),
      previous_snapshot_files_(engine_),
      sorted_runs_(0),
      total_storage_count_(0),
      current_buffer_(0) {}

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

  /** These are public, but used only from LogGleaner other than itself. */
  uint32_t get_root_info_page_count() const { return total_storage_count_; }
  memory::AlignedMemory& get_root_info_buffer() { return root_info_buffer_; }
  storage::Composer* create_composer(storage::StorageId storage_id);
  memory::AlignedMemory& get_composer_work_memory() { return composer_work_memory_; }

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
    /** @see DumpStorageHeaderFiller */
    kStorageHeaderFillerMagicWord = 0x8BADF00D,
    /** @see DumpStorageHeaderReal */
    kStorageHeaderRealMagicWord = 0xCAFEBABE,
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

  /**
   * All storage blocks in dump file start with this header.
   * This base object MUST be within 8 bytes so that DumpStorageHeaderFiller is within 8 bytes.
   */
  struct DumpStorageHeaderBase {
    /**
     * This is used to identify the storage block is a dummy (filler) one or a real one.
     * This must be either kStorageHeaderFillerMagicWord or kStorageHeaderRealMagicWord.
     */
    uint32_t            magic_word_;
    /**
     * Length of this block \e including the header.
     */
    BufferPosition      block_length_;
  };

  /**
   * A storage block in dump file that actually stores some storage.
   * The magic word for this is kStorageHeaderDummyMagicWord.
   */
  struct DumpStorageHeaderReal : public DumpStorageHeaderBase {
    storage::StorageId  storage_id_;
    uint32_t            log_count_;
  };

  /**
   * @brief A header for a dummy storage block that fills the gap between the end of
   * previous storage block and the beginning of next storage block.
   * @details
   * Such a dummy storage is needed to guarantee aligned (4kb) writes on DirectIoFile.
   * (we can also do it without dummy blocks by retaining the "fragment" until the next
   * storage block, but the complexity isn't worth it. 4kb for each storage? nothing.)
   * This object MUST be 8 bytes so that it can fill any gap (all log entries are 8-byte aligned).
   * The magic word for this is kStorageHeaderFillerMagicWord.
   */
  struct DumpStorageHeaderFiller : public DumpStorageHeaderBase {};

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
   * Context object used throughout merge_sort().
   */
  struct MergeContext {
    explicit MergeContext(uint32_t sorted_buffer_count);
    ~MergeContext();

    const uint32_t                            sorted_buffer_count_;
    memory::AlignedMemory                     io_memory_;
    std::vector< memory::AlignedMemorySlice > io_buffers_;

    /**
     * @brief stream objects that keep reading storage blocks.
     * @details
     * The first one is always the InMemorySortedBuffer (based on last_buffer_).
     * Others are DumpFileSortedBuffer for the sorted run files.
     * Dummy block is automatically skipped.
     * If storage_id_ is zero, it means that the stream reached the end.
     */
    std::vector< std::unique_ptr<SortedBuffer> >  sorted_buffers_;

    /**
     * Just to automatically close/delete them.
     * Index is sorted_buffers_'s - 1, but anyway we never explicitly access this.
     */
    std::vector< std::unique_ptr<fs::DirectIoFile> > sorted_files_auto_ptrs_;

    SortedBuffer**                          tmp_sorted_buffer_array_;
    uint32_t                                tmp_sorted_buffer_count_;

    /**
     * Returns the minimum storage_id the sorted buffers are currently at.
     * Iff all sorted buffers reached the end, returns 0.
     */
    storage::StorageId  get_min_storage_id() const;
    void                set_tmp_sorted_buffer_array(storage::StorageId storage_id);
  };

  /**
   * Writes out composed snapshot pages to a new snapshot file.
   */
  SnapshotWriter          snapshot_writer_;
  /**
   * To read previous snapshot versions.
   */
  cache::SnapshotFileSet  previous_snapshot_files_;

  /**
   * Underlying memory of reducer buffer.
   */
  memory::AlignedMemory   buffer_memory_;

  /**
   * Buffer for writing out a sorted run.
   */
  memory::AlignedMemory   dump_io_buffer_;

  /**
   * Used to sort log entries in each storage.
   * This is automatically extended when needed.
   */
  memory::AlignedMemory   sort_buffer_;

  /**
   * Used to store information output from composers to construct root pages.
   * 4kb * storages. This is automatically extended when needed.
   */
  memory::AlignedMemory   root_info_buffer_;

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
   * Temporary work memory for composers during merge_sort().
   * Automatically expanded if needed.
   */
  memory::AlignedMemory   composer_work_memory_;

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
   * Set at the end of merge_sort().
   * Total number of storages this reducer has merged and composed.
   * This is also the number of root-info pages this reducer has produced.
   */
  uint32_t      total_storage_count_;

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

  ReducerBuffer* get_non_current_buffer() { return &buffers_[(current_buffer_ + 1) % 2]; }
  ReducerBuffer* get_current_buffer() { return &buffers_[current_buffer_ % 2]; }
  const ReducerBuffer* get_non_current_buffer() const {
    return &buffers_[(current_buffer_ + 1) % 2];
  }
  const ReducerBuffer* get_current_buffer() const {
    return &buffers_[current_buffer_ % 2];
  }

  void expand_if_needed(
    uint64_t required_size,
    memory::AlignedMemory *memory,
    const std::string& name);
  void expand_sort_buffer_if_needed(uint64_t required_size) {
    expand_if_needed(required_size, &sort_buffer_, "sort_buffer_");
  }
  void expand_composer_work_memory_if_needed(uint64_t required_size) {
    expand_if_needed(required_size, &composer_work_memory_, "composer_work_memory_");
  }
  void expand_root_info_buffer_if_needed(uint64_t required_size) {
    expand_if_needed(required_size, &root_info_buffer_, "root_info_buffer_");
  }
  /** This one is a bit special. */
  void expand_positions_buffers_if_needed(uint64_t required_size_per_buffer);

  fs::Path get_sorted_run_file_path(uint32_t sorted_run) const;

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

  /**
   * Third sub routine of dump_buffer().
   * For the specified storage, sort all log entries in key-and-ordinal order, then dump
   * them to the file.
   */
  ErrorStack dump_buffer_sort_storage(
    const LogBuffer &buffer,
    storage::StorageId storage_id,
    const std::vector<BufferPosition>& log_positions,
    fs::DirectIoFile *dump_file);

  /** Sub routine of dump_buffer_sort_storage to write the sorted logs to the file. */
  ErrorStack dump_buffer_sort_storage_write(
    const LogBuffer &buffer,
    storage::StorageId storage_id,
    const BufferPosition* sorted_logs,
    uint32_t log_count,
    fs::DirectIoFile *dump_file);

  /**
   * @brief Called at the end of the reducer to construct a snapshot file
   * from the dumped buffers and in-memory buffer.
   * @pre at most one of the buffers are in-use (non-current buffer's tail_position==0)
   * @pre all mappers completed (thus both buffers active_writers==0 and won't change)
   * @details
   * This invokes a foedus::storage::Composer for each storage, giving sorted buffers as inputs.
   * The result is just one snapshot file, which is written by SnapshotWriter.
   */
  ErrorStack  merge_sort();

  /** just sanity checks. */
  void        merge_sort_check_buffer_status() const;

  /**
   * First sub routine of merge_sort() which allocates I/O buffers to read from sorted run files.
   */
  void        merge_sort_allocate_io_buffers(MergeContext* context) const;
  /**
   * Second sub routine that opens the files with the I/O buffers.
   */
  ErrorStack  merge_sort_open_sorted_runs(MergeContext* context) const;
  /**
   * Initial reading and locating first storage blocks in each buffer.
   */
  ErrorStack  merge_sort_initialize_sort_buffers(MergeContext* context) const;
  /**
   * After processing each storage, merge_sort() calls this method to advance each input stream
   * that had contained the processed storage to next storage block.
   * This method assumes that, the streams are at next block header because they fully read
   * previous blocks.
   */
  ErrorCode   merge_sort_advance_sort_buffers(
    SortedBuffer* buffer,
    storage::StorageId processed_storage_id) const;
};
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_REDUCER_IMPL_HPP_
