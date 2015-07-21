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
#ifndef FOEDUS_SNAPSHOT_LOG_MAPPER_IMPL_HPP_
#define FOEDUS_SNAPSHOT_LOG_MAPPER_IMPL_HPP_
#include <stdint.h>

#include <iosfwd>
#include <string>

#include "foedus/compiler.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/fs/fwd.hpp"
#include "foedus/log/fwd.hpp"
#include "foedus/log/log_id.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/mapreduce_base_impl.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/stoppable_thread_impl.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace snapshot {
/**
 * @brief A log mapper, which reads log files from one logger and
 * sends them to corresponding log reducers.
 * @ingroup SNAPSHOT
 * @details
 * @section MAPPER_OVERVIEW Overview
 * Mappers read logs per epoch.
 * As log files are guaranteed to be strictly ordered by epoch (see Logger code), we can simply
 * read log files sequentially to achieve this.
 *
 * Mappers send logs to partitions as follows:
 *  \li Engine-wide and Storage-wide logs (eg DROP STORAGE) are centrally processed at the end of
 * epoch. So, mappers just buffer them and send all of them back to LogGleaner, which will process
 * all of them.
 *  \li Record-wise logs always have storage-id. Mappers bucketize logs by storage ID to
 * do the following in a batched fashion.
 *  \li For each storage batch, mappers check the partitioning information, creating one if not
 * exists (see LogGleaner).
 *  \li Mappers send logs to corresponding reducers with a compact metadata for each storage.
 *
 * @section MAPPER_OPTIMIZATION Possible Optimization
 * The log gleaner so far simply reads from log files.
 * We have a plan to optimize its behavior when we have a large amount of DRAM by directly reading
 * from the log buffer if it is not blown away yet.
 * ThreadLogBuffer has an additional marker "head" for this purpose, but so far we don't use it
 * to simplify the implementation.
 *
 * @note
 * This is a private implementation-details of \ref SNAPSHOT, thus file name ends with _impl.
 * Do not include this header from a client program. There is no case client program needs to
 * access this internal class.
 */
class LogMapper final : public MapReduceBase {
 public:
  LogMapper(Engine* engine, uint16_t local_ordinal);

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  std::string             to_string() const override {
    return std::string("LogMapper-") + std::to_string(id_);
  }
  friend std::ostream&    operator<<(std::ostream& o, const LogMapper& v);

 protected:
  ErrorStack  handle_process() override;

 private:
  enum Constsants {
    kBucketSize = 1 << 16,
    kBucketMaxCount = (kBucketSize - 16) / 4,
    /**
     * How many distinct StorageId one batch (I/O buffer) is expected to have at most.
     * If it exceeds this number, we have to flush buckets before fully processing the batch.
     * It's very unlikely, though.
     */
    kBucketHashListMaxCount = 1 << 12,
    /**
     * Size of temporary buffer to stitch log entries in one storage and in one partition.
     * We stitch them in our memory not in reducer's memory even when the two are in same
     * NUMA node. We have to anyway calculate the total byte length of what will be sent.
     * Otherwise we need atomic operation at reducer's memory for every log entry to send!
     */
    kSendBufferSize = 1 << 20,
  };

  /**
   * Stores bunch of byte positions in IO buffer to one storage.
   */
  struct Bucket final {
    inline bool is_full() const ALWAYS_INLINE { return counts_ >= kBucketMaxCount; }

    /** This bucket stores log positions for this storage. */
    storage::StorageId  storage_id_;   // +4 => 4
    /** Number of active log positions stored. */
    uint32_t            counts_;       // +4 => 8
    /** A storage can have more than one bucket, thus it forms a singly linked list. */
    Bucket*             next_bucket_;  // +8 => 16
    /** Byte positions in IO buffer. */
    BufferPosition      log_positions_[kBucketMaxCount];  // + 4 * kBucketMaxCount => kBucketSize
  };
  STATIC_SIZE_CHECK(sizeof(Bucket), kBucketSize)

  /**
   * Entry in the hashtable for storage bucketing, corresponding to one storage.
   * It is a singly linked list of Bucket.
   * @par Why head_ and tail_
   * We don't have to store tail by just inserting new buckets at head, but then iteration
   * will be in back-order. By inserting to tail_, iteration from head to tail guarantees
   * log-entry order. We might rely on it later. It's anyway just 8 bytes per list.
   */
  struct BucketHashList {
    storage::StorageId  storage_id_;  // +4 => 4
    uint16_t            bucket_counts_;  // +2 => 6
    uint16_t            dummy_;       // +2 => 8
    Bucket*             head_;        // +8 => 16
    Bucket*             tail_;        // +8 => 24
    BucketHashList*     hashlist_next_;  // +8 => 32
  };

  /**
   * Used to sort log entries by partition.
   * @todo we don't really have to sorting, and also the sort key here is just 1 byte.
   * we should rather pre-alocate a sufficient memory for 256 buckets and just
   * append to them, flushing when it gets full. by doing this, we can also batch-process
   * multiple Bucket efficiently.
   */
  struct PartitionSortEntry {
    inline void set(storage::PartitionId partition, BufferPosition position) ALWAYS_INLINE {
      partition_ = partition;
      position_ = position;
    }

    inline bool operator<(const PartitionSortEntry& other) const ALWAYS_INLINE {
      return partition_ < other.partition_;
    }
    inline bool operator<=(const PartitionSortEntry& other) const ALWAYS_INLINE {
      return partition_ <= other.partition_;
    }
    inline bool operator==(const PartitionSortEntry& other) const ALWAYS_INLINE {
      return partition_ == other.partition_;
    }
    inline bool operator!=(const PartitionSortEntry& other) const ALWAYS_INLINE {
      return partition_ != other.partition_;
    }
    inline bool operator>(const PartitionSortEntry& other) const ALWAYS_INLINE {
      return partition_ > other.partition_;
    }
    inline bool operator>=(const PartitionSortEntry& other) const ALWAYS_INLINE {
      return partition_ >= other.partition_;
    }

    uint16_t              filler1_;  // +2 => 2
    uint8_t               filler2_;  // +1 => 3
    storage::PartitionId  partition_;  // +1 => 4
    BufferPosition        position_;  // +4 => 8
  };

  struct IoBufStatus {
    uint64_t size_inbuf_aligned_;
    uint64_t size_infile_aligned_;

    uint64_t next_infile_;
    uint64_t buf_infile_aligned_;
    uint64_t cur_inbuf_;
    uint64_t end_inbuf_aligned_;
    uint64_t end_infile_;
    bool more_in_the_file_;
    bool first_read_;
    bool ended_;
    log::LogFileOrdinal cur_file_ordinal_;

    uint64_t to_infile(uint64_t inbuf) const { return inbuf + buf_infile_aligned_; }
  };

  /** buffer to read from file. */
  memory::AlignedMemory   io_buffer_;

  /** memory for Bucket. */
  memory::AlignedMemory   buckets_memory_;

  /**
   * used for various temporary variables that are big and have to be very fast.
   * these include pointer array and partition array passed around from/to partitioner.
   * All of these variables are cleared after each handle_process_buffer(), thus "tmp".
   * Currently the size is always 2MB so that it uses hugepage.
   * kBucketSize is 1 << 16 (64k), so in total a few hundred KB.
   * kBucketHashListMaxCount is 1 << 12, adding another hundred KB.
   * In sum they should be within 1MB. Plus the 1MB send buffer. Thus within 2MB (hugepage).
   */
  memory::AlignedMemory   tmp_memory_;

  /** temporary memory for pre-sort. automatically expands. */
  memory::AlignedMemory   presort_buffer_;
  /** same above. for output positions. */
  memory::AlignedMemory   presort_ouputs_;
  /** same above. for re-ordering logs using the ouputs. */
  memory::AlignedMemory   presort_reordered_;

  /**
   * Slice of tmp_memory_ used as send buffer.
   * Size is kSendBufferSize (1MB).
   */
  memory::AlignedMemorySlice  tmp_send_buffer_slice_;

  /**
   * Slice of tmp_memory_ used as position array (BufferPosition[]).
   * Size is kBucketSize bytes (= (kBucketSize / 4) positions).
   */
  memory::AlignedMemorySlice  tmp_position_array_slice_;

  /**
   * Slice of tmp_memory_ used as sort array (PartitionSortEntry[]).
   * Size is kBucketSize * 2 bytes.
   */
  memory::AlignedMemorySlice  tmp_sort_array_slice_;

  /**
   * Slice of tmp_memory_ used as BucketHashList memory (BucketHashList[]).
   * Size is kBucketHashListMaxCount * sizeof(BucketHashList) bytes.
   */
  memory::AlignedMemorySlice  tmp_hashlist_buffer_slice_;

  /**
   * Slice of tmp_memory_ used as partition array (storage::PartitionId[]).
   * Size is kBucketSize bytes (as so far sizeof(storage::PartitionId) == 1, this is too much,
   * but not a big issue.).
   */
  memory::AlignedMemorySlice  tmp_partition_array_slice_;

  /** How many Bucket allocated. This is zero-cleared when the I/O buffer is fully processed. */
  uint32_t                buckets_allocated_count_;

  /**
   * How many BucketHashList allocated.
   * @invariant hashlist_allocated_count_ <= kBucketHashListMaxCount
   */
  uint32_t                hashlist_allocated_count_;

  /** just for reporting. */
  uint64_t                processed_log_count_;

  /**
   * A stupidly simple hashtable for BucketHashList.
   * Key is StorageId. 256 entries for the last 1 byte of StorageId (StorageId & 0xFF).
   * In each entry, we sequentially look for the storage ID.
   * We don't want to do expensive hash calculation for each log entry, so this is the
   * fastest way to do this unless each IO buffer contains lots of storage logs mixed.
   * This hashtable is zero-cleared when the I/O buffer is fully processed.
   * @see clear_storage_buckets()
   */
  BucketHashList*       storage_hashlists_[256];

  /**
   * Process one I/O buffer, which is the unit of batching in mapper.
   */
  ErrorStack  handle_process_buffer(const fs::DirectIoFile &file, IoBufStatus* status);

  /**
   * Add the given log position to a bucket for the specified storage.
   * This method must be VERY fast as it's called for every log entry.
   * @param[in] storage_id storage ID of the log
   * @param[in] log_position byte position of the log
   * @return whether we added the log to an existing non-full bucket.
   * You can assume almost all cases this returns true (for compiler hint).
   * When this returns false, it should be followed by add_new_bucket()
   */
  bool        bucket_log(storage::StorageId storage_id, uint64_t pos) ALWAYS_INLINE;

  /**
   * Add a new bucket for the specified storage.
   * This method is only occasionally called.
   * @param[in] storage_id storage ID of the log
   * @return Whether we could create a new bucket for this. As a VERY unlikely event,
   * this might return false when buckets_memory_ runs out.
   * If that happens, you must call flush_buckets() to send out all log entries to reducers.
   * It shouldn't happen often for better performance.
   */
  bool        add_new_bucket(storage::StorageId storage_id);

  /**
   * Send out all the bucketized log entries to reducers.
   * This can be called either at the end of handle_process_buffer() or in the middle.
   * For better performance, it should be the former.
   */
  void        flush_all_buckets();

  /**
   * Send out one storage's bucketized log entries to reducers.
   * Called from flush_all_buckets().
   */
  void        flush_bucket(const BucketHashList& hashlist);

  /**
   * Send out all logs in the bucket to the given partition.
   */
  void        send_bucket_partition(Bucket* bucket, storage::PartitionId partition);
  void send_bucket_partition_general(
    const Bucket* bucket,
    storage::StorageType storage_type,
    storage::PartitionId partition,
    const BufferPosition* positions);
  void        send_bucket_partition_presort(
    Bucket* bucket,
    storage::StorageType storage_type,
    storage::PartitionId partition);
  /** subroutine of send_bucket_partition to send out a send-buffer. */
  void        send_bucket_partition_buffer(
    const Bucket* bucket,
    storage::PartitionId partition,
    const char* send_buffer,
    uint32_t log_count,
    uint64_t written,
    uint32_t shortest_key_length,
    uint32_t longest_key_length);

  /**
   * Zero-clears storage_hashlists_ and resets other related temporary variables.
   */
  void        clear_storage_buckets();

  /**
   * Return BucketHashList for the given storage ID.
   * NULL if not found.
   * This method is optimized for the case where the first search with last 1 byte is sufficient.
   */
  inline BucketHashList* find_storage_hashlist(storage::StorageId storage_id) ALWAYS_INLINE {
    uint8_t index = static_cast<uint8_t>(storage_id);
    BucketHashList* hashlist = storage_hashlists_[index];
    if (UNLIKELY(hashlist == nullptr)) {
      return nullptr;
    } else {
      while (UNLIKELY(hashlist->storage_id_ != storage_id)) {
        hashlist = hashlist->hashlist_next_;
        if (hashlist == nullptr) {
          return nullptr;
        }
      }
    }

    return hashlist;
  }
  /** Insert the new BucketHashList. This shouldn't be called often. */
  void add_storage_hashlist(BucketHashList* new_hashlist);

  void report_completion(double elapsed_sec);
};


}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_MAPPER_IMPL_HPP_
