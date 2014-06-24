/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_LOG_MAPPER_IMPL_HPP_
#define FOEDUS_SNAPSHOT_LOG_MAPPER_IMPL_HPP_
#include <stdint.h>

#include <cstring>
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
  LogMapper(Engine* engine, LogGleaner* parent, log::LoggerId id, thread::ThreadGroupId numa_node)
    : MapReduceBase(engine, parent, id, numa_node),
      buckets_allocated_count_(0), buckets_all_count_(0), processed_log_count_(0) {
    std::memset(storage_buckets_, 0, sizeof(storage_buckets_));
  }

  /**
   * Unique ID of this log mapper. One log mapper corresponds to one logger, so this ID is also
   * the corresponding logger's ID (log::LoggerId).
   */
  log::LoggerId           get_id() const { return id_; }
  std::string             to_string() const override {
    return std::string("LogMapper-") + std::to_string(id_);
  }
  friend std::ostream&    operator<<(std::ostream& o, const LogMapper& v);

 protected:
  ErrorStack  handle_initialize() override;
  ErrorStack  handle_uninitialize() override;
  ErrorStack  handle_process() override;

  void        pre_handle_uninitialize() override;

 private:
  /**
   * Represents a position in IO buffer.
   * As log is always 8-byte aligned, we divide the original byte position by 8.
   * Thus, this can represent up to 8 * 2^32=32GB, which is the maximum value of
   * log_mapper_io_buffer_mb_.
   */
  typedef uint32_t MapperBufferPosition;

  /**
   * Stores bunch of byte positions in IO buffer to one storage.
   */
  struct Bucket final {
    enum Constsants {
      kBucketSize = 1 << 16,
      kMaxCount = (kBucketSize - 16) / 4,
    };
    inline bool is_full() const ALWAYS_INLINE { return counts_ < kMaxCount; }

    /** This bucket stores log positions for this storage. */
    storage::StorageId    storage_id_;   // +4 => 4
    /** Number of active log positions stored. */
    uint32_t              counts_;       // +4 => 8
    /** A storage can have more than one bucket, thus it forms a singly linked list. */
    Bucket*               next_bucket_;  // +8 => 16
    /** Byte positions in IO buffer. */
    MapperBufferPosition  log_positions_[kMaxCount];  // + 4 * kMaxCount => kBucketSize
  };
  STATIC_SIZE_CHECK(sizeof(Bucket), Bucket::kBucketSize)

  /**
   * Entry in the hashtable for storage bucketing.
   * This object is new/deleted only when we have non-null hashtable_next_, in other words
   * a hash bucket has more than one storage.
   */
  struct BucketLinkedList {
    storage::StorageId  storage_id_;
    Bucket*             head_;
    Bucket*             tail_;
    BucketLinkedList*   hashtable_next_;
  };

  /** buffer to read from file. */
  memory::AlignedMemory   io_buffer_;

  /** memory for Bucket. */
  memory::AlignedMemory   buckets_memory_;

  /** How many Bucket allocated. This is zero-cleared when the I/O buffer is fully processed. */
  uint32_t                buckets_allocated_count_;

  /** How many Bucket we can allocate in total. */
  uint32_t                buckets_all_count_;

  /** just for reporting. */
  uint64_t                processed_log_count_;

  /**
   * A stupidly simple hashtable for BucketLinkedList.
   * Key is StorageId. 256 entries for the last 1 byte of StorageId (StorageId & 0xFF).
   * In each entry, we sequentially look for the storage ID.
   * We don't want to do expensive hash calculation for each log entry, so this is the
   * fastest way to do this unless each IO buffer contains lots of storage logs mixed.
   * This hashtable is zero-cleared when the I/O buffer is fully processed.
   * @see clear_storage_buckets()
   */
  BucketLinkedList        storage_buckets_[256];

  /**
   * Process one I/O buffer, which is the unit of batching in mapper.
   */
  ErrorStack  handle_process_buffer(const fs::DirectIoFile &file, uint64_t buffered_bytes,
                      log::LogFileOrdinal cur_file_ordinal, uint64_t *cur_offset, bool *first_read);

  ErrorCode   map_log(const log::LogHeader* header);

  /**
   * Zero-clear storage_buckets_() and delete new-ed BucketLinkedList.
   * Do not use this from constructor because the initial content of the memory might be garbage.
   */
  void        clear_storage_buckets();

  /**
   * Add the given log position to a bucket for the specified storage.
   * This method must be VERY fast as it's called for every log entry.
   * @param[in] storage_id storage ID of the log
   * @param[in] log_position byte position of the log
   * @return whether we added the log to an existing non-full bucket.
   * You can assume almost all cases this returns true (for compiler hint).
   * When this returns false, it should be followed by add_new_bucket()
   */
  bool        bucket_log(storage::StorageId storage_id, MapperBufferPosition log_position)
    ALWAYS_INLINE;

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
  ErrorStack  flush_buckets();

  inline static MapperBufferPosition to_mapper_buffer_position(uint64_t byte_position) {
    ASSERT_ND(byte_position % 8 == 0);
    return byte_position >> 3;
  }
  inline static uint64_t from_mapper_buffer_position(MapperBufferPosition mapper_buffer_position) {
    return static_cast<uint64_t>(mapper_buffer_position) << 3;
  }
};


}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_LOG_MAPPER_IMPL_HPP_
