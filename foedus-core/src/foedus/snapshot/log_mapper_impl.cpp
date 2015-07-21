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
#include "foedus/snapshot/log_mapper_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <ostream>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/logger_impl.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/snapshot/log_gleaner_impl.hpp"
#include "foedus/snapshot/log_reducer_impl.hpp"
#include "foedus/snapshot/snapshot.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"

namespace foedus {
namespace snapshot {

/**
 * Unique ID of this log mapper. One log mapper corresponds to one logger, so this ID is also
 * the corresponding logger's ID (log::LoggerId).
 */
uint16_t calculate_logger_id(Engine* engine, uint16_t local_ordinal) {
  return engine->get_options().log_.loggers_per_node_ * engine->get_soc_id() + local_ordinal;
}

LogMapper::LogMapper(Engine* engine, uint16_t local_ordinal)
  : MapReduceBase(engine, calculate_logger_id(engine, local_ordinal)),
    processed_log_count_(0) {
  clear_storage_buckets();
}

ErrorStack LogMapper::initialize_once() {
  const SnapshotOptions& option = engine_->get_options().snapshot_;

  uint64_t io_buffer_size = static_cast<uint64_t>(option.log_mapper_io_buffer_mb_) << 20;
  io_buffer_size = assorted::align<uint64_t, memory::kHugepageSize>(io_buffer_size);
  io_buffer_.alloc(
    io_buffer_size,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);
  ASSERT_ND(!io_buffer_.is_null());

  uint64_t bucket_size = static_cast<uint64_t>(option.log_mapper_bucket_kb_) << 10;
  buckets_memory_.alloc(
    bucket_size,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);
  ASSERT_ND(!buckets_memory_.is_null());

  const uint64_t tmp_memory_size = memory::kHugepageSize;
  tmp_memory_.alloc(
    tmp_memory_size,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);
  ASSERT_ND(!tmp_memory_.is_null());

  // these automatically expand
  presort_buffer_.alloc(
    1U << 21,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);
  presort_ouputs_.alloc(
    1U << 21,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);

  uint64_t tmp_offset = 0;
  tmp_send_buffer_slice_ = memory::AlignedMemorySlice(&tmp_memory_, tmp_offset, kSendBufferSize);
  tmp_offset += kSendBufferSize;
  tmp_position_array_slice_ = memory::AlignedMemorySlice(&tmp_memory_, tmp_offset, kBucketSize);
  tmp_offset += kBucketSize;
  tmp_sort_array_slice_ = memory::AlignedMemorySlice(&tmp_memory_, tmp_offset, kBucketSize << 1);
  tmp_offset += kBucketSize << 1;
  const uint64_t hashlist_bytesize = kBucketHashListMaxCount * sizeof(BucketHashList);
  tmp_hashlist_buffer_slice_ = memory::AlignedMemorySlice(
    &tmp_memory_, tmp_offset, hashlist_bytesize);
  tmp_offset += hashlist_bytesize;
  tmp_partition_array_slice_ = memory::AlignedMemorySlice(&tmp_memory_, tmp_offset, kBucketSize);
  tmp_offset += kBucketSize;
  if (tmp_memory_size < tmp_offset) {
    LOG(FATAL) << "tmp_memory_size is too small. some contant values are messed up";
  }

  processed_log_count_ = 0;
  clear_storage_buckets();

  return kRetOk;
}

ErrorStack LogMapper::uninitialize_once() {
  ErrorStackBatch batch;
  io_buffer_.release_block();
  buckets_memory_.release_block();
  tmp_memory_.release_block();
  clear_storage_buckets();
  return SUMMARIZE_ERROR_BATCH(batch);
}

const uint64_t kIoAlignment = 0x1000;
uint64_t align_io_floor(uint64_t offset) { return (offset / kIoAlignment) * kIoAlignment; }
uint64_t align_io_ceil(uint64_t offset) { return align_io_floor(offset + kIoAlignment - 1U); }

ErrorStack LogMapper::handle_process() {
  const Epoch base_epoch = parent_.get_base_epoch();
  const Epoch until_epoch = parent_.get_valid_until_epoch();
  log::LoggerRef logger = engine_->get_log_manager()->get_logger(id_);
  const log::LogRange log_range = logger.get_log_range(base_epoch, until_epoch);
  // uint64_t cur_offset = log_range.begin_offset;
  if (log_range.is_empty()) {
    LOG(INFO) << to_string() << " has no logs to process";
    report_completion(0);
    return kRetOk;
  }

  // open the file and seek to there. be careful on page boundary.
  // as we use direct I/O, all I/O must be 4kb-aligned. when the read range is not
  // a multiply of 4kb, we read a little bit more (at most 4kb per read, so negligible).
  // to clarify, here we use the following suffixes
  //   "infile"/"inbuf" : the offset is an offset in entire file/IO buffer
  //   "aligned" : the offset is 4kb-aligned (careful on floor vs ceil)
  // Lengthy, but otherwise it's so confusing.
  processed_log_count_ = 0;
  IoBufStatus status;
  status.size_inbuf_aligned_ = io_buffer_.get_size();
  status.cur_file_ordinal_ = log_range.begin_file_ordinal;
  status.ended_ = false;
  status.first_read_ = true;
  debugging::StopWatch watch;
  while (!status.ended_) {  // loop for log file switch
    fs::Path path(engine_->get_options().log_.construct_suffixed_log_path(
      numa_node_,
      id_,
      status.cur_file_ordinal_));
    uint64_t file_size = fs::file_size(path);
    if (file_size % kIoAlignment != 0) {
      LOG(WARNING) << to_string() << " Interesting, non-aligned file size, which probably means"
        << " previous writes didn't flush. file path=" << path << ", file size=" << file_size;
      file_size = align_io_floor(file_size);
    }
    ASSERT_ND(file_size % kIoAlignment == 0);
    status.size_infile_aligned_ = file_size;

    // If this is the first file to read, we might be reading from non-zero position.
    // In that case, be careful on alignment.
    if (status.cur_file_ordinal_ == log_range.begin_file_ordinal) {
      status.next_infile_ = log_range.begin_offset;
    } else {
      status.next_infile_ = 0;
    }

    if (status.cur_file_ordinal_ == log_range.end_file_ordinal) {
      ASSERT_ND(log_range.end_offset <= file_size);
      status.end_infile_ = log_range.end_offset;
    } else {
      status.end_infile_ = file_size;
    }

    DVLOG(1) << to_string() << " file path=" << path << ", file size=" << assorted::Hex(file_size)
      << ", read_end=" << assorted::Hex(status.end_infile_);
    fs::DirectIoFile file(path, engine_->get_options().snapshot_.emulation_);
    WRAP_ERROR_CODE(file.open(true, false, false, false));
    DVLOG(1) << to_string() << "opened log file " << file;

    while (true) {
      WRAP_ERROR_CODE(check_cancelled());  // check per each read
      status.buf_infile_aligned_ = align_io_floor(status.next_infile_);
      WRAP_ERROR_CODE(file.seek(status.buf_infile_aligned_, fs::DirectIoFile::kDirectIoSeekSet));
      DVLOG(1) << to_string() << " seeked to: " << assorted::Hex(status.buf_infile_aligned_);
      status.end_inbuf_aligned_ = std::min(
        io_buffer_.get_size(),
        align_io_ceil(status.end_infile_ - status.buf_infile_aligned_));
      ASSERT_ND(status.end_inbuf_aligned_ % kIoAlignment == 0);
      WRAP_ERROR_CODE(file.read(status.end_inbuf_aligned_, &io_buffer_));

      status.cur_inbuf_ = 0;
      if (status.next_infile_ != status.buf_infile_aligned_) {
        ASSERT_ND(status.next_infile_ > status.buf_infile_aligned_);
        status.cur_inbuf_ = status.next_infile_ - status.buf_infile_aligned_;
        status.cur_inbuf_ = status.next_infile_ - status.buf_infile_aligned_;
        DVLOG(1) << to_string() << " skipped " << status.cur_inbuf_ << " bytes for aligned read";
      }

      CHECK_ERROR(handle_process_buffer(file, &status));
      if (status.more_in_the_file_) {
        ASSERT_ND(status.next_infile_ > status.buf_infile_aligned_);
      } else {
        if (log_range.end_file_ordinal == status.cur_file_ordinal_) {
          status.ended_ = true;
          break;
        } else {
          ++status.cur_file_ordinal_;
          status.next_infile_ = 0;
          LOG(INFO) << to_string()
            << " moved on to next log file ordinal " << status.cur_file_ordinal_;
        }
      }
    }
    file.close();
  }
  watch.stop();
  LOG(INFO) << to_string() << " processed " << processed_log_count_ << " log entries in "
    << watch.elapsed_sec() << "s";
  report_completion(watch.elapsed_sec());
  return kRetOk;
}
void LogMapper::report_completion(double elapsed_sec) {
  uint16_t value_after = parent_.increment_completed_mapper_count();
  if (value_after == parent_.get_mappers_count()) {
    LOG(INFO) << "All mappers done. " << to_string() << " was the last mapper. took "
      << elapsed_sec << "s";
  }
}

ErrorStack LogMapper::handle_process_buffer(const fs::DirectIoFile &file, IoBufStatus* status) {
  const Epoch base_epoch = parent_.get_base_epoch();  // only for assertions
  const Epoch until_epoch = parent_.get_valid_until_epoch();  // only for assertions

  // many temporary memory are used only within this method and completely cleared out
  // for every call.
  clear_storage_buckets();

  char* buffer = reinterpret_cast<char*>(io_buffer_.get_block());
  status->more_in_the_file_ = false;
  for (; status->cur_inbuf_ < status->end_inbuf_aligned_; ++processed_log_count_) {
    // Note: The loop here must be a VERY tight loop, iterated over every single log entry!
    // In most cases, we should be just calling bucket_log().
    const log::LogHeader* header
      = reinterpret_cast<const log::LogHeader*>(buffer + status->cur_inbuf_);
    ASSERT_ND(header->log_length_ > 0);
    ASSERT_ND(status->buf_infile_aligned_ != 0 || status->cur_inbuf_ != 0
      || header->get_type() == log::kLogCodeEpochMarker);  // file starts with marker
    // we must be starting from epoch marker.
    ASSERT_ND(!status->first_read_ || header->get_type() == log::kLogCodeEpochMarker);
    ASSERT_ND(header->get_kind() == log::kRecordLogs
      || header->get_type() == log::kLogCodeEpochMarker
      || header->get_type() == log::kLogCodeFiller);

    if (UNLIKELY(header->log_length_ + status->cur_inbuf_ > status->end_inbuf_aligned_)) {
      // if a log goes beyond this read, stop processing here and read from that offset again.
      // this is simpler than glue-ing the fragment. This happens just once per 64MB read,
      // so not a big waste.
      if (status->to_infile(status->cur_inbuf_ + header->log_length_)
          > status->size_infile_aligned_) {
        // but it never spans two files. something is wrong.
        LOG(ERROR) << "inconsistent end of log entry. offset="
          << status->to_infile(status->cur_inbuf_)
          << ", file=" << file << ", log header=" << *header;
        return ERROR_STACK_MSG(kErrorCodeSnapshotInvalidLogEnd, file.get_path().c_str());
      }
      status->next_infile_ = status->to_infile(status->cur_inbuf_);
      status->more_in_the_file_ = true;
      break;
    } else if (UNLIKELY(header->get_type() == log::kLogCodeEpochMarker)) {
      // skip epoch marker
      const log::EpochMarkerLogType *marker =
        reinterpret_cast<const log::EpochMarkerLogType*>(header);
      ASSERT_ND(header->log_length_ == sizeof(log::EpochMarkerLogType));
      ASSERT_ND(marker->log_file_ordinal_ == status->cur_file_ordinal_);
      ASSERT_ND(marker->log_file_offset_ == status->to_infile(status->cur_inbuf_));
      ASSERT_ND(marker->new_epoch_ >= marker->old_epoch_);
      ASSERT_ND(!base_epoch.is_valid() || marker->new_epoch_ >= base_epoch);
      ASSERT_ND(marker->new_epoch_ <= until_epoch);
      if (status->first_read_) {
        ASSERT_ND(!base_epoch.is_valid()
          || marker->old_epoch_ <= base_epoch  // otherwise we skipped some logs
          || marker->old_epoch_ == marker->new_epoch_);  // the first marker (old==new) is ok
        status->first_read_ = false;
      } else {
        ASSERT_ND(!base_epoch.is_valid() || marker->old_epoch_ >= base_epoch);
      }
    } else if (UNLIKELY(header->get_type() == log::kLogCodeFiller)) {
      // skip filler log
    } else {
      bool bucketed = bucket_log(header->storage_id_, status->cur_inbuf_);
      if (UNLIKELY(!bucketed)) {
        // need to add a new bucket
        bool added = add_new_bucket(header->storage_id_);
        if (added) {
          bucketed = bucket_log(header->storage_id_, status->cur_inbuf_);
          ASSERT_ND(bucketed);
        } else {
          // runs out of bucket_memory. have to flush now.
          flush_all_buckets();
          added = add_new_bucket(header->storage_id_);
          ASSERT_ND(added);
          bucketed = bucket_log(header->storage_id_, status->cur_inbuf_);
          ASSERT_ND(bucketed);
        }
      }
    }

    status->cur_inbuf_ += header->log_length_;
  }

  // bucktized all logs. now let's send them out to reducers
  flush_all_buckets();
  return kRetOk;
}

inline bool LogMapper::bucket_log(storage::StorageId storage_id, uint64_t pos) {
  BucketHashList* hashlist = find_storage_hashlist(storage_id);
  if (UNLIKELY(hashlist == nullptr)) {
    return false;
  }

  if (LIKELY(!hashlist->tail_->is_full())) {
    // 99.99% cases we are hitting here straight out of the tight loop.
    // hope the hints guide the compiler well.
    BufferPosition log_position = to_buffer_position(pos);
    hashlist->tail_->log_positions_[hashlist->tail_->counts_] = log_position;
    ++hashlist->tail_->counts_;
    return true;
  } else {
    // found it, but we need to add a new bucket for this storage.
    return false;
  }
}

bool LogMapper::add_new_bucket(storage::StorageId storage_id) {
  if (buckets_allocated_count_ >= buckets_memory_.get_size() / sizeof(Bucket)) {
    // we allocated all buckets_memory_! we have to flush the buckets now.
    // this shouldn't happen often.
    LOG(WARNING) << to_string() << " ran out of buckets_memory_, so it has to flush buckets before"
      " processing one IO buffer. This shouldn't happen often. check your log_mapper_bucket_kb_"
      " setting. this=" << *this;
    return false;
  }

  Bucket* base_address = reinterpret_cast<Bucket*>(buckets_memory_.get_block());
  Bucket* new_bucket = base_address + buckets_allocated_count_;
  ++buckets_allocated_count_;
  new_bucket->storage_id_ = storage_id;
  new_bucket->counts_ = 0;
  new_bucket->next_bucket_ = nullptr;

  BucketHashList* hashlist = find_storage_hashlist(storage_id);
  if (hashlist) {
    // just add this as a new tail
    ASSERT_ND(hashlist->storage_id_ == storage_id);
    ASSERT_ND(hashlist->tail_->is_full());
    hashlist->tail_->next_bucket_ = new_bucket;
    hashlist->tail_ = new_bucket;
    ++hashlist->bucket_counts_;
  } else {
    // we don't even have a linked list for this.
    // If this happens often, maybe we should have 65536 hash buckets...
    if (hashlist_allocated_count_ >= kBucketHashListMaxCount) {
      LOG(WARNING) << to_string() << " ran out of hashlist memory, so it has to flush buckets now"
        " This shouldn't happen often. We must consider increasing kBucketHashListMaxCount."
        " this=" << *this;
      return false;
    }

    // allocate from the pool
    BucketHashList* new_hashlist =
      reinterpret_cast<BucketHashList*>(tmp_hashlist_buffer_slice_.get_block())
      + hashlist_allocated_count_;
    ++hashlist_allocated_count_;

    new_hashlist->storage_id_ = storage_id;
    new_hashlist->head_ = new_bucket;
    new_hashlist->tail_ = new_bucket;
    new_hashlist->bucket_counts_ = 1;

    add_storage_hashlist(new_hashlist);
  }
  return true;
}
void LogMapper::add_storage_hashlist(BucketHashList* new_hashlist) {
  ASSERT_ND(new_hashlist);
  uint8_t index = static_cast<uint8_t>(new_hashlist->storage_id_);
  if (storage_hashlists_[index] == nullptr) {
    storage_hashlists_[index] = new_hashlist;
    new_hashlist->hashlist_next_ = nullptr;
  } else {
    new_hashlist->hashlist_next_ = storage_hashlists_[index];
    storage_hashlists_[index] = new_hashlist;
  }
}

void LogMapper::clear_storage_buckets() {
  std::memset(storage_hashlists_, 0, sizeof(storage_hashlists_));
  buckets_allocated_count_ = 0;
  hashlist_allocated_count_ = 0;
}

void LogMapper::flush_all_buckets() {
  for (uint16_t i = 0; i < 256; ++i) {
    for (BucketHashList* hashlist = storage_hashlists_[i];
          hashlist != nullptr && hashlist->storage_id_ != 0;
          hashlist = hashlist->hashlist_next_) {
      flush_bucket(*hashlist);
    }
  }
  clear_storage_buckets();
}

void LogMapper::flush_bucket(const BucketHashList& hashlist) {
  ASSERT_ND(hashlist.head_);
  ASSERT_ND(hashlist.tail_);
  // temporary variables to store partitioning results
  BufferPosition* position_array = reinterpret_cast<BufferPosition*>(
    tmp_position_array_slice_.get_block());
  PartitionSortEntry* sort_array = reinterpret_cast<PartitionSortEntry*>(
    tmp_sort_array_slice_.get_block());
  storage::PartitionId* partition_array = reinterpret_cast<storage::PartitionId*>(
    tmp_partition_array_slice_.get_block());
  LogBuffer log_buffer(reinterpret_cast<char*>(io_buffer_.get_block()));
  const bool multi_partitions = engine_->get_options().thread_.group_count_ > 1U;

  if (!engine_->get_storage_manager()->get_storage(hashlist.storage_id_)->exists()) {
    // We ignore such logs in snapshot. As DROP STORAGE immediately becomes durable,
    // There is no point to collect logs for the storage.
    LOG(INFO) << "These logs are sent to a dropped storage.. ignore them";
    return;
  }

  uint64_t log_count = 0;  // just for reporting
  debugging::StopWatch stop_watch;
  for (Bucket* bucket = hashlist.head_; bucket != nullptr; bucket = bucket->next_bucket_) {
    ASSERT_ND(bucket->counts_ > 0);
    ASSERT_ND(bucket->counts_ <= kBucketMaxCount);
    ASSERT_ND(bucket->storage_id_ == hashlist.storage_id_);
    log_count += bucket->counts_;

    // if there are multiple partitions, we first partition log entries.
    if (multi_partitions) {
      storage::Partitioner partitioner(engine_, bucket->storage_id_);
      ASSERT_ND(partitioner.is_valid());
      if (partitioner.is_partitionable()) {
        // calculate partitions
        for (uint32_t i = 0; i < bucket->counts_; ++i) {
          position_array[i] = bucket->log_positions_[i];
          ASSERT_ND(log_buffer.resolve(position_array[i])->header_.storage_id_
            == bucket->storage_id_);
          ASSERT_ND(log_buffer.resolve(position_array[i])->header_.storage_id_
            == hashlist.storage_id_);
        }
        storage::Partitioner::PartitionBatchArguments args = {
          static_cast< storage::PartitionId >(numa_node_),
          log_buffer,
          position_array,
          bucket->counts_,
          partition_array};
        partitioner.partition_batch(args);

        // sort the log positions by the calculated partitions
        std::memset(sort_array, 0, sizeof(PartitionSortEntry) * bucket->counts_);
        for (uint32_t i = 0; i < bucket->counts_; ++i) {
          sort_array[i].set(partition_array[i], bucket->log_positions_[i]);
        }
        std::sort(sort_array, sort_array + bucket->counts_);

        // let's reuse the current bucket as a temporary memory to hold sorted entries.
        // buckets are discarded after the flushing, so this doesn't cause any issue.
        const uint32_t original_count = bucket->counts_;
        storage::PartitionId current_partition = sort_array[0].partition_;
        bucket->log_positions_[0] = sort_array[0].position_;
        bucket->counts_ = 1;
        for (uint32_t i = 1; i < original_count; ++i) {
          if (current_partition == sort_array[i].partition_) {
            bucket->log_positions_[bucket->counts_] = sort_array[i].position_;
            ++bucket->counts_;
            ASSERT_ND(bucket->counts_ <= original_count);
          } else {
            // the current partition has ended.
            // let's send out these log entries to this partition
            send_bucket_partition(bucket, current_partition);
            // this is the beginning of next partition
            current_partition = sort_array[i].partition_;
            bucket->log_positions_[0] = sort_array[i].position_;
            bucket->counts_ = 1;
          }
        }

        ASSERT_ND(bucket->counts_ > 0);
        // send out the last partition
        send_bucket_partition(bucket, current_partition);
      } else {
        // in this case, it's same as single partition regarding this storage.
        send_bucket_partition(bucket, 0);
      }
    } else {
      // if it's not multi-partition, we blindly send everything to partition-0 (NUMA node 0)
      send_bucket_partition(bucket, 0);
    }
  }

  stop_watch.stop();
  LOG(INFO) << to_string() << " sent out " << log_count << " log entries for storage-"
    << hashlist.storage_id_ << " in " << stop_watch.elapsed_ms() << " milliseconds";
}

inline void update_key_lengthes(
  const log::LogHeader* header,
  storage::StorageType storage_type,
  uint32_t* shortest_key_length,
  uint32_t* longest_key_length) {
  if (storage_type == storage::kArrayStorage) {
    *shortest_key_length = sizeof(storage::array::ArrayOffset);
    *longest_key_length = sizeof(storage::array::ArrayOffset);
  } else if (storage_type == storage::kMasstreeStorage) {
    const storage::masstree::MasstreeCommonLogType* the_log =
      reinterpret_cast<const storage::masstree::MasstreeCommonLogType*>(header);
    uint16_t key_length = the_log->key_length_;
    ASSERT_ND(key_length > 0);
    *shortest_key_length = std::min<uint32_t>(*shortest_key_length, key_length);
    *longest_key_length = std::max<uint32_t>(*longest_key_length, key_length);
  } else if (storage_type == storage::kHashStorage) {
    const storage::hash::HashCommonLogType* the_log =
      reinterpret_cast<const storage::hash::HashCommonLogType*>(header);
    uint16_t key_length = the_log->key_length_;
    ASSERT_ND(key_length > 0);
    *shortest_key_length = std::min<uint32_t>(*shortest_key_length, key_length);
    *longest_key_length = std::max<uint32_t>(*longest_key_length, key_length);
  } else if (storage_type == storage::kSequentialStorage) {
    // this has no meaning for sequential storage. just put some number.
    *shortest_key_length = 8U;
    *longest_key_length = 8U;
  }
}


void LogMapper::send_bucket_partition(
  Bucket* bucket, storage::PartitionId partition) {
  VLOG(0) << to_string() << " sending " << bucket->counts_ << " log entries for storage-"
    << bucket->storage_id_ << " to partition-" << static_cast<int>(partition);
  storage::StorageType storage_type
    = engine_->get_storage_manager()->get_storage(bucket->storage_id_)->meta_.type_;

  // let's do "pre-sort" to mitigate work from reducer to mapper
  if (engine_->get_options().snapshot_.log_mapper_sort_before_send_
    && storage_type != storage::kSequentialStorage) {  // if sequential, presorting is useless
    send_bucket_partition_presort(bucket, storage_type, partition);
  } else {
    send_bucket_partition_general(bucket, storage_type, partition, bucket->log_positions_);
  }
}

void LogMapper::send_bucket_partition_general(
  const Bucket* bucket,
  storage::StorageType storage_type,
  storage::PartitionId partition,
  const BufferPosition* positions) {
  uint64_t written = 0;
  uint32_t log_count = 0;
  uint32_t shortest_key_length = 0xFFFF;
  uint32_t longest_key_length = 0;
  // stitch the log entries in send buffer
  char* send_buffer = reinterpret_cast<char*>(tmp_send_buffer_slice_.get_block());
  const char* io_base = reinterpret_cast<const char*>(io_buffer_.get_block());
  ASSERT_ND(tmp_send_buffer_slice_.get_size() == kSendBufferSize);

  for (uint32_t i = 0; i < bucket->counts_; ++i) {
    uint64_t pos = from_buffer_position(positions[i]);
    const log::LogHeader* header = reinterpret_cast<const log::LogHeader*>(io_base + pos);
    ASSERT_ND(header->storage_id_ == bucket->storage_id_);
    uint16_t log_length = header->log_length_;
    ASSERT_ND(log_length > 0);
    ASSERT_ND(log_length % 8 == 0);
    if (written + log_length > kSendBufferSize) {
      // buffer full. send out.
      send_bucket_partition_buffer(
        bucket,
        partition,
        send_buffer,
        log_count,
        written,
        shortest_key_length,
        longest_key_length);
      log_count = 0;
      written = 0;
      shortest_key_length = 0xFFFF;
      longest_key_length = 0;
    }
    std::memcpy(send_buffer + written, header, header->log_length_);
    written += header->log_length_;
    ++log_count;
    update_key_lengthes(header, storage_type, &shortest_key_length, &longest_key_length);
  }
  send_bucket_partition_buffer(
    bucket,
    partition,
    send_buffer,
    log_count,
    written,
    shortest_key_length,
    longest_key_length);
}

void LogMapper::send_bucket_partition_presort(
  Bucket* bucket,
  storage::StorageType storage_type,
  storage::PartitionId partition) {
  storage::Partitioner partitioner(engine_, bucket->storage_id_);

  char* io_base = reinterpret_cast<char*>(io_buffer_.get_block());
  presort_ouputs_.assure_capacity(sizeof(BufferPosition) * bucket->counts_);
  BufferPosition* outputs = reinterpret_cast<BufferPosition*>(presort_ouputs_.get_block());

  uint32_t shortest_key_length = 0xFFFF;
  uint32_t longest_key_length = 0;
  if (storage_type == storage::kMasstreeStorage) {
    for (uint32_t i = 0; i < bucket->counts_; ++i) {
      uint64_t pos = from_buffer_position(bucket->log_positions_[i]);
      const log::LogHeader* header = reinterpret_cast<const log::LogHeader*>(io_base + pos);
      update_key_lengthes(header, storage_type, &shortest_key_length, &longest_key_length);
    }
  }

  LogBuffer buffer(io_base);
  uint32_t count = 0;
  storage::Partitioner::SortBatchArguments args = {
    buffer,
    bucket->log_positions_,
    bucket->counts_,
    shortest_key_length,
    longest_key_length,
    &presort_buffer_,
    parent_.get_base_epoch(),
    outputs,
    &count};
  partitioner.sort_batch(args);
  ASSERT_ND(count <= bucket->counts_);
  bucket->counts_ = count;  // it might be compacted

  // then same as usual send_bucket_partition() except we use outputs
  send_bucket_partition_general(bucket, storage_type, partition, outputs);
}

void LogMapper::send_bucket_partition_buffer(
  const Bucket* bucket,
  storage::PartitionId partition,
  const char* send_buffer,
  uint32_t log_count,
  uint64_t written,
  uint32_t shortest_key_length,
  uint32_t longest_key_length) {
  if (written == 0) {
    return;
  }

  LogReducerRef reducer(engine_, partition);
  reducer.append_log_chunk(
    bucket->storage_id_,
    send_buffer,
    log_count,
    written,
    shortest_key_length,
    longest_key_length);
}


std::ostream& operator<<(std::ostream& o, const LogMapper& v) {
  o << "<LogMapper>"
    << "<id_>" << v.id_ << "</id_>"
    << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
    << "<buckets_allocated_count_>" << v.buckets_allocated_count_ << "</buckets_allocated_count_>"
    << "<hashlist_allocated_count>" << v.hashlist_allocated_count_ << "</hashlist_allocated_count>"
    << "<processed_log_count_>" << v.processed_log_count_ << "</processed_log_count_>"
    << "</LogMapper>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
