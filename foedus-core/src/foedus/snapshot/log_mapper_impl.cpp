/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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

namespace foedus {
namespace snapshot {

ErrorStack LogMapper::handle_initialize() {
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

ErrorStack LogMapper::handle_uninitialize() {
  ErrorStackBatch batch;
  io_buffer_.release_block();
  buckets_memory_.release_block();
  tmp_memory_.release_block();
  clear_storage_buckets();
  return SUMMARIZE_ERROR_BATCH(batch);
}

void LogMapper::pre_handle_complete() {
  uint16_t value_after = parent_->increment_completed_mapper_count();
  if (value_after == parent_->get_mappers_count()) {
    LOG(INFO) << "wait_for_next_epoch(): " << to_string() << " was the last mapper.";
  }
}

ErrorStack LogMapper::handle_process() {
  const Epoch base_epoch = parent_->get_snapshot()->base_epoch_;
  const Epoch until_epoch = parent_->get_snapshot()->valid_until_epoch_;
  log::Logger& logger = engine_->get_log_manager().get_logger(id_);
  const log::Logger::LogRange log_range = logger.get_log_range(base_epoch, until_epoch);
  log::LogFileOrdinal cur_file_ordinal = log_range.begin_file_ordinal;
  uint64_t cur_offset = log_range.begin_offset;
  if (log_range.is_empty()) {
    LOG(INFO) << to_string() << " has no logs to process";
    return kRetOk;
  }

  // open the file and seek to there.
  processed_log_count_ = 0;
  bool ended = false;
  bool first_read = true;
  while (!ended) {  // loop for log file switch
    fs::Path path = logger.construct_suffixed_log_path(cur_file_ordinal);
    uint64_t file_size = fs::file_size(path);
    uint64_t read_end;
    if (cur_file_ordinal == log_range.end_file_ordinal) {
      ASSERT_ND(log_range.end_offset <= file_size);
      read_end = log_range.end_offset;
    } else {
      read_end = file_size;
    }
    DVLOG(1) << to_string() << " file path=" << path << ", file size=" << file_size
      << ", read_end=" << read_end;
    fs::DirectIoFile file(path, engine_->get_options().snapshot_.emulation_);
    CHECK_ERROR(file.open(true, false, false, false));
    DVLOG(1) << to_string() << "opened log file " << file;

    while (!ended && cur_offset < read_end) {  // loop for each read in the file
      WRAP_ERROR_CODE(check_cancelled());  // check per each read
      if (file.get_current_offset() != cur_offset) {
        CHECK_ERROR(file.seek(cur_offset, fs::DirectIoFile::kDirectIoSeekSet));
        DVLOG(1) << to_string() << "seeked to: " << cur_offset;
      }
      const uint64_t reads = std::min(io_buffer_.get_size(), read_end - cur_offset);
      CHECK_ERROR(file.read(reads, &io_buffer_));
      CHECK_ERROR(handle_process_buffer(file, reads, cur_file_ordinal, &cur_offset, &first_read));

      if (!ended && cur_offset == read_end) {
        // we reached end of this file. was it the last file?
        if (log_range.end_file_ordinal == cur_file_ordinal) {
          ended = true;
        } else {
          ++cur_file_ordinal;
          cur_offset = 0;
          LOG(INFO) << to_string()
            << " moved on to next log file ordinal " << cur_file_ordinal;
        }
      }
    }
    file.close();
  }
  VLOG(0) << to_string() << " processed " << processed_log_count_ << " log entries";
  return kRetOk;
}

ErrorStack LogMapper::handle_process_buffer(
  const fs::DirectIoFile &file, uint64_t buffered_bytes, log::LogFileOrdinal cur_file_ordinal,
  uint64_t *cur_offset, bool *first_read) {
  const Epoch base_epoch = parent_->get_snapshot()->base_epoch_;  // only for assertions
  const Epoch until_epoch = parent_->get_snapshot()->valid_until_epoch_;  // only for assertions
  const uint64_t file_len = fs::file_size(file.get_path());

  // many temporary memory are used only within this method and completely cleared out
  // for every call.
  clear_storage_buckets();

  uint64_t pos;  // buffer-position
  char* buffer = reinterpret_cast<char*>(io_buffer_.get_block());
  for (pos = 0; pos < buffered_bytes; ++processed_log_count_) {
    // Note: The loop here must be a VERY tight loop, iterated over every single log entry!
    // In most cases, we should be just calling bucket_log().
    const log::LogHeader* header
      = reinterpret_cast<const log::LogHeader*>(buffer + pos);
    ASSERT_ND(*cur_offset != 0 || pos != 0
      || header->get_type() == log::kLogCodeEpochMarker);  // file starts with marker
    // we must be starting from epoch marker.
    ASSERT_ND(!*first_read || header->get_type() == log::kLogCodeEpochMarker);

    if (UNLIKELY(header->log_length_ > buffered_bytes - pos)) {
      // if a log goes beyond this read, stop processing here and read from that offset again.
      // this is simpler than glue-ing the fragment. This happens just once per 64MB read,
      // so not a big waste.
      if (*cur_offset + pos + header->log_length_ > file_len) {
        // but it never spans two files. something is wrong.
        LOG(ERROR) << "inconsistent end of log entry. offset=" << (*cur_offset + pos)
          << ", file=" << file << ", log header=" << *header;
        return ERROR_STACK_MSG(kErrorCodeSnapshotInvalidLogEnd, file.get_path().c_str());
      }
      break;
    } else if (UNLIKELY(header->get_type() == log::kLogCodeEpochMarker)) {
      // skip epoch marker
      const log::EpochMarkerLogType *marker =
        reinterpret_cast<const log::EpochMarkerLogType*>(header);
      ASSERT_ND(header->log_length_ == sizeof(log::EpochMarkerLogType));
      ASSERT_ND(marker->log_file_ordinal_ == cur_file_ordinal);
      ASSERT_ND(marker->log_file_offset_ == *cur_offset + pos);
      ASSERT_ND(marker->new_epoch_ >= marker->old_epoch_);
      ASSERT_ND(!base_epoch.is_valid() || marker->new_epoch_ >= base_epoch);
      ASSERT_ND(marker->new_epoch_ <= until_epoch);
      if (*first_read) {
        ASSERT_ND(!base_epoch.is_valid() || marker->old_epoch_ <= base_epoch);
        *first_read = false;
      } else {
        ASSERT_ND(!base_epoch.is_valid() || marker->old_epoch_ >= base_epoch);
      }
    } else if (UNLIKELY(header->get_type() == log::kLogCodeFiller)) {
      // skip filler log
    } else if (UNLIKELY(header->get_kind() != log::kRecordLogs)) {
      // every thing other than record-targetted logs is processed at the end of epoch.
      parent_->add_nonrecord_log(header);
    } else {
      bool bucketed = bucket_log(header->storage_id_, pos);
      if (UNLIKELY(!bucketed)) {
        // need to add a new bucket
        bool added = add_new_bucket(header->storage_id_);
        if (added) {
          bucketed = bucket_log(header->storage_id_, pos);
          ASSERT_ND(bucketed);
        } else {
          // runs out of bucket_memory. have to flush now.
          flush_all_buckets();
          added = add_new_bucket(header->storage_id_);
          ASSERT_ND(added);
          bucketed = bucket_log(header->storage_id_, pos);
          ASSERT_ND(bucketed);
        }
      }
    }

    pos += header->log_length_;
  }
  *cur_offset += pos;

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
  const bool multi_partitions = engine_->get_options().thread_.group_count_ > 1;

  uint64_t log_count = 0;  // just for reporting
  debugging::StopWatch stop_watch;
  for (Bucket* bucket = hashlist.head_; bucket != nullptr; bucket = bucket->next_bucket_) {
    ASSERT_ND(bucket->counts_ > 0);
    ASSERT_ND(bucket->counts_ <= kBucketMaxCount);
    ASSERT_ND(bucket->storage_id_ == hashlist.storage_id_);
    log_count += bucket->counts_;

    // if there are multiple partitions, we first partition log entries.
    if (multi_partitions) {
      const storage::Partitioner* partitioner = parent_->get_or_create_partitioner(
        bucket->storage_id_);
      if (partitioner->is_partitionable()) {
        // calculate partitions
        for (uint32_t i = 0; i < bucket->counts_; ++i) {
          position_array[i] = bucket->log_positions_[i];
          ASSERT_ND(log_buffer.resolve(position_array[i])->header_.storage_id_
            == bucket->storage_id_);
          ASSERT_ND(log_buffer.resolve(position_array[i])->header_.storage_id_
            == hashlist.storage_id_);
        }
        partitioner->partition_batch(
          numa_node_,
          log_buffer,
          position_array,
          bucket->counts_,
          partition_array);

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
            send_bucket_partition(*bucket, current_partition);
            // this is the beginning of next partition
            current_partition = sort_array[i].partition_;
            bucket->log_positions_[0] = sort_array[i].position_;
            bucket->counts_ = 1;
          }
        }

        ASSERT_ND(bucket->counts_ > 0);
        // send out the last partition
        send_bucket_partition(*bucket, current_partition);
      } else {
        // in this case, it's same as single partition regarding this storage.
        send_bucket_partition(*bucket, 0);
      }
    } else {
      // if it's not multi-partition, we blindly send everything to partition-0 (NUMA node 0)
      send_bucket_partition(*bucket, 0);
    }
  }

  stop_watch.stop();
  LOG(INFO) << to_string() << " sent out " << log_count << " log entries for storage-"
    << hashlist.storage_id_ << " in " << stop_watch.elapsed_ms() << " milliseconds";
}

void LogMapper::send_bucket_partition(
  const Bucket& bucket, storage::PartitionId partition) {
  VLOG(0) << to_string() << " sending " << bucket.counts_ << " log entries for storage-"
    << bucket.storage_id_ << " to partition-" << static_cast<int>(partition);
  // stitch the log entries in send buffer
  char* send_buffer = reinterpret_cast<char*>(tmp_send_buffer_slice_.get_block());
  const char* buffer_base_address = reinterpret_cast<const char*>(io_buffer_.get_block());
  ASSERT_ND(tmp_send_buffer_slice_.get_size() == kSendBufferSize);

  uint64_t written = 0;
  uint32_t log_count = 0;
  for (uint32_t i = 0; i < bucket.counts_; ++i) {
    const log::LogHeader* header = reinterpret_cast<const log::LogHeader*>(
      buffer_base_address + from_buffer_position(bucket.log_positions_[i]));
    ASSERT_ND(header->storage_id_ == bucket.storage_id_);
    ASSERT_ND(header->log_length_ > 0);
    ASSERT_ND(header->log_length_ % 8 == 0);
    if (written + header->log_length_ > kSendBufferSize) {
      // buffer full. send out.
      send_bucket_partition_buffer(bucket, partition, send_buffer, log_count, written);
      log_count = 0;
      written = 0;
    }
    std::memcpy(send_buffer, header, header->log_length_);
    written += header->log_length_;
    ++log_count;
  }
  send_bucket_partition_buffer(bucket, partition, send_buffer, log_count, written);
}

void LogMapper::send_bucket_partition_buffer(
  const Bucket& bucket,
  storage::PartitionId partition,
  const char* send_buffer,
  uint32_t log_count,
  uint64_t written) {
  if (written == 0) {
    return;
  }
  LogReducer* reducer = parent_->get_reducer(partition);
  reducer->append_log_chunk(bucket.storage_id_, send_buffer, log_count, written);
}


std::ostream& operator<<(std::ostream& o, const LogMapper& v) {
  o << "<LogMapper>"
    << "<id_>" << v.id_ << "</id_>"
    << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
    << "<buckets_allocated_count_>" << v.buckets_allocated_count_ << "</buckets_allocated_count_>"
    << "<hashlist_allocated_count>" << v.hashlist_allocated_count_ << "</hashlist_allocated_count>"
    << "<processed_log_count_>" << v.processed_log_count_ << "</processed_log_count_>"
    << "<thread_>" << v.thread_ << "</thread_>"
    << "</LogMapper>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
