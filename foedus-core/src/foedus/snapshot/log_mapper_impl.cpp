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
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/logger_impl.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/snapshot/log_gleaner_impl.hpp"
#include "foedus/snapshot/snapshot.hpp"

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
  buckets_all_count_ = bucket_size / sizeof(Bucket);
  buckets_memory_.alloc(
    bucket_size,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);
  ASSERT_ND(!buckets_memory_.is_null());

  buckets_allocated_count_ = 0;
  processed_log_count_ = 0;
  clear_storage_buckets();

  return kRetOk;
}

ErrorStack LogMapper::handle_uninitialize() {
  ErrorStackBatch batch;
  io_buffer_.release_block();
  buckets_memory_.release_block();
  clear_storage_buckets();
  return SUMMARIZE_ERROR_BATCH(batch);
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

  uint64_t pos = 0;
  char* buffer = reinterpret_cast<char*>(io_buffer_.get_block());
  while (pos < buffered_bytes) {
    const log::LogHeader* header
      = reinterpret_cast<const log::LogHeader*>(buffer + pos);
    ASSERT_ND(*cur_offset != 0 || pos != 0
      || header->get_type() == log::kLogCodeEpochMarker);  // file starts with marker
    // we must be starting from epoch marker.
    ASSERT_ND(!*first_read || header->get_type() == log::kLogCodeEpochMarker);

    if (header->log_length_ > buffered_bytes - pos) {
      // if a log goes beyond this read, stop processing here and read from that offset again.
      // this is simpler than glue-ing the fragment. This happens just once per 64MB read,
      // so not a big waste.
      if (*cur_offset + pos + header->log_length_ > fs::file_size(file.get_path())) {
        // but it never spans two files. something is wrong.
        LOG(ERROR) << "inconsistent end of log entry. offset=" << (*cur_offset + pos)
          << ", file=" << file << ", log header=" << *header;
        return ERROR_STACK_MSG(kErrorCodeSnapshotInvalidLogEnd, file.get_path().c_str());
      }
      break;
    }

    // skip epoch marker
    if (header->get_type() == log::kLogCodeEpochMarker) {
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
      pos += header->log_length_;
      continue;
    }

    WRAP_ERROR_CODE(map_log(header));
    pos += header->log_length_;
  }
  *cur_offset += pos;
  return kRetOk;
}

ErrorCode LogMapper::map_log(const log::LogHeader* header) {
  ++processed_log_count_;
  if (header->get_type() == log::kLogCodeFiller) {
    return kErrorCodeOk;
  }
  if (header->get_kind() != log::kRecordLogs) {
    // every thing other than record-targetted logs is processed at the end of epoch.
    parent_->add_nonrecord_log(header);
    return kErrorCodeOk;
  }
  return kErrorCodeOk;
}

void LogMapper::pre_handle_uninitialize() {
  uint16_t value_after = parent_->increment_completed_mapper_count();
  if (value_after == parent_->get_mappers_count()) {
    LOG(INFO) << "wait_for_next_epoch(): " << to_string() << " was the last mapper.";
  }
}


void LogMapper::clear_storage_buckets() {
  // we have to delete something only when there is non-null hashtable_next_
  for (uint16_t i = 0; i < 256; ++i) {
    for (BucketLinkedList* next = storage_buckets_[i].hashtable_next_; next;) {
      BucketLinkedList* tmp_next = next->hashtable_next_;
      delete next;
      next = tmp_next;
    }
  }
  std::memset(storage_buckets_, 0, sizeof(storage_buckets_));
}

bool LogMapper::add_new_bucket(storage::StorageId storage_id) {
  if (buckets_allocated_count_ >= buckets_all_count_) {
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

  BucketLinkedList* hashtable_entry = storage_buckets_ + static_cast<uint8_t>(storage_id);
  BucketLinkedList* existing_buckets;
  for (existing_buckets = hashtable_entry;
      existing_buckets->storage_id_ != storage_id && existing_buckets != nullptr;
      existing_buckets = existing_buckets->hashtable_next_) {
    continue;
  }

  if (existing_buckets) {
    // just add this as a new tail
    ASSERT_ND(existing_buckets->tail_->is_full());
    existing_buckets->tail_->next_bucket_ = new_bucket;
    existing_buckets->tail_ = new_bucket;
  } else {
    // we don't even have a linked list for this.
    BucketLinkedList* new_entry;
    if (hashtable_entry->storage_id_ == 0) {
      // this is the first in this bucket, so let's just use it.
      new_entry = hashtable_entry;
      new_entry->hashtable_next_ = nullptr;
    } else {
      // it's already taken. we have to instantiate ourself. If this happens often, maybe
      // we should have 65536 hash buckets...
      new_entry = new BucketLinkedList();

      // insert between hashtable_entry and hashtable_entry->hashtable_next_
      new_entry->hashtable_next_ = hashtable_entry->hashtable_next_;
      hashtable_entry->hashtable_next_ = new_entry;
    }
    new_entry->storage_id_ = storage_id;
    new_entry->head_ = new_bucket;
    new_entry->tail_ = new_bucket;
  }
  return true;
}

inline bool LogMapper::bucket_log(
  storage::StorageId storage_id, MapperBufferPosition log_position) {
  BucketLinkedList* buckets = &storage_buckets_[storage_id & 0xFF];
  while (UNLIKELY(buckets->storage_id_ != storage_id)) {
    buckets = buckets->hashtable_next_;
    if (buckets == nullptr) {
      return false;  // not found
    }
  }

  if (LIKELY(!buckets->tail_->is_full())) {
    buckets->tail_->log_positions_[buckets->tail_->counts_] = log_position;
    ++buckets->tail_->counts_;
    return true;
  } else {
    // found it, but we need to add a new bucket for this storage.
    return false;
  }
}

ErrorStack LogMapper::flush_buckets() {
  return kRetOk;
}

std::ostream& operator<<(std::ostream& o, const LogMapper& v) {
  o << "<LogMapper>"
    << "<id_>" << v.id_ << "</id_>"
    << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
    << "<buckets_allocated_count_>" << v.buckets_allocated_count_ << "</buckets_allocated_count_>"
    << "<buckets_all_count_>" << v.buckets_all_count_ << "</buckets_all_count_>"
    << "<processed_log_count_>" << v.processed_log_count_ << "</processed_log_count_>"
    << "<thread_>" << v.thread_ << "</thread_>"
    << "</LogMapper>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
