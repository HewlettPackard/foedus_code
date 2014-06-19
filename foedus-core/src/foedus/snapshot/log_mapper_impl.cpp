/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
4 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/epoch.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/fs/direct_io_file.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/log/log_manager.hpp>
#include <foedus/log/log_type.hpp>
#include <foedus/log/logger_impl.hpp>
#include <foedus/memory/memory_id.hpp>
#include <foedus/snapshot/log_gleaner_impl.hpp>
#include <foedus/snapshot/log_mapper_impl.hpp>
#include <foedus/snapshot/snapshot.hpp>
#include <glog/logging.h>
#include <algorithm>
#include <ostream>
#include <string>
namespace foedus {
namespace snapshot {

ErrorStack LogMapper::handle_initialize() {
  const SnapshotOptions& option = engine_->get_options().snapshot_;

  uint64_t io_buffer_size = static_cast<uint64_t>(option.log_mapper_io_buffer_kb_) << 10;
  io_buffer_size = assorted::align<uint64_t, memory::kHugepageSize>(io_buffer_size);
  io_buffer_.alloc(
    io_buffer_size,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);
  ASSERT_ND(!io_buffer_.is_null());

  io_fragment_tmp_.alloc(1 << 16, 1 << 12, memory::AlignedMemory::kNumaAllocOnnode, numa_node_);
  ASSERT_ND(!io_fragment_tmp_.is_null());

  bucket_size_kb_ = option.log_mapper_bucket_kb_;
  uint64_t bucket_size = static_cast<uint64_t>(bucket_size_kb_) << 10;
  uint64_t bucket_size_total = bucket_size * parent_->get_reducers_count();
  bucket_size_total = assorted::align<uint64_t, memory::kHugepageSize>(bucket_size_total);
  buckets_memory_.alloc(
    bucket_size_total,
    memory::kHugepageSize,
    memory::AlignedMemory::kNumaAllocOnnode,
    numa_node_);
  ASSERT_ND(!buckets_memory_.is_null());

  processed_log_count_ = 0;

  return kRetOk;
}

ErrorStack LogMapper::handle_uninitialize() {
  ErrorStackBatch batch;
  io_buffer_.release_block();
  io_fragment_tmp_.release_block();
  buckets_memory_.release_block();
  return SUMMARIZE_ERROR_BATCH(batch);
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
    CHECK_ERROR(file.seek(cur_offset, fs::DirectIoFile::kDirectIoSeekSet));
    DVLOG(1) << to_string() << "seeked to: " << cur_offset;

    uint32_t fragment_saved = 0;  // bytes of fragment in previous read
    uint32_t fragment_remaining = 0;
    while (!ended && cur_offset < read_end) {  // loop for each read in the file
      WRAP_ERROR_CODE(check_cancelled());  // check per each read
      uint64_t pos = 0;
      const uint64_t reads = std::min(io_buffer_.get_size(), read_end - cur_offset);
      CHECK_ERROR(file.read(reads, &io_buffer_));
      if (fragment_saved > 0) {
        // last log entry is continuing to this read.
        DVLOG(1) << to_string() << " gluing the last log fragment. fragment_saved="
          << fragment_saved << ", fragment_remaining=" << fragment_remaining;
        ASSERT_ND(fragment_remaining > 0);
        ASSERT_ND(fragment_remaining <= reads);
        char* fragment = reinterpret_cast<char*>(io_fragment_tmp_.get_block());
        std::memcpy(
          fragment + fragment_saved,
          io_buffer_.get_block(),
          fragment_remaining);

        const log::LogHeader *header = reinterpret_cast<const log::LogHeader*>(fragment);
        ASSERT_ND(header->log_length_ == fragment_saved + fragment_remaining);
        WRAP_ERROR_CODE(map_log(header));

        pos = fragment_remaining;
        fragment_saved = 0;
        fragment_remaining = 0;
      }

      char* buffer = reinterpret_cast<char*>(io_buffer_.get_block());
      while (pos < reads) {
        const log::LogHeader* header
          = reinterpret_cast<const log::LogHeader*>(buffer + pos);
        ASSERT_ND(cur_offset != 0 || pos != 0
          || header->get_type() == log::kLogCodeEpochMarker);  // file starts with marker
        // we must be starting from epoch marker.
        ASSERT_ND(!first_read || header->get_type() == log::kLogCodeEpochMarker);

        // skip epoch marker
        if (header->get_type() == log::kLogCodeEpochMarker) {
          const log::EpochMarkerLogType *marker =
            reinterpret_cast<const log::EpochMarkerLogType*>(header);
          ASSERT_ND(header->log_length_ == sizeof(log::EpochMarkerLogType));
          ASSERT_ND(marker->log_file_ordinal_ == cur_file_ordinal);
          ASSERT_ND(marker->log_file_offset_ == cur_offset + pos);
          ASSERT_ND(marker->new_epoch_ >= marker->old_epoch_);
          ASSERT_ND(!base_epoch.is_valid() || marker->new_epoch_ >= base_epoch);
          ASSERT_ND(marker->new_epoch_ <= until_epoch);
          if (first_read) {
            ASSERT_ND(!base_epoch.is_valid() || marker->old_epoch_ <= base_epoch);
            first_read = false;
          } else {
            ASSERT_ND(!base_epoch.is_valid() || marker->old_epoch_ >= base_epoch);
          }
          pos += header->log_length_;
          continue;
        }

        if (header->log_length_ > reads - pos) {
          // this log spans two file reads.
          if (cur_offset >= read_end) {
            // but it shouldn't span two files or two epochs. something is wrong.
            LOG(ERROR) << "inconsistent end of log entry. offset=" << (cur_offset + pos)
              << ", file=" << file << ", log header=" << *header;
            return ERROR_STACK_MSG(
              kErrorCodeSnapshotInvalidLogEnd,
              file.get_path().c_str());
          }

          // save this piece in the temporary buffer
          char* fragment = reinterpret_cast<char*>(io_fragment_tmp_.get_block());
          std::memcpy(fragment, buffer + pos, reads - pos);
          fragment_saved = reads - pos;
          fragment_remaining =  header->log_length_ - fragment_saved;
          pos += reads - pos;
        } else {
          WRAP_ERROR_CODE(map_log(header));
          pos += header->log_length_;
        }
      }
      ASSERT_ND(pos == reads);

      // we processed this file read.
      cur_offset += reads;
      if (!ended && cur_offset == read_end) {
        // we reached end of this file. was it the last file?
        ASSERT_ND(fragment_saved == 0);  // no log spans two files
        ASSERT_ND(fragment_remaining == 0);
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

void LogMapper::pre_handle_uninitialize() {
  uint16_t value_after = parent_->increment_completed_mapper_count();
  if (value_after == parent_->get_mappers_count()) {
    LOG(INFO) << "wait_for_next_epoch(): " << to_string() << " was the last mapper.";
  }
}

std::ostream& operator<<(std::ostream& o, const LogMapper& v) {
  o << "<LogMapper>"
    << "<id_>" << v.id_ << "</id_>"
    << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
    << "<thread_>" << v.thread_ << "</thread_>"
    << "</LogMapper>";
  return o;
}


}  // namespace snapshot
}  // namespace foedus
