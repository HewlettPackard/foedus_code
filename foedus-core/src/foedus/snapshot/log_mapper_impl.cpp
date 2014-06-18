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

ErrorStack LogMapper::handle_epoch() {
    const Epoch epoch = parent_->get_processing_epoch();
    const log::Logger& logger = engine_->get_log_manager().get_logger(id_);
    log::LogFileOrdinal cur_file_ordinal;
    uint64_t cur_offset;
    {
        const log::EpochHistory *history = logger.get_epoch_history_for(epoch);
        if (history == nullptr) {
            VLOG(0) << to_string() << " has no log for epoch-" << epoch;
            return kRetOk;
        }
        DVLOG(0) << to_string() << " found epoch history: " << *history;
        cur_file_ordinal = history->log_file_ordinal_;
        cur_offset = history->log_file_offset_;
    }

    // open the file and seek to there.
    // here, we open log file for every handle_epoch().
    // for most epochs, we are opening the same file from the previously-ended position.
    // so, we can speed this up by keeping the file object, but not sure it's worth doing.
    // we anyway assume NVRAM or fast SSD.
    processed_log_count_ = 0;
    bool epoch_ended = false;
    while (!epoch_ended) {  // loop for log file switch in the epoch
        fs::Path path = logger.construct_suffixed_log_path(cur_file_ordinal);
        uint64_t file_size = fs::file_size(path);
        DVLOG(1) << to_string() << " file path=" << path << ", file size=" << file_size;
        fs::DirectIoFile file(path, engine_->get_options().snapshot_.emulation_);
        CHECK_ERROR(file.open(true, false, false, false));
        DVLOG(1) << to_string() << "opened log file " << file;
        CHECK_ERROR(file.seek(cur_offset, fs::DirectIoFile::kDirectIoSeekSet));
        DVLOG(1) << to_string() << "seeked to: " << cur_offset;

        uint32_t fragment_saved = 0;  // bytes of fragment in previous read
        uint32_t fragment_remaining = 0;
        while (!epoch_ended && cur_offset < file_size) {  // loop for each read in the file
            uint64_t pos = 0;
            const uint64_t reads = std::min(io_buffer_.get_size(), file_size - cur_offset);
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
                if (header->get_type() == log::kLogCodeEpochMarker) {
                    const log::EpochMarkerLogType *marker =
                        reinterpret_cast<const log::EpochMarkerLogType*>(header);
                    ASSERT_ND(header->log_length_ == sizeof(log::EpochMarkerLogType));
                    ASSERT_ND(marker->old_epoch_ == epoch || marker->new_epoch_ == epoch);
                    ASSERT_ND(marker->log_file_ordinal_ == cur_file_ordinal);
                    ASSERT_ND(marker->log_file_offset_ == cur_offset + pos);
                    ASSERT_ND(marker->new_epoch_ >= marker->old_epoch_);
                    if (epoch == marker->new_epoch_) {
                        // this is the first epoch marker or a dummy epoch marker. just skip it.
                        pos += header->log_length_;
                        continue;
                    } else {
                        // we reached the beginning of next epoch, so we are done!
                        ASSERT_ND(marker->new_epoch_ > epoch);
                        epoch_ended = true;
                        break;
                    }
                }
                if (header->log_length_ > reads - pos) {
                    // this log spans two file reads.
                    if (cur_offset >= file_size) {
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
                    break;
                } else {
                    WRAP_ERROR_CODE(map_log(header));
                    pos += header->log_length_;
                }
            }

            // we processed this file read.
            cur_offset += reads;
            if (!epoch_ended && cur_offset == file_size) {
                // we reached end of this file, but we might not be done yet. thus, we do either
                // 1) move on to next file.
                // 2) if that was the last file, stop here.
                ASSERT_ND(fragment_saved == 0);  // no log spans two files
                ASSERT_ND(fragment_remaining == 0);
                if (logger.get_current_ordinal() == cur_file_ordinal) {
                    LOG(INFO) << to_string() << " reached end of all log files. this means"
                        " there has been no log emitted by this logger for a while";
                    epoch_ended = true;
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
    VLOG(0) << to_string() << " processed " << processed_log_count_ << " log entries in epoch-"
        << epoch;
    return kRetOk;
}

void LogMapper::pre_wait_for_next_epoch() {
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
