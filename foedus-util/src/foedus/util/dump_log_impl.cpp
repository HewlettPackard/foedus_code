/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <foedus/epoch.hpp>
#include <foedus/fs/direct_io_file.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/log/common_log_types.hpp>
#include <foedus/log/log_type.hpp>
#include <foedus/log/log_type_invoke.hpp>
#include <foedus/memory/aligned_memory.hpp>
#include <foedus/util/dump_log.hpp>
#include <stdint.h>
#include <algorithm>
#include <iostream>

namespace foedus {
namespace util {

int DumpLog::dump_to_stdout() {
    // runtime arguments
    std::cout << "<DumpLog>" << std::endl
        << "<Args>" << std::endl
            << "  <verbose_>" << verbose_ << "</verbose_>" << std::endl
            << "  <limit_>" << limit_ << "</limit_>" << std::endl
            << "  <from_epoch_>" << from_epoch_ << "</from_epoch_>" << std::endl
            << "  <to_epoch_>" << to_epoch_ << "</to_epoch_>" << std::endl
            << "  <files_>" << std::endl;
    for (const fs::Path &file : files_) {
        std::cout << "    <file>" << file << "</file>" << std::endl;
    }
    std::cout << "  </files_>" << std::endl
        << "</Args>" << std::endl;

    // callback object to output the meat part
    struct DumpCallback : public ParserCallback {
        void process(log::LogHeader *entry, uint64_t offset) override {
            std::cout << "    <Entry offset=\"" << offset << "\" offset_hex=\"0x"
                << std::hex << std::uppercase << offset << std::nouppercase << std::dec << "\">";
            log::invoke_ostream(entry, &std::cout);
            std::cout << "</Entry>" << std::endl;
        }
        Epoch cur_epoch_;
        DumpLog* enclosure_;
    };

    // the meat part for each file
    DumpCallback callback;
    callback.enclosure_ = this;
    for (uint32_t file_index = 0; file_index < files_.size(); ++file_index) {
        std::cout << "  <LogFile\n    file_index=\"" << file_index
            << "\"\n     path=\"" << files_[file_index]
            << "\"\n     bytes=\"" << fs::file_size(files_[file_index]) << "\">" << std::endl;
        parse_log_file(file_index, &callback);
        std::cout << "  </LogFile>" << std::endl;
    }

    // also write out execution summary at the end
    std::cout << "<Results>" << std::endl
        << "  <processed_logs_>" << result_processed_logs_ << "</processed_logs_>" << std::endl
        << "  <limit_reached_>" << result_limit_reached_ << "</limit_reached_>" << std::endl
        << "  <first_epoch_>" << result_first_epoch_ << "</first_epoch_>" << std::endl
        << "  <last_epoch_>" << result_last_epoch_ << "</last_epoch_>" << std::endl
        << "  <inconsistencies_>" << std::endl;
    for (const LogInconsistency &inconsistency : result_inconsistencies_) {
        std::cout << "    " << inconsistency << std::endl;
    }

    std::cout << "  </inconsistencies_>" << std::endl
        << "</Results>" << std::endl;

    std::cout << "</DumpLog>" << std::endl;
    return 0;
}

void DumpLog::parse_log_file(uint32_t file_index, ParserCallback* callback) {
    if (result_limit_reached_) {
        return;
    }

    const fs::Path &path = files_[file_index];
    fs::DirectIoFile file(path);
    const uint32_t ALIGNMENT = log::FillerLogType::LOG_WRITE_UNIT_SIZE;
    memory::AlignedMemory buffer(1 << 24, ALIGNMENT, memory::AlignedMemory::POSIX_MEMALIGN, 0);
    uint64_t file_size = fs::file_size(path);
    if (file_size % ALIGNMENT != 0) {
        result_inconsistencies_.emplace_back(
            LogInconsistency(LogInconsistency::NON_ALIGNED_FILE_END, file_index, 0));
        file_size = (file_size / ALIGNMENT) * ALIGNMENT;
    }

    uint64_t prev_file_offset = 0;
    uint64_t buffer_size = 0;
    uint64_t buffer_offset = 0;
    while (true) {
        const uint64_t cur_offset = prev_file_offset + buffer_offset;
        char* address = reinterpret_cast<char*>(buffer.get_block()) + buffer_offset;
        log::LogHeader *header = reinterpret_cast<log::LogHeader*>(address);

        // do we need to read from file?
        bool need_to_read_file = false;
        uint64_t new_file_offset = 0;
        uint64_t skip_after_read = 0;
        if (buffer_size == buffer_offset) {
            need_to_read_file = true;
            new_file_offset = prev_file_offset + buffer_size;
        } else {
            // in this case, we have to partially re-read to keep accesses aligned
            if (header->log_length_ > (buffer_size - buffer_offset)) {
                need_to_read_file = true;
                new_file_offset = prev_file_offset + (buffer_offset / ALIGNMENT * ALIGNMENT);
                skip_after_read = (buffer_offset / ALIGNMENT * ALIGNMENT) - buffer_offset;
            }
        }

        if (need_to_read_file) {
            ASSERT_ND(new_file_offset > prev_file_offset);
            uint64_t next_reads = std::min(file_size - new_file_offset, buffer.get_size());
            if (next_reads - skip_after_read < header->log_length_) {
                result_inconsistencies_.emplace_back(
                    LogInconsistency(LogInconsistency::INCOMPLETE_ENTRY_AT_END,
                                        file_index, cur_offset, header->log_length_));
                break;
            }
            ASSERT_ND(next_reads % ALIGNMENT == 0);
            ASSERT_ND(new_file_offset + next_reads <= file_size);
            COERCE_ERROR(file.seek(new_file_offset, fs::DirectIoFile::DIRECT_IO_SEEK_SET));
            COERCE_ERROR(file.read(next_reads, &buffer));
            prev_file_offset = new_file_offset;
            buffer_size = next_reads;
            buffer_offset = skip_after_read;
            continue;
        }

        ASSERT_ND(buffer_size > buffer_offset);
        ASSERT_ND(header->log_length_ <= (buffer_size - buffer_offset));

        if (header->log_length_ == 0 || header->log_length_ % 8 != 0) {
            result_inconsistencies_.emplace_back(
                LogInconsistency(LogInconsistency::MISSING_LOG_LENGTH, file_index, cur_offset,
                    header->log_length_));
        }

        if (log::is_valid_log_type(header->get_type())) {
            log::LogCodeKind kind = log::get_log_code_kind(header->get_type());
            if (kind == foedus::log::STORAGE_LOGS || kind == foedus::log::RECORD_LOGS) {
                if (header->storage_id_ == 0) {
                    result_inconsistencies_.emplace_back(
                        LogInconsistency(LogInconsistency::MISSING_STORAGE_ID, file_index,
                                        cur_offset, header->get_type()));
                }
            }
            if (header->get_type() == log::LOG_CODE_EPOCH_MARKER) {
                log::EpochMarkerLogType *marker
                    = reinterpret_cast<log::EpochMarkerLogType*>(header);
                if (!marker->old_epoch_.is_valid()) {
                    result_inconsistencies_.emplace_back(LogInconsistency(
                        LogInconsistency::INVALID_OLD_EPOCH, file_index, cur_offset));
                }
                if (!marker->new_epoch_.is_valid()) {
                    result_inconsistencies_.emplace_back(LogInconsistency(
                        LogInconsistency::INVALID_NEW_EPOCH, file_index, cur_offset));
                }
                if (result_cur_epoch_.is_valid() && result_cur_epoch_ != marker->old_epoch_) {
                    result_inconsistencies_.emplace_back(LogInconsistency(
                        LogInconsistency::EPOCH_MARKER_DOES_NOT_MATCH, file_index, cur_offset));
                }
                if (!result_first_epoch_.is_valid()) {
                    result_first_epoch_ = marker->new_epoch_;
                }
                result_cur_epoch_ = marker->new_epoch_;
                result_last_epoch_ = marker->new_epoch_;
            }

            callback->process(header, cur_offset);
        } else {
            result_inconsistencies_.emplace_back(
                LogInconsistency(LogInconsistency::MISSING_LOG_LENGTH, file_index, cur_offset,
                                 header->log_type_code_));
        }

        buffer_offset += header->log_length_;
        ++result_processed_logs_;
        if (limit_ >= 0 && static_cast<uint64_t>(limit_) <= result_processed_logs_) {
            result_limit_reached_ = true;
            break;
        }
    }

    buffer.release_block();
    file.close();
}

std::ostream& operator<<(std::ostream& o, const LogInconsistency& v) {
    o << "<inconsistency>"
        << "<file_index_>" << v.file_index_ << "</file_index_>"
        << "<offset_>" << v.offset_ << "</offset_>"
        << "<additional_data_>" << v.additional_data_ << "</additional_data_>"
        << "<type_code_>0x" << std::hex << std::uppercase << v.type_ << std::nouppercase << std::dec
                << "</type_code_>"
        << "<type_name_>" << LogInconsistency::type_to_string(v.type_) << "</type_name_>"
        << "<type_description_>"
            << LogInconsistency::type_to_description(v.type_) << "</type_description_>"
        << "</inconsistency>";
    return o;
}

}  // namespace util
}  // namespace foedus
