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
#include "foedus/assert_nd.hpp"
#include "foedus/epoch.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/log_type_invoke.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/util/dump_log.hpp"
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
      // epoch marker or engine/storage logs are fully shown even in brief mode.
      bool important_log =
        entry->get_type() == log::kLogCodeEpochMarker
        ||  log::get_log_code_kind(entry->get_type()) == log::kEngineLogs
        ||  log::get_log_code_kind(entry->get_type()) == log::kStorageLogs;
      if (important_log || enclosure_->verbose_ > kBrief) {
        std::cout << "    <Log offset=\"" << assorted::Hex(offset) << "\""
          << " len=\"" << assorted::Hex(entry->log_length_) << "\""
          << " type=\"" << assorted::Hex(entry->log_type_code_) << "\""
          << " storage_id=\"" << assorted::Hex(entry->storage_id_) << "\"";
        if (entry->log_length_ >= 8U) {
          std::cout << " xct_id_epoch=\"" << entry->xct_id_.get_epoch_int() << "\"";
          std::cout << " xct_id_ordinal=\"" << entry->xct_id_.get_ordinal() << "\"";
        }
        std::cout << ">";
        if (important_log || enclosure_->verbose_ == kDetail) {
          log::invoke_ostream(entry, &std::cout);
        } else {
          std::cout << log::get_log_type_name(entry->get_type());
        }
        std::cout << "</Log>" << std::endl;
      }
    }
    Epoch cur_epoch_;
    DumpLog* enclosure_;
  };

  // the meat part for each file
  DumpCallback callback;
  callback.enclosure_ = this;
  for (uint32_t file_index = 0; file_index < files_.size(); ++file_index) {
    std::cout << "  <LogFile\n     file_index=\"" << file_index
      << "\"\n     path=\"" << files_[file_index]
      << "\"\n     bytes=\"" << assorted::Hex(fs::file_size(files_[file_index]))
      << "\">" << std::endl;
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
  result_cur_epoch_ = INVALID_EPOCH;

  const fs::Path &path = files_[file_index];
  const uint32_t kAlignment = log::FillerLogType::kLogWriteUnitSize;
  uint64_t file_size = fs::file_size(path);
  if (file_size % kAlignment != 0) {
    result_inconsistencies_.emplace_back(
      LogInconsistency(LogInconsistency::kNonAlignedFileEnd, file_index, 0));
    file_size = (file_size / kAlignment) * kAlignment;
  }

  fs::DirectIoFile file(path);
  memory::AlignedMemory buffer(1 << 24, kAlignment, memory::AlignedMemory::kPosixMemalign, 0);
  COERCE_ERROR_CODE(file.open(true, false, false, false));

  uint64_t prev_file_offset = 0;
  uint64_t buffer_size = 0;
  uint64_t buffer_offset = 0;
  while (prev_file_offset + buffer_offset < file_size) {
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
        skip_after_read = buffer_offset % kAlignment;
        new_file_offset = prev_file_offset + buffer_offset - skip_after_read;
        ASSERT_ND(new_file_offset % kAlignment == 0);
      }
    }

    if (need_to_read_file) {
      ASSERT_ND(new_file_offset >= prev_file_offset);
      uint64_t next_reads = std::min(file_size - new_file_offset, buffer.get_size());
      if (next_reads - skip_after_read < header->log_length_) {
        result_inconsistencies_.emplace_back(
          LogInconsistency(LogInconsistency::kIncompleteEntryAtEnd,
                    file_index, cur_offset, *header));
        break;
      }
      ASSERT_ND(next_reads % kAlignment == 0);
      ASSERT_ND(new_file_offset + next_reads <= file_size);
      COERCE_ERROR_CODE(file.seek(new_file_offset, fs::DirectIoFile::kDirectIoSeekSet));
      COERCE_ERROR_CODE(file.read(next_reads, &buffer));
      prev_file_offset = new_file_offset;
      buffer_size = next_reads;
      buffer_offset = skip_after_read;
      continue;
    }

    ASSERT_ND(buffer_size > buffer_offset);
    ASSERT_ND(header->log_length_ <= (buffer_size - buffer_offset));

    if (cur_offset == 0 && header->log_type_code_ != log::kLogCodeEpochMarker) {
      result_inconsistencies_.emplace_back(
        LogInconsistency(LogInconsistency::kNoEpochMarkerAtBeginning, file_index,
                cur_offset, *header));
    }

    if (header->log_length_ == 0 || header->log_length_ % 8 != 0) {
      result_inconsistencies_.emplace_back(
        LogInconsistency(LogInconsistency::kMissingLogLength, file_index, cur_offset,
          *header));
      break;
    }

    if (log::is_valid_log_type(header->get_type())) {
      log::LogCodeKind kind = log::get_log_code_kind(header->get_type());
      if (kind == foedus::log::kStorageLogs || kind == foedus::log::kRecordLogs) {
        if (header->storage_id_ == 0) {
          result_inconsistencies_.emplace_back(
            LogInconsistency(LogInconsistency::kMissingStorageId, file_index,
                    cur_offset, *header));
        }
      }
      if (header->get_type() == log::kLogCodeEpochMarker) {
        log::EpochMarkerLogType *marker
          = reinterpret_cast<log::EpochMarkerLogType*>(header);
        if (!marker->old_epoch_.is_valid()) {
          result_inconsistencies_.emplace_back(LogInconsistency(
            LogInconsistency::kInvalidOldExpoch, file_index, cur_offset, *header));
        }
        if (!marker->new_epoch_.is_valid()) {
          result_inconsistencies_.emplace_back(LogInconsistency(
            LogInconsistency::kInvaligNewEpoch, file_index, cur_offset, *header));
        }
        if (result_cur_epoch_.is_valid() && result_cur_epoch_ != marker->old_epoch_) {
          result_inconsistencies_.emplace_back(LogInconsistency(
            LogInconsistency::kEpochMarkerDoesNotMatch, file_index, cur_offset,
            *header));
        }
        if (marker->log_file_offset_ != cur_offset) {
          result_inconsistencies_.emplace_back(LogInconsistency(
            LogInconsistency::kEpochMarkerIncorrectOffset, file_index, cur_offset,
            *header));
        }
        result_first_epoch_.store_min(marker->new_epoch_);
        result_cur_epoch_ = marker->new_epoch_;
        result_last_epoch_.store_max(marker->new_epoch_);
      }

      callback->process(header, cur_offset);
    } else {
      result_inconsistencies_.emplace_back(
        LogInconsistency(LogInconsistency::kMissingLogLength, file_index, cur_offset,
                 *header));
    }

    buffer_offset += header->log_length_;
    ++result_processed_logs_;
    if (limit_ >= 0 && static_cast<uint64_t>(limit_) <= result_processed_logs_) {
      result_limit_reached_ = true;
      break;
    }
    if (result_inconsistencies_.size() > (1 << 8)) {
      result_inconsistencies_.emplace_back(
        LogInconsistency(LogInconsistency::kTooManyInconsistencies, file_index, 0));
      result_limit_reached_ = true;
      break;
    }
  }

  buffer.release_block();
  file.close();
}

std::ostream& operator<<(std::ostream& o, const LogInconsistency& v) {
  o << "<inconsistency"
    << " file_index=\"" << v.file_index_ << "\""
    << " offset=\"" << assorted::Hex(v.offset_) << "\""
    << " log_type=\"" << v.header_.log_type_code_ << "\""
    << " len=\"" << v.header_.log_length_ << "\""
    << " storage_id=\"" << v.header_.storage_id_ << "\""
    << " code=\"" << assorted::Hex(v.type_)
    << "\" name=\"" << LogInconsistency::type_to_string(v.type_) << "\""
    << " description=\"" << LogInconsistency::type_to_description(v.type_) << "\""
    << " />";
  return o;
}

}  // namespace util
}  // namespace foedus
