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
#include "foedus/epoch.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/util/dump_log.hpp"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <stdint.h>
#include <iostream>
/**
 * @file dump_log.cpp
 * @brief Log Dumper Utility
 * @details
 * Shows the content of specified log file(s) for debugging/trouble-shooting.
 * @todo implement the feature to specify folder rather than file. recursively find log files.
 */
DEFINE_int32(verbose, 0, "Verbosity level of outputs. 0: Shows only per-file metadata and"
  " important logs, 1: Shows all logs without their details. 2: Shows all logs with all details");
DEFINE_int32(limit, 10000, "Maximum number of log entries to show (negative value=no limit).");
DEFINE_int64(from_epoch, 0, "0 means not specified. If specified, start showing logs from"
" this epoch.");
DEFINE_int64(to_epoch, 0, "0 means not specified. If specified, stop showing logs as soon as"
" this epoch appears in the log.");

bool ValidateVerbose(const char* flagname, int32_t value) {
  if (value >= static_cast<int32_t>(foedus::util::DumpLog::kBrief)
      && value <= static_cast<int32_t>(foedus::util::DumpLog::kDetail)) {
    return true;
  } else {
    std::cout << "Invalid value for --" << flagname << ": " << value << std::endl;
    return false;
  }
}

bool ValidateEpoch(const char* flagname, int64_t value) {
  if (value >= 0 && value < (static_cast<int64_t>(1) << 32)) {
    return true;
  } else {
    std::cout << "Invalid value for --" << flagname << ": " << value << std::endl;
    return false;
  }
}

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Log Dumper Utility for libfoedus\n"
    "  Shows the content of specified log file(s) for debugging/trouble-shooting\n"
    "  Usage: foedus_dump_log <flags> <log file(s) or folder(s)>\n"
    "  Example: foedus_dump_log -verbose=1 -limit -1 foedus_node0.log.0\n"
    "  Example2: foedus_dump_log -from_epoch 123 -to_epoch 130 ~/foedus_log/*"
  );
  gflags::RegisterFlagValidator(&FLAGS_verbose,       &ValidateVerbose);
  gflags::RegisterFlagValidator(&FLAGS_from_epoch,    &ValidateEpoch);
  gflags::RegisterFlagValidator(&FLAGS_to_epoch,      &ValidateEpoch);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  foedus::util::DumpLog dump;
  dump.verbose_       = static_cast<foedus::util::DumpLog::Verbosity>(FLAGS_verbose);
  dump.limit_         = FLAGS_limit;
  dump.from_epoch_    = foedus::Epoch(FLAGS_from_epoch);
  dump.to_epoch_      = foedus::Epoch(FLAGS_to_epoch);

  if (argc == 1) {
    std::cerr << "No files specified" << std::endl;
    gflags::ShowUsageWithFlags(argv[0]);
    return 1;
  }

  for (int i = 1; i < argc; ++i) {
    std::string str(argv[i]);
    foedus::fs::Path path(str);
    if (!foedus::fs::exists(path)) {
      std::cerr << "File does not exist: " << str << " (" << path << ")" << std::endl;
      return 1;
    } else if (!foedus::fs::is_regular_file(path)) {
      std::cerr << "Not a regular file: " << str << " (" << path << ")" << std::endl;
      return 1;
    }
    dump.files_.emplace_back(path);
  }

  FLAGS_stderrthreshold = 2;
  FLAGS_minloglevel = 3;
  google::InitGoogleLogging(argv[0]);
  int ret = dump.dump_to_stdout();
  google::ShutdownGoogleLogging();
  return ret;
}
