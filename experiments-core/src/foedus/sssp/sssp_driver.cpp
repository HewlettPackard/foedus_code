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
#include "foedus/sssp/sssp_driver.hpp"

#include <numa.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "foedus/assorted/uniform_random.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/sssp/sssp_common.hpp"
#include "foedus/thread/numa_thread_scope.hpp"

namespace foedus {
namespace sssp {

DEFINE_bool(htt, true, "Whether the machine runs hyper-threaded cores. Used only for sizing");
DEFINE_int32(duration, 10, "Duration of the experiments in seconds.");
DEFINE_int32(p_x, 2, "Number of partitions in x direction. Increase this to enlarge data");
DEFINE_int32(p_y, 2, "Number of partitions in y direction. Increase this to enlarge data");

int SsspDriver::main_impl(int argc, char **argv) {
  gflags::SetUsageMessage("SSSP query engine");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (::numa_available() < 0) {
    LOG(ERROR) << "NUMA feature is not available on this machine. Exitting";
    return 1;
  }

  return 0;
}

}  // namespace sssp
}  // namespace foedus

int main(int argc, char **argv) {
  return foedus::sssp::SsspDriver().main_impl(argc, argv);
}
