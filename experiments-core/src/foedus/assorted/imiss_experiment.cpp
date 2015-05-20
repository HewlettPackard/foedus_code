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

/**
 * CAUTION CAUTION CAUTION!
 * Do not open this file with IDE. It will torture the intellisense parser
 * and potentially crash it.
 */
// @cond DOXYGEN_IGNORE

#include <numa.h>
#include <numaif.h>
#include <papi.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/wait.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include "foedus/debugging/stop_watch.hpp"
#include "foedus/thread/numa_thread_scope.hpp"

#define REP1E(X) X X
// To deliverately cause instruction cache misses, generate a large function.
#define REP2E(X) REP1E(X) REP1E(X)
#define REP3E(X) REP2E(X) REP2E(X)
#define REP4E(X) REP3E(X) REP3E(X)
#define REP5E(X) REP4E(X) REP4E(X)
#define REP6E(X) REP5E(X) REP5E(X)
#define REP7E(X) REP6E(X) REP6E(X)
#define REP8E(X) REP7E(X) REP7E(X)
#define REP9E(X) REP8E(X) REP8E(X)
#define REP10E(X) REP9E(X) REP9E(X)
#define REP11E(X) REP10E(X) REP10E(X)
#define REP12E(X) REP11E(X) REP11E(X)
#define REP13E(X) REP12E(X) REP12E(X)
#define REP14E(X) REP13E(X) REP13E(X)
#define REP15E(X) REP14E(X) REP14E(X)
#define REP16E(X) REP15E(X) REP15E(X)
#define REP17E(X) REP16E(X) REP16E(X)
#define REP18E(X) REP17E(X) REP17E(X)
// #define REP19E(X) REP18E(X) REP18E(X)
// #define REP20E(X) REP19E(X) REP19E(X)
// #define REP21E(X) REP20E(X) REP20E(X)
// #define REP22E(X) REP21E(X) REP21E(X)

int32_t the_stupid_func(int32_t val) {
  REP17E(asm("mov %1, %0\n\t add $1, %0" : "=r" (val) : "r" (val)););
  return val;
}


int nodes;
int processes_per_node;
int reps;

int process_main(int node, int p) {
  foedus::thread::NumaThreadScope scope(node);
  std::cout << "Proc-" << node << "-" << p << " started working on pid-" << ::getpid() << std::endl;
  foedus::debugging::StopWatch watch;
  int val = node + p;
  void* func_ptr = reinterpret_cast<void*>(&the_stupid_func);
  char buffer[1<<16];
  std::memcpy(buffer, func_ptr, sizeof(buffer));
  for (int rep = 0; rep < reps; ++rep) {
    val = the_stupid_func(val);
    std::memcpy(func_ptr, buffer, sizeof(buffer));
  }
  watch.stop();
  std::cout << "Proc-" << node << "-" << p << " took " << watch.elapsed_ms() << "ms."
    " result=" << val << std::endl;
  return 0;
}

int main(int argc, const char** argv) {
  if (argc < 4) {
    std::cerr << "Usage: ./imiss_experiment <nodes> <processes_per_node> <reps>" << std::endl;
    return 1;
  }
  nodes = std::atoi(argv[1]);
  processes_per_node = std::atoi(argv[2]);
  reps = std::atoi(argv[3]);

  int version = ::PAPI_library_init(PAPI_VER_CURRENT);
  int total_counters = ::PAPI_num_counters();
  if (total_counters <= PAPI_OK) {
    std::cerr << "PAPI is not supported in this environment. PAPI runtime version=" << version
      << ", PAPI_VER_CURRENT=" << PAPI_VER_CURRENT << std::endl;
    ::PAPI_shutdown();
    return 1;
  }
  int kPapiEvents[] = { PAPI_L1_ICM, PAPI_L2_ICM, PAPI_TLB_IM };
  const int kPapiEventCount = 3;

  std::cout << "PAPI has " << total_counters << " counters. PAPI runtime version=" << version
    << ", PAPI_VER_CURRENT=" << PAPI_VER_CURRENT << std::endl;
  int papi_stat_ret = ::PAPI_start_counters(kPapiEvents, kPapiEventCount);
  if (papi_stat_ret != PAPI_OK) {
    std::cerr << "PAPI_start_counters failed. retval=" << papi_stat_ret;
    ::PAPI_shutdown();
    return 1;
  } else {
    std::cout << "Started counting " << kPapiEventCount
      << "performance events via PAPI" << std::endl;
  }

  std::vector<pid_t> pids;
  std::vector<bool> exitted;
  for (int node = 0; node < nodes; ++node) {
    for (int p = 0; p < processes_per_node; ++p) {
      pid_t pid = ::fork();
      if (pid == -1) {
        std::cerr << "fork() failed" << std::endl;
        return 1;
      }
      if (pid == 0) {
        return process_main(node, p);
      } else {
        // parent
        std::cout << "child process-" << pid << " has been forked" << std::endl;
        pids.push_back(pid);
        exitted.push_back(false);
      }
    }
  }

  int exit_count = 0;
  while (exit_count < nodes * processes_per_node) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::cout << "Waiting for end... exit_count=" << exit_count << std::endl;
    for (uint16_t i = 0; i < pids.size(); ++i) {
      if (exitted[i]) {
        continue;
      }
      pid_t pid = pids[i];
      int status;
      pid_t result = ::waitpid(pid, &status, WNOHANG);
      if (result == 0) {
        std::cout << "  pid-" << pid << " is still alive.." << std::endl;
      } else if (result == -1) {
        std::cout << "  pid-" << pid << " had an error! quit" << std::endl;
        return 1;
      } else {
        std::cout << "  pid-" << pid << " has exit with status code " << status << std::endl;
        exitted[i] = true;
        ++exit_count;
      }
    }
  }
  std::cout << "All done!" << std::endl;

  long long int counters[kPapiEventCount];  // NOLINT[runtime/int] PAPI API requirement
  ::PAPI_stop_counters(counters, kPapiEventCount);
  std::cout << "PAPI:PAPI_L1_ICM=" << counters[0] << std::endl;
  std::cout << "PAPI:PAPI_L2_ICM=" << counters[1] << std::endl;
  std::cout << "PAPI:PAPI_TLB_IM=" << counters[2] << std::endl;
  return 0;
}

// @endcond

