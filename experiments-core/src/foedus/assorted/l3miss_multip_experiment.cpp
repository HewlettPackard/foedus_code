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

// Tests the cost of L3 cache miss using child processes.
// Each NUMA node is spawned as a child process via fork(2).
// This one uses mmap() even for 2MB pages (otherwise we can't share), so
// make sure you have non-transparent hugepages.
//   sudo sh -c 'echo 196608 > /proc/sys/vm/nr_hugepages'
#include <numa.h>
#include <numaif.h>
#include <unistd.h>
#include <sys/wait.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/memory/shared_memory.hpp"
#include "foedus/thread/numa_thread_scope.hpp"

const uint32_t kRep = 1ULL << 26;

int nodes;
int cores_per_node;
uint64_t mb_per_core;

struct ProcessChannel {
  std::atomic<int>  initialized_count;
  std::atomic<bool> experiment_started;
  std::atomic<int>  exit_count;
};
foedus::memory::SharedMemory process_channel_memory;
ProcessChannel* process_channel;

foedus::memory::SharedMemory* data_memories;

uint64_t run(const char* blocks, foedus::assorted::UniformRandom* rands) {
  uint64_t memory_per_core = mb_per_core << 20;
  uint64_t ret = 0;
  for (uint32_t i = 0; i < kRep; ++i) {
    const char* block = blocks + ((rands->next_uint32() % (memory_per_core >> 6)) << 6);
    block += ret % (1 << 6);
    ret += *block;
  }
  return ret;
}

void main_impl(int id, int node) {
  foedus::thread::NumaThreadScope scope(node);
  uint64_t memory_per_core = mb_per_core << 20;
  char* memory = data_memories[node].get_block();
  memory += (memory_per_core * id);

  foedus::assorted::UniformRandom uniform_random(id);

  ++process_channel->initialized_count;
  while (process_channel->experiment_started.load() == false) {
    continue;
  }
  {
    foedus::debugging::StopWatch stop_watch;
    uint64_t ret = run(memory, &uniform_random);
    stop_watch.stop();
    std::stringstream str;
    str << "Done " << node << "-" << id
      << " (ret=" << ret << ") in " << stop_watch.elapsed_ms() << " ms. "
      << "On average, " << (static_cast<double>(stop_watch.elapsed_ns()) / kRep)
      << " ns/miss" << std::endl;
    std::cout << str.str();
  }
}
int process_main(int node) {
  std::cout << "Node-" << node << " started working on pid-" << ::getpid() << std::endl;
  foedus::thread::NumaThreadScope scope(node);
  std::vector<std::thread> threads;
  for (auto i = 0; i < cores_per_node; ++i) {
    threads.emplace_back(std::thread(main_impl, i, node));
  }
  std::cout << "Node-" << node << " launched " << cores_per_node << " threads" << std::endl;
  for (auto& t : threads) {
    t.join();
  }
  std::cout << "Node-" << node << " ended normally" << std::endl;
  ++process_channel->exit_count;
  return 0;
}
void data_alloc(int node) {
  std::stringstream meta_path;
  meta_path << "/tmp/foedus_l3miss_multip_experiment_" << ::getpid() << "_node_" << node;
  COERCE_ERROR(data_memories[node].alloc(
    meta_path.str(),
    (mb_per_core << 20) * cores_per_node,
    node,
    true));
  std::cout << "Allocated memory for node-" << node << ":"
    << data_memories[node].get_block() << std::endl;
}

int main(int argc, char **argv) {
  if (argc < 4) {
    std::cerr << "Usage: ./l3miss_multip_experiment <nodes> <cores_per_node> <mb_per_core>"
      << std::endl;
    return 1;
  }
  nodes = std::atoi(argv[1]);
  if (nodes == 0 || nodes > ::numa_num_configured_nodes()) {
    std::cerr << "Invalid <nodes>:" << argv[1] << std::endl;
    return 1;
  }
  cores_per_node = std::atoi(argv[2]);
  if (cores_per_node == 0 ||
    cores_per_node > (::numa_num_configured_cpus() / ::numa_num_configured_nodes())) {
    std::cerr << "Invalid <cores_per_node>:" << argv[2] << std::endl;
    return 1;
  }
  mb_per_core  = std::atoi(argv[3]);

  std::cout << "Allocating data memory.." << std::endl;
  data_memories = new foedus::memory::SharedMemory[nodes];
  {
    std::vector<std::thread> alloc_threads;
    for (auto node = 0; node < nodes; ++node) {
      alloc_threads.emplace_back(data_alloc, node);
    }
    for (auto& t : alloc_threads) {
      t.join();
    }
  }
  std::cout << "Allocated all data memory." << std::endl;

  std::stringstream meta_path;
  meta_path << "/tmp/foedus_l3miss_multip_experiment_" << ::getpid() << "_channel";
  COERCE_ERROR(process_channel_memory.alloc(meta_path.str(), 1 << 21, 0, true));
  process_channel = reinterpret_cast<ProcessChannel*>(process_channel_memory.get_block());
  process_channel->initialized_count = 0;
  process_channel->experiment_started = false;
  process_channel->exit_count = 0;
  std::cout << "Allocated channel memory:" << process_channel_memory << std::endl;

  std::vector<pid_t> pids;
  std::vector<bool> exitted;
  for (auto node = 0; node < nodes; ++node) {
    pid_t pid = ::fork();
    if (pid == -1) {
      std::cerr << "fork() failed, error=" << foedus::assorted::os_error() << std::endl;
      return 1;
    }
    if (pid == 0) {
      return process_main(node);
    } else {
      // parent
      std::cout << "child process-" << pid << " has been forked" << std::endl;
      pids.push_back(pid);
      exitted.push_back(false);
    }
  }
  while (process_channel->initialized_count < (nodes * cores_per_node)) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  std::cout << "Child initialization done! Starts the experiment..." << std::endl;
  process_channel->experiment_started.store(true);

  while (process_channel->exit_count < nodes) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::cout << "Waiting for end... exit_count=" << process_channel->exit_count << std::endl;
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
      }
    }
  }
  std::cout << "All done!" << std::endl;
  delete[] data_memories;
  return 0;
}
