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

// Tests the cost of L3 cache miss.
#include <numa.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/memory_id.hpp"
#include "foedus/thread/numa_thread_scope.hpp"
#include "foedus/thread/rendezvous_impl.hpp"

const uint32_t kRep = 1ULL << 26;
foedus::thread::Rendezvous start_rendezvous;
std::atomic<int> initialized_count;
std::mutex output_mutex;

int nodes;
int begin_node;
int cores_per_node;
uint64_t mb_per_core;

foedus::memory::AlignedMemory* data_memories;

uint64_t run(const char* blocks, foedus::assorted::UniformRandom* rands) {
  uint64_t memory_size = mb_per_core << 20;
  uint64_t ret = 0;
  for (uint32_t i = 0; i < kRep; ++i) {
    const char* block = blocks + ((rands->next_uint32() % (memory_size >> 6)) << 6);
    block += ret % (1 << 6);
    ret += *block;
  }
  return ret;
}

void main_impl(int id, int node) {
  foedus::thread::NumaThreadScope scope(node);
  uint64_t memory_per_core = mb_per_core << 20;
  char* memory = reinterpret_cast<char*>(data_memories[node].get_block());
  memory += (memory_per_core * id);

  foedus::assorted::UniformRandom uniform_random(id);

  ++initialized_count;
  start_rendezvous.wait();
  {
    foedus::debugging::StopWatch stop_watch;
    uint64_t ret = run(memory, &uniform_random);
    stop_watch.stop();
    {
      std::lock_guard<std::mutex> guard(output_mutex);
      std::cout << "Done " << node << "-" << id
        << " (ret=" << ret << ") in " << stop_watch.elapsed_ms() << " ms. "
        << "On average, " << (static_cast<double>(stop_watch.elapsed_ns()) / kRep)
        << " ns/miss" << std::endl;
    }
  }
}

void data_alloc(int node) {
  data_memories[node].alloc(
    (mb_per_core << 20) * cores_per_node,
    1ULL << 30,
    foedus::memory::AlignedMemory::kNumaAllocOnnode,
    node);
  std::cout << "Allocated memory for node-" << node << ":"
    << data_memories[node].get_block() << std::endl;
}


int main(int argc, char **argv) {
  if (argc < 5) {
    std::cerr << "Usage: ./l3miss_experiment <nodes> <begin_node> <cores_per_node> <mb_per_core>"
      << std::endl;
    return 1;
  }
  nodes = std::atoi(argv[1]);
  if (nodes == 0 || nodes > ::numa_num_configured_nodes()) {
    std::cerr << "Invalid <nodes>:" << argv[1] << std::endl;
    return 1;
  }
  begin_node = std::atoi(argv[2]);
  if (begin_node + nodes > ::numa_num_configured_nodes()) {
    std::cerr << "Invalid <begin_node>:" << argv[2] << std::endl;
    return 1;
  }
  cores_per_node = std::atoi(argv[3]);
  if (cores_per_node == 0 ||
    cores_per_node > (::numa_num_configured_cpus() / ::numa_num_configured_nodes())) {
    std::cerr << "Invalid <cores_per_node>:" << argv[3] << std::endl;
    return 1;
  }
  mb_per_core  = std::atoi(argv[4]);

  std::cout << "Allocating data memory.." << std::endl;
  data_memories = new foedus::memory::AlignedMemory[nodes];
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

  initialized_count = 0;
  std::vector<std::thread> threads;
  for (auto node = begin_node; node < begin_node + nodes; ++node) {
    for (auto i = 0; i < cores_per_node; ++i) {
      threads.emplace_back(std::thread(main_impl, i, node));
    }
  }
  while (initialized_count < (nodes * cores_per_node)) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  std::cout << "Experiment has started! now start performance monitors etc" << std::endl;
  start_rendezvous.signal();
  for (auto& t : threads) {
    t.join();
  }
  std::cout << "All done!" << std::endl;
  delete[] data_memories;
  return 0;
}
