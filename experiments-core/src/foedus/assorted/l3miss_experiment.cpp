/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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

const uint64_t kMemory = 12ULL << 30;
const uint32_t kRep = 1ULL << 26;
foedus::thread::Rendezvous start_rendezvous;
std::atomic<int> initialized_count;
std::mutex output_mutex;

int nodes;
int begin_node;
int cores_per_node;
foedus::memory::AlignedMemory::AllocType alloc_type
  = foedus::memory::AlignedMemory::kNumaAllocOnnode;

uint64_t run(const char* blocks, foedus::assorted::UniformRandom* rands) {
  uint64_t memory_per_core = kMemory / nodes;
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
  foedus::memory::AlignedMemory memory;
  uint64_t memory_per_core = kMemory / nodes;
  memory.alloc(memory_per_core, 1ULL << 30, alloc_type, node);

  foedus::assorted::UniformRandom uniform_random(id);

  ++initialized_count;
  start_rendezvous.wait();
  {
    foedus::debugging::StopWatch stop_watch;
    uint64_t ret = run(reinterpret_cast<char*>(memory.get_block()), &uniform_random);
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

int main(int argc, char **argv) {
  if (argc < 4) {
    std::cerr << "Usage: ./l3miss_experiment <nodes> <begin_node> <cores_per_node> [<use_mmap>]"
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
  if (argc >= 5 && std::string(argv[4]) != std::string("false")) {
    alloc_type = foedus::memory::AlignedMemory::kNumaMmapOneGbPages;
  }

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
  start_rendezvous.signal();
  for (auto& t : threads) {
    t.join();
  }
  return 0;
}
