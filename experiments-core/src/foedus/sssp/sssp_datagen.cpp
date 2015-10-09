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

DEFINE_double(miles_fluke, 0.5, "Max fluctuation of edge length, relative to euclidean distance");
DEFINE_double(edge_prob, 0.8, "Probably for each edge to be instantiated");
DEFINE_int32(p_x, 2, "Number of partitions in x direction. Increase this to enlarge data");
DEFINE_int32(p_y, 2, "Number of partitions in y direction. Increase this to enlarge data");
DEFINE_bool(out_binary, false, "Whether to output partitioned binary files. Text files.");

/**
 * @brief Data generator for the road-like graph microbench.
 * @details
 * This parallelizes the whole data generation, outputting FLAGS_parallel files.
 *
 * The entire data is a 2-d road-like graph. To simplify data generation,
 * it consists of the 3-level hierarchy as below:
 *
 * \li Node: a point whose (int x, int y) is unique.
 * \li Block: a small (tens) collection of nodes in a small square x-y region.
 * \li Partition: a large square x-y region consisting of blocks.
 *
 * The data generation consists of two phases:
 *
 * \li Node generation: all nodes in all partitions' x/y are finalized.
 * \li Edge generation: Using the completely generated nodes, fill out edge info.
 */
struct DataGen {
  int main_impl(int argc, char **argv);

  void generate_nodes();
  void generate_nodes_socket(int socket_id, uint32_t partition_from, uint32_t partition_to);
  void generate_nodes_thread(
    int socket_id,
    int core,
    Partition* partition,
    uint32_t block_from,
    uint32_t block_to);

  void generate_edges();
  void generate_edges_socket(int socket_id, uint32_t partition_from, uint32_t partition_to);
  void generate_edges_thread(
    int socket_id,
    int core,
    Partition* partition,
    uint32_t block_from,
    uint32_t block_to);

  void writeout();
  void writeout_socket(int socket_id, uint32_t partition_from, uint32_t partition_to);
  void writeout_socket_thread(
    int socket_id,
    int core,
    uint32_t partition_from,
    uint32_t partition_to);

  Partition* get_partition(uint32_t px, uint32_t py) const {
    ASSERT_ND(px < static_cast<uint32_t>(FLAGS_p_x));
    ASSERT_ND(py < static_cast<uint32_t>(FLAGS_p_y));
    return partitions_.get()[px + py * FLAGS_p_x];
  }
  Block* get_block(uint32_t bx, uint32_t by) const {
    uint32_t px = bx / kPartitionSize;
    uint32_t py = by / kPartitionSize;
    Partition* partition = get_partition(px, py);
    uint32_t index = (bx % kPartitionSize) + (by % kPartitionSize) * kPartitionSize;
    Block* block = partition->blocks_ + index;
    ASSERT_ND(block->bx_ == bx);
    ASSERT_ND(block->by_ == by);
    return block;
  }
  void set_partition(uint32_t px, uint32_t py, Partition* ptr) {
    partitions_.get()[px + py * FLAGS_p_x] = ptr;
  }
  void to_px_py(uint32_t partition, uint32_t* px, uint32_t* py) const {
    *px = partition % FLAGS_p_x;
    *py = partition / FLAGS_p_x;
  }
  void to_bx_by(Partition* partition, uint32_t block, uint32_t* bx, uint32_t* by) const {
    *bx = partition->px_ * kPartitionSize + (block % kPartitionSize);
    *by = partition->py_ * kPartitionSize + (block / kPartitionSize);
  }

  uint32_t partition_count_;
  uint32_t numa_nodes_;
  uint32_t cores_per_node_;

  // partitions_[px + py * FLAGS_p_x] points to the partition (px, py)
  std::unique_ptr< Partition*[] > partitions_;

  std::unique_ptr< memory::AlignedMemory[] > partition_memories_;
};

int DataGen::main_impl(int argc, char **argv) {
  gflags::SetUsageMessage("SSSP data generator");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (::numa_available() < 0) {
    LOG(ERROR) << "NUMA feature is not available on this machine. Exitting";
    return 1;
  }

  partition_count_ = FLAGS_p_x * FLAGS_p_y;
  partition_memories_.reset(new memory::AlignedMemory[partition_count_]);
  std::memset(partition_memories_.get(), 0, sizeof(memory::AlignedMemory) * partition_count_);
  partitions_.reset(new Partition*[partition_count_]);
  std::memset(partitions_.get(), 0, sizeof(Partition*) * partition_count_);
  numa_nodes_ = ::numa_num_configured_nodes();
  cores_per_node_ = std::thread::hardware_concurrency() / (numa_nodes_ * 2U);  // /2 for HTT

  LOG(INFO) << "Generating data. miles_fluke=" << FLAGS_miles_fluke
    << ", edge_prob=" << FLAGS_edge_prob << ", numa_nodes_=" << numa_nodes_
    << ", cores_per_node_=" << cores_per_node_;

  generate_nodes();
  generate_edges();
  writeout();

  partition_memories_.reset(nullptr);
  partitions_.reset(nullptr);
  return 0;
}

uint32_t calculate_from(uint32_t total, uint32_t splits, uint32_t index) {
  uint32_t count_per_split = total / splits;
  return count_per_split * index;
}

uint32_t calculate_to(uint32_t total, uint32_t splits, uint32_t index) {
  uint32_t count_per_split = total / splits;
  if (index + 1U == splits) {
    // last one takes all
    return total;
  } else {
    return count_per_split * (index + 1U);
  }
}

void DataGen::generate_nodes() {
  std::vector< std::thread > threads;
  for (uint32_t socket_id = 0; socket_id < numa_nodes_; ++socket_id) {
    uint32_t partition_from = calculate_from(partition_count_, numa_nodes_, socket_id);
    uint32_t partition_to = calculate_to(partition_count_, numa_nodes_, socket_id);
    threads.emplace_back(
      &DataGen::generate_nodes_socket,
      this,
      socket_id,
      partition_from,
      partition_to);
  }
  for (auto& t : threads) {
    t.join();
  }

  LOG(INFO) << "Looks like all nodes are generated. Verifying...";
  for (uint32_t p = 0; p < partition_count_; ++p) {
    Partition* partition = partitions_.get()[p];
    ASSERT_ND(partition);
    uint32_t px, py;
    to_px_py(p, &px, &py);
    ASSERT_ND(partition->pid_ == p);
    ASSERT_ND(partition->px_ == px);
    ASSERT_ND(partition->py_ == py);
    for (uint32_t b = 0; b < kBlocksPerPartition; ++b) {
      const Block* block = partition->blocks_ + b;
      for (uint32_t n = 0; n < kNodesPerBlock; ++n) {
        const Node* node = block->nodes_ + n;
        ASSERT_ND(node->id_ == p * kNodesPerPartition + b * kNodesPerBlock + n);
      }
    }
  }
  LOG(INFO) << "Verified!";
}

void DataGen::generate_nodes_socket(int socket_id, uint32_t partition_from, uint32_t partition_to) {
  for (auto p = partition_from; p < partition_to; ++p) {
    uint32_t px, py;
    to_px_py(p, &px, &py);
    LOG(INFO) << "Socket-" << socket_id << ", generating nodes in p-" << p
      << " (px=" << px << ", py=" << py << ")";

    auto* mem = partition_memories_.get();
    mem[p].alloc(
      kPartitionAlignedByteSize,
      1U << 21,
      memory::AlignedMemory::kNumaAllocOnnode,
      socket_id);
    Partition* partition = reinterpret_cast<Partition*>(mem[p].get_block());
    partition->init(px, py, p);
    set_partition(px, py, partition);

    LOG(INFO) << "Socket-" << socket_id << ", allocated memory for p-" << p;
    std::vector< std::thread > threads;
    for (uint32_t core = 0; core < cores_per_node_; ++core) {
      uint32_t block_from = calculate_from(kBlocksPerPartition, cores_per_node_, core);
      uint32_t block_to = calculate_to(kBlocksPerPartition, cores_per_node_, core);
      threads.emplace_back(
        &DataGen::generate_nodes_thread,
        this,
        socket_id,
        core,
        partition,
        block_from,
        block_to);
    }
    for (auto& t : threads) {
      t.join();
    }
  }
  LOG(INFO) << "Socket-" << socket_id << " Nodes done!";
}
void DataGen::generate_nodes_thread(
  int socket_id,
  int core,
  Partition* partition,
  uint32_t block_from,
  uint32_t block_to) {
  thread::NumaThreadScope thread_scope(socket_id);
  Coordinate coordinates[kNodesPerBlock];
  assorted::UniformRandom ur((socket_id * 1234ULL) + core * 567ULL + block_from);
  for (auto b = block_from; b < block_to;) {
    if (b % (1 << 13) == 0) {
      VLOG(0) << "Generating block... " << socket_id << "-" << core
        << ": " << b << " (" << block_from << "~" << block_to << ")";
    }

    Block* block = partition->blocks_ + b;
    uint32_t bx, by;
    to_bx_by(partition, b, &bx, &by);
    block->init(bx, by);
    for (uint32_t i = 0; i < kNodesPerBlock; ++i) {
      coordinates[i].x_ = block->x_ + ((ur.next_uint32() >> 6) % kBlockSize);
      coordinates[i].y_ = block->y_ + ((ur.next_uint32() >> 6) % kBlockSize);
    }

    // Sort by x -> y
    std::sort(coordinates, coordinates + kNodesPerBlock);

    // Not quite smartest thing to do, but just check duplicate and redo whenever found.
    // This is quite rare
    bool has_duplicate = false;
    for (uint32_t i = 1; i < kNodesPerBlock; ++i) {
      if (coordinates[i - 1] == coordinates[i]) {
        has_duplicate = true;
        break;
      }
    }

    if (has_duplicate) {
      DVLOG(1) << "Oops, a rare duplicate found. redoing this block";
      continue;  // before incrementing b.
    }

    // Okay, set them to block
    for (uint32_t i = 0; i < kNodesPerBlock; ++i) {
      block->nodes_[i].coordinate_ = coordinates[i];
      block->nodes_[i].edge_count_ = 0;
      block->nodes_[i].id_ = partition->pid_ * kNodesPerPartition + b * kNodesPerBlock + i;
      std::memset(block->nodes_[i].edges_, 0, sizeof(block->nodes_[i].edges_));
    }

    ++b;
  }
}

void DataGen::generate_edges() {
  std::vector< std::thread > threads;
  for (uint32_t socket_id = 0; socket_id < numa_nodes_; ++socket_id) {
    uint32_t partition_from = calculate_from(partition_count_, numa_nodes_, socket_id);
    uint32_t partition_to = calculate_to(partition_count_, numa_nodes_, socket_id);
    threads.emplace_back(
      &DataGen::generate_edges_socket,
      this,
      socket_id,
      partition_from,
      partition_to);
  }
  for (auto& t : threads) {
    t.join();
  }
}

void DataGen::generate_edges_socket(int socket_id, uint32_t partition_from, uint32_t partition_to) {
  for (uint32_t p = partition_from; p < partition_to; ++p) {
    LOG(INFO) << "Socket-" << socket_id << ", generating edges in p-" << p;
    Partition* partition = partitions_.get()[p];

    std::vector< std::thread > threads;
    for (uint32_t core = 0; core < cores_per_node_; ++core) {
      uint32_t block_from = calculate_from(kBlocksPerPartition, cores_per_node_, core);
      uint32_t block_to = calculate_to(kBlocksPerPartition, cores_per_node_, core);
      threads.emplace_back(
        &DataGen::generate_edges_thread,
        this,
        socket_id,
        core,
        partition,
        block_from,
        block_to);
    }
    for (auto& t : threads) {
      t.join();
    }
  }
  LOG(INFO) << "Socket-" << socket_id << " Edges done!";
}

void DataGen::generate_edges_thread(
  int socket_id,
  int core,
  Partition* partition,
  uint32_t block_from,
  uint32_t block_to) {
  thread::NumaThreadScope thread_scope(socket_id);
  assorted::UniformRandom ur((socket_id * 1234ULL) + core * 567ULL + block_from);

  EdgeCandidate ranking[kMaxEdges];
  uint32_t ranking_count;
  for (auto b = block_from; b < block_to; ++b) {
    if (b % (1 << 13) == 0) {
      VLOG(0) << "Generating edges from blocks... " << socket_id << "-" << core
        << ": " << b << " (" << block_from << "~" << block_to << ")";
    }

    // The algorithm is super simple.
    // We consider all nodes in this block and all neighboring blocks.
    // Then we pick up kMaxEdges closest neighbor
    Block* my_block = partition->blocks_ + b;
    uint32_t max_bx = kPartitionSize * FLAGS_p_x - 1U;
    uint32_t max_by = kPartitionSize * FLAGS_p_y - 1U;

    for (uint32_t i = 0; i < kNodesPerBlock; ++i) {
      Node* my_node = my_block->nodes_ + i;
      ranking_count = 0;
      for (int y_diff = -1; y_diff <= 1; ++y_diff) {
        for (int x_diff = -1; x_diff <= 1; ++x_diff) {
          if (x_diff == -1 && my_block->bx_ == 0) {
            continue;
          } else if (y_diff == -1 && my_block->by_ == 0) {
            continue;
          } else if (x_diff == 1 && my_block->bx_ == max_bx) {
            continue;
          } else if (y_diff == 1 && my_block->by_ == max_by) {
            continue;
          }

          uint32_t other_bx = my_block->bx_ + x_diff;
          uint32_t other_by = my_block->by_ + y_diff;
          const Block* other_block = get_block(other_bx, other_by);

          for (uint32_t j = 0; j < kNodesPerBlock; ++j) {
            if (y_diff == 0 && x_diff == 0 && i == j) {
              ASSERT_ND(other_block->nodes_ + j == my_node);
              continue;
            }
            const Node* other_node = other_block->nodes_ + j;
            EdgeCandidate new_entry(*my_node, *other_node);

            if (ranking_count == kMaxEdges) {
              if (new_entry < ranking[ranking_count - 1]) {
                --ranking_count;
              } else {
                continue;
              }
            }

            ASSERT_ND(ranking_count < kMaxEdges);

            // Where should we insert? kMaxEdges is small, so sequential search is
            // faster than std::lower_bound.
            uint32_t insert_at = 0;
            for (; insert_at < ranking_count; ++insert_at) {
              if (new_entry < ranking[insert_at]) {
                break;
              }
            }
            for (uint32_t move_to = ranking_count; move_to > insert_at; --move_to) {
              ranking[move_to] = ranking[move_to - 1];
            }
            ranking[insert_at] = new_entry;
            ++ranking_count;
          }
        }
      }

      ASSERT_ND(ranking_count == kMaxEdges);
      uint32_t edge_count = 0;
      for (uint32_t edge = 0; edge < kMaxEdges; ++edge) {
        double prob = ur.next_double();
        if (prob > FLAGS_edge_prob) {
          continue;
        }
        my_node->edges_[edge_count].to_ = ranking[edge].to_;
        double miles_mlt = 1.0d + ur.next_double() * FLAGS_miles_fluke;
        my_node->edges_[edge_count].mileage_ = std::sqrt(ranking[edge].distance_sq_) * miles_mlt;
        ++edge_count;
      }
      my_node->edge_count_ = edge_count;
      // Sort them by to_
      std::sort(my_node->edges_, my_node->edges_ + edge_count);
    }
  }
}

void DataGen::writeout() {
  fs::Path folder("/dev/shm");
  LOG(INFO) << "Deleting old files...";
  std::vector< fs::Path > existing_files = folder.child_paths();
  for (fs::Path& path : existing_files) {
    std::string name(path.string());
    if (fs::is_regular_file(path) && name.find("sssp_out_") != std::string::npos) {
      if (fs::remove(path)) {
        LOG(INFO) << "Deleted " << path;
      } else {
        LOG(FATAL) << "Failed to delete " << path << ". Permission issue? Exitting";
      }
    }
  }

  std::vector< std::thread > threads;
  for (uint32_t socket_id = 0; socket_id < numa_nodes_; ++socket_id) {
    uint32_t partition_from = calculate_from(partition_count_, numa_nodes_, socket_id);
    uint32_t partition_to = calculate_to(partition_count_, numa_nodes_, socket_id);
    threads.emplace_back(
      &DataGen::writeout_socket,
      this,
      socket_id,
      partition_from,
      partition_to);
  }
  for (auto& t : threads) {
    t.join();
  }
}

void DataGen::writeout_socket(int socket_id, uint32_t partition_from, uint32_t partition_to) {
  const uint32_t sub_count = partition_to - partition_from;
  if (sub_count == 0) {
    LOG(INFO) << "No work";
    return;
  }

  std::vector< std::thread > threads;
  for (uint32_t core = 0; core < cores_per_node_; ++core) {
    uint32_t core_from = partition_from + calculate_from(sub_count, cores_per_node_, core);
    uint32_t core_to = partition_from + calculate_to(sub_count, cores_per_node_, core);
    if (core_from == core_to) {
      continue;
    }
    threads.emplace_back(
      &DataGen::writeout_socket_thread,
      this,
      socket_id,
      core,
      core_from,
      core_to);
  }
  for (auto& t : threads) {
    t.join();
  }
  LOG(INFO) << "Socket-" << socket_id << " wrote out files!";
}

const uint32_t kTextScratchSize = 1U << 25;
void flush_text_file(fs::DirectIoFile* file, char* text_buffer, uint32_t* text_buffer_pos) {
  uint32_t flush_pos = ((*text_buffer_pos) >> 12U) << 12U;
  uint32_t move_bytes = (*text_buffer_pos) - flush_pos;
  COERCE_ERROR_CODE(file->write_raw(flush_pos, text_buffer));
  std::memcpy(text_buffer, text_buffer + flush_pos, move_bytes);
  *text_buffer_pos = move_bytes;
}

uint32_t write_text_file(fs::DirectIoFile* file, const Partition* partition, char* text_buffer) {
  uint32_t text_buffer_pos = 0;
  const uint32_t pos_threshold = kTextScratchSize * 8U / 10U;
  for (uint32_t b = 0; b < kBlocksPerPartition; ++b) {
    const Block* block = partition->blocks_ + b;
    for (uint32_t n = 0; n < kNodesPerBlock; ++n) {
      const Node* node = block->nodes_ + n;
      ASSERT_ND(node->id_ == partition->pid_ * kNodesPerPartition + b * kNodesPerBlock + n);
      for (uint32_t e = 0; e < node->edge_count_; ++e) {
        const Edge* edge = node->edges_ + e;
        // from_id, to_id, mileage
        uint32_t written = std::sprintf(
          text_buffer + text_buffer_pos,
          "%d,%d,%d\n",
          node->id_,
          edge->to_,
          edge->mileage_);
        text_buffer_pos += written;
      }

      ASSERT_ND(text_buffer_pos <= kTextScratchSize);
      if (text_buffer_pos > pos_threshold) {
        flush_text_file(file, text_buffer, &text_buffer_pos);
      }
      ASSERT_ND(text_buffer_pos <= pos_threshold);
    }
  }

  // fill the last 4k with LF so that we can easily write out
  uint32_t fill_bytes = (((text_buffer_pos >> 12) + 1U) << 12) - text_buffer_pos;
  std::memset(text_buffer + text_buffer_pos, '\n', fill_bytes);
  uint32_t flush_pos = text_buffer_pos + fill_bytes;
  ASSERT_ND((flush_pos % (1U << 12)) == 0);
  COERCE_ERROR_CODE(file->write_raw(flush_pos, text_buffer));
  return fill_bytes;
}

void DataGen::writeout_socket_thread(
  int socket_id,
  int core,
  uint32_t partition_from,
  uint32_t partition_to) {
  memory::AlignedMemory text_scratch_memory;
  char* text_buffer = nullptr;
  if (!FLAGS_out_binary) {
    text_scratch_memory.alloc_onnode(kTextScratchSize, 1U << 21, socket_id);
    text_buffer = reinterpret_cast<char*>(text_scratch_memory.get_block());
  }

  for (auto p = partition_from; p < partition_to; ++p) {
    uint32_t px, py;
    to_px_py(p, &px, &py);

    fs::Path path("/dev/shm/sssp_out_");
    path += FLAGS_out_binary ? "bin_" : "text_";
    path += std::to_string(px);
    path += "_";
    path += std::to_string(py);

    LOG(INFO) << "Core-" << socket_id << "_" << core << " writing out " << path;
    const Partition* partition = get_partition(px, py);

    fs::DirectIoFile file(path);
    if (file.open(true, true, true, true) != kErrorCodeOk) {
      LOG(FATAL) << "Core-" << socket_id << "_" << core << " failed to create " << path;
    }

    uint32_t text_fill_bytes = 0;
    if (FLAGS_out_binary) {
      // If we are writing out a binary file, we just dump out the whole struct. that's it.
      file.write_raw(kPartitionAlignedByteSize, partition);
    } else {
      // Text conversion.. ugggrrr
      text_fill_bytes = write_text_file(&file, partition, text_buffer);
    }

    file.sync();
    file.close();

    // Just for convenience, let's remove extra LFs at the end. Just
    if (text_fill_bytes > 0) {
      uint64_t size = fs::file_size(path);
      ASSERT_ND(size % (1U << 12) == 0);
      ::truncate(path.c_str(), size - text_fill_bytes);
    }
  }
}

}  // namespace sssp
}  // namespace foedus


int main(int argc, char **argv) {
  return foedus::sssp::DataGen().main_impl(argc, argv);
}
