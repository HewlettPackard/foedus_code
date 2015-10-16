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
#include "foedus/sssp/sssp_load.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <mutex>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/epoch.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/sssp/sssp_common.hpp"
#include "foedus/sssp/sssp_schema.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"


namespace foedus {
namespace sssp {

ErrorStack sssp_load_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  if (args.input_len_ != sizeof(SsspLoadTask::Inputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  const SsspLoadTask::Inputs* inputs = reinterpret_cast<const SsspLoadTask::Inputs*>(
    args.input_buffer_);
  SsspLoadTask task(*inputs);
  return task.run(context);
}

ErrorStack create_all(Engine* engine, uint32_t total_partitions) {
  uint64_t total_nodes = kNodesPerPartition * total_partitions;
  storage::array::ArrayMetadata data_meta("vertex_data", sizeof(Node), total_nodes);
  Epoch ep;
  CHECK_ERROR(engine->get_storage_manager()->create_storage(&data_meta, &ep));

  storage::array::ArrayMetadata bf_meta("vertex_bf", sizeof(VertexBfData), total_nodes);
  CHECK_ERROR(engine->get_storage_manager()->create_storage(&bf_meta, &ep));

  LOG(INFO) << "Created empty tables";
  return kRetOk;
}

ErrorStack SsspLoadTask::run(thread::Thread* context) {
  context_ = context;
  engine_ = context->get_engine();
  storages_.initialize_tables(engine_);
  xct_manager_ = engine_->get_xct_manager();
  debugging::StopWatch watch;
  CHECK_ERROR(load_tables());
  watch.stop();
  LOG(INFO) << "Loaded SSSP tables in " << watch.elapsed_sec() << "sec";
  return kRetOk;
}

ErrorStack SsspLoadTask::load_tables() {
  CHECK_ERROR(load_vertex_bf());
  VLOG(0) << "Loaded vertex_bf:" << engine_->get_memory_manager()->dump_free_memory_stat();

  memory::AlignedMemory partition_buffer;
  partition_buffer.alloc_onnode(kPartitionAlignedByteSize, 1U << 21, context_->get_numa_node());
  VLOG(0) << "Allocated buffer to read partitions. thread=" << context_->get_thread_id();
  for (uint32_t p = inputs_.partition_from_; p < inputs_.partition_to_; ++p) {
    CHECK_ERROR(load_vertex_data(p, reinterpret_cast<Partition*>(partition_buffer.get_block())));
    VLOG(0) << "Loaded vertex_data:" << engine_->get_memory_manager()->dump_free_memory_stat();
  }
  return kRetOk;
}

ErrorCode SsspLoadTask::commit_if_full() {
  if (context_->get_current_xct().get_write_set_size() >= kCommitBatch) {
    Epoch commit_epoch;
    CHECK_ERROR_CODE(xct_manager_->precommit_xct(context_, &commit_epoch));
    CHECK_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kDirtyRead));
  }
  return kErrorCodeOk;
}

ErrorStack SsspLoadTask::load_vertex_bf() {
  LOG(INFO) << "Loading vertex_bf";
  Epoch ep;
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  NodeId from_id = inputs_.partition_from_ * kNodesPerPartition;
  NodeId to_id = inputs_.partition_to_ * kNodesPerPartition;
  for (NodeId id = from_id; id < to_id; ++id) {
    WRAP_ERROR_CODE(storages_.vertex_bf_.overwrite_record_primitive<uint64_t>(
      context_,
      id,
      kEmptyVertexBfData,
      0));
    WRAP_ERROR_CODE(commit_if_full());
  }
  WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &ep));
  return kRetOk;
}


/** Read the data file. The path is hardcoded. lame, but it suffices. */
ErrorStack load_input_file(uint32_t p, uint32_t flags_p_x, uint32_t flags_p_y, Partition* buffer) {
  fs::Path path("/dev/shm/sssp_bin_");
  if (flags_p_x == 1U && flags_p_y == 1U) {
    path += std::string("1x1");
  } else if (flags_p_x == 2U && flags_p_y == 2U) {
    path += std::string("2x2");
  } else if (flags_p_x == 6U && flags_p_y == 8U) {
    path += std::string("6x8");
  } else if (flags_p_x == 12U && flags_p_y == 16U) {
    path += std::string("12x16");
  } else {
    LOG(FATAL) << "Unexpected px_py: " << flags_p_x << "/" << flags_p_y;
  }
  path /= "sssp_out_bin_";

  uint32_t px, py;
  to_px_py(p, flags_p_x, &px, &py);
  path += std::to_string(px);
  path += "_";
  path += std::to_string(py);

  LOG(INFO) << "Loading input file " << path << "...";
  if (!fs::exists(path)) {
    LOG(FATAL) << "Input file " << path << " doesn't exist. WTF!";
  }
  if (fs::file_size(path) != kPartitionAlignedByteSize) {
    LOG(FATAL) << "Input file " << path << " filesize wrong. WTF!";
  }
  fs::DirectIoFile file(path);
  if (file.open(true, false, false, false) != kErrorCodeOk) {
    LOG(FATAL) << "Failed to open input file " << path << ". WTF!";
  }
  file.read_raw(kPartitionAlignedByteSize, buffer);
  file.close();


  if (buffer->px_ != px) {
    LOG(FATAL) << "px doesn't match in " << path << ". WTF!";
  } else if (buffer->py_ != py) {
    LOG(FATAL) << "py doesn't match in " << path << ". WTF!";
  } else if (buffer->pid_ != p) {
    LOG(FATAL) << "pid doesn't match in " << path << ". WTF!";
  }
  LOG(INFO) << "Loaded input file " << path << ".";
  return kRetOk;
}

ErrorStack SsspLoadTask::load_vertex_data(uint32_t p, Partition* buffer) {
  LOG(INFO) << "Loading vertex_data for partition-" << p;
  // Load the whole Partition object from the binary file
  CHECK_ERROR(load_input_file(p, inputs_.max_px_, inputs_.max_py_, buffer));

  // Then, simply go over all nodes and insert.
  Epoch ep;
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  for (uint32_t n = 0; n < kNodesPerPartition; ++n) {
    uint32_t b = n / kNodesPerBlock;
    const Block* block = buffer->blocks_ + b;
    NodeId node_id = n + p * kNodesPerPartition;
    const Node* node = block->nodes_ + (n % kNodesPerBlock);
    if (UNLIKELY(node->id_ != node_id)) {
      LOG(FATAL) << "Node-Id doesn't match " << node_id << ". WTF!";
    }

    WRAP_ERROR_CODE(storages_.vertex_data_.overwrite_record(context_, node_id, node));
    WRAP_ERROR_CODE(commit_if_full());
    if ((n % 20000) == 0) {
      LOG(INFO) << "Partition-" << p << ": " << n << "/" << kNodesPerPartition;
    }
  }
  WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &ep));

  LOG(INFO) << "Loaded vertex_data for partition-" << p;
  return kRetOk;
}

}  // namespace sssp
}  // namespace foedus
