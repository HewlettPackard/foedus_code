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
#include "foedus/soc/shared_memory_repo.hpp"

#include <unistd.h>

#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/partitioner.hpp"

namespace foedus {
namespace soc {

std::string get_self_path(uint64_t upid, Eid eid) {
  std::string pid_str = std::to_string(upid);
  std::string eid_str = std::to_string(eid);
  return std::string("/tmp/libfoedus_shm_") + pid_str + std::string("_") + eid_str;
}
std::string get_master_path(uint64_t master_upid, Eid master_eid) {
  std::string pid_str = std::to_string(master_upid);
  std::string eid_str = std::to_string(master_eid);
  return std::string("/tmp/libfoedus_shm_") + pid_str + std::string("_") + eid_str;
}

void NodeMemoryAnchors::allocate_arrays(const EngineOptions& options) {
  deallocate_arrays();
  logger_memories_ = new log::LoggerControlBlock*[options.log_.loggers_per_node_];
  thread_anchors_ = new ThreadMemoryAnchors[options.thread_.thread_count_per_group_];
}

void NodeMemoryAnchors::deallocate_arrays() {
  if (logger_memories_) {
    delete[] logger_memories_;
    logger_memories_ = nullptr;
  }
  if (thread_anchors_) {
    delete[] thread_anchors_;
    thread_anchors_ = nullptr;
  }
}

uint64_t align_4kb(uint64_t value) { return assorted::align< uint64_t, (1U << 12) >(value); }
uint64_t align_2mb(uint64_t value) { return assorted::align< uint64_t, (1U << 21) >(value); }

void SharedMemoryRepo::allocate_one_node(
  uint64_t upid,
  Eid eid,
  uint16_t node,
  uint64_t node_memory_size,
  bool rigorous_memory_boundary_check,
  bool rigorous_page_boundary_check,
  ErrorStack* alloc_result,
  SharedMemoryRepo* repo) {
  // NEVER do COERCE_ERROR here. We must responsibly release shared memory even on errors.
  std::string node_memory_path
    = get_self_path(upid, eid) + std::string("_node_") + std::to_string(node);
  bool use_hugepages = true;
  if (rigorous_memory_boundary_check || rigorous_page_boundary_check) {
    // when mprotect is enabled, we cannot use hugepages
    use_hugepages = false;
  }
  *alloc_result = repo->node_memories_[node].alloc(
    node_memory_path,
    node_memory_size,
    node,
    use_hugepages);
  if (alloc_result->is_error()) {
    repo->node_memories_[node].release_block();
    return;
  }
}

ErrorStack SharedMemoryRepo::allocate_shared_memories(
  uint64_t upid,
  Eid eid,
  const EngineOptions& options) {
  deallocate_shared_memories();
  init_empty(options);

  // We place a serialized EngineOptions in the beginning of shared memory.
  std::stringstream options_stream;
  options.save_to_stream(&options_stream);
  std::string xml(options_stream.str());
  uint64_t xml_size = xml.size();

  // construct unique meta files using PID.
  uint64_t global_memory_size = align_2mb(calculate_global_memory_size(xml_size, options));
  std::string global_memory_path = get_self_path(upid, eid) + std::string("_global");
  const bool global_hugepages = !options.memory_.rigorous_memory_boundary_check_;
  CHECK_ERROR(global_memory_.alloc(global_memory_path, global_memory_size, 0, global_hugepages));

  // from now on, be very careful to not exit without releasing this shared memory.

  set_global_memory_anchors(xml_size, options, true);
  global_memory_anchors_.master_status_memory_->status_code_ = MasterEngineStatus::kInitial;

  // copy the EngineOptions string into the beginning of the global memory
  std::memcpy(global_memory_.get_block(), &xml_size, sizeof(xml_size));
  std::memcpy(global_memory_.get_block() + sizeof(xml_size), xml.data(), xml_size);

  // the following is parallelized
  uint64_t node_memory_size = align_2mb(calculate_node_memory_size(options));
  ErrorStack alloc_results[kMaxSocs];
  std::vector< std::thread > alloc_threads;
  for (uint16_t node = 0; node < soc_count_; ++node) {
    alloc_threads.emplace_back(std::thread(
      SharedMemoryRepo::allocate_one_node,
      upid,
      eid,
      node,
      node_memory_size,
      options.memory_.rigorous_memory_boundary_check_,
      options.memory_.rigorous_page_boundary_check_,
      alloc_results + node,
      this));
  }

  ErrorStack last_error;
  bool failed = false;
  for (uint16_t node = 0; node < soc_count_; ++node) {
    alloc_threads[node].join();
    if (alloc_results[node].is_error()) {
      std::cerr << "[FOEDUS] Failed to allocate node shared memory for node-" << node
        << ". " << alloc_results[node] << std::endl;
      last_error = alloc_results[node];
      failed = true;
    }
  }

  if (failed) {
    deallocate_shared_memories();
    return last_error;
  }

  for (uint16_t node = 0; node < soc_count_; ++node) {
    set_node_memory_anchors(node, options, true);
  }

  return kRetOk;
}

ErrorStack SharedMemoryRepo::attach_shared_memories(
  uint64_t master_upid,
  Eid master_eid,
  SocId my_soc_id,
  EngineOptions* options) {
  deallocate_shared_memories();

  std::string base = get_master_path(master_upid, master_eid);
  std::string global_memory_path = base + std::string("_global");
  const bool global_hugepages = !options->memory_.rigorous_memory_boundary_check_;
  global_memory_.attach(global_memory_path, global_hugepages);
  if (global_memory_.is_null()) {
    deallocate_shared_memories();
    return ERROR_STACK(kErrorCodeSocShmAttachFailed);
  }

  // read the options from global_memory
  uint64_t xml_size = 0;
  std::memcpy(&xml_size, global_memory_.get_block(), sizeof(xml_size));
  ASSERT_ND(xml_size > 0);
  std::string xml(global_memory_.get_block() + sizeof(xml_size), xml_size);
  CHECK_ERROR(options->load_from_string(xml));

  my_soc_id_ = my_soc_id;
  init_empty(*options);
  set_global_memory_anchors(xml_size, *options, false);

  bool failed = false;
  for (uint16_t node = 0; node < soc_count_; ++node) {
    std::string node_memory_str = base + std::string("_node_") + std::to_string(node);
    node_memories_[node].attach(node_memory_str, !options->memory_.rigorous_memory_boundary_check_);
    if (node_memories_[node].is_null()) {
      failed = true;
    } else {
      set_node_memory_anchors(node, *options, false);
    }
  }

  if (failed) {
    if (!node_memories_[my_soc_id].is_null()) {
      // then we can at least notify the error via the shared memory
      change_child_status(my_soc_id, ChildEngineStatus::kFatalError);
    }
    deallocate_shared_memories();
    return ERROR_STACK(kErrorCodeSocShmAttachFailed);
  }
  return kRetOk;
}

void SharedMemoryRepo::mark_for_release() {
  // mark_for_release() is idempotent, so just do it on all of them
  global_memory_.mark_for_release();
  for (uint16_t i = 0; i < soc_count_; ++i) {
    if (node_memories_) {
      node_memories_[i].mark_for_release();
    }
  }
}
void SharedMemoryRepo::deallocate_shared_memories() {
  mark_for_release();

  if (!global_memory_.is_null()) {
    if (global_memory_anchors_.protected_boundaries_needs_release_) {
      for (uint32_t i = 0; i < global_memory_anchors_.protected_boundaries_count_; ++i) {
        assorted::ProtectedBoundary* boundary = global_memory_anchors_.protected_boundaries_[i];
        boundary->release_protect();
        boundary->assert_boundary();
      }
    }
  }
  global_memory_anchors_.clear();

  // release_block() is idempotent, so just do it on all of them
  global_memory_.release_block();

  for (uint16_t i = 0; i < soc_count_; ++i) {
    if (node_memories_) {
      if (node_memory_anchors_[i].protected_boundaries_needs_release_) {
        for (uint32_t j = 0; j < node_memory_anchors_[i].protected_boundaries_count_; ++j) {
          assorted::ProtectedBoundary* boundary = node_memory_anchors_[i].protected_boundaries_[j];
          boundary->release_protect();
          boundary->assert_boundary();
        }
      }

      node_memories_[i].release_block();
    }
  }

  if (node_memories_) {
    delete[] node_memories_;
    node_memories_ = nullptr;
  }
  if (node_memory_anchors_) {
    delete[] node_memory_anchors_;
    node_memory_anchors_ = nullptr;
  }
  soc_count_ = 0;
}

void SharedMemoryRepo::init_empty(const EngineOptions& options) {
  soc_count_ = options.thread_.group_count_;
  node_memories_ = new memory::SharedMemory[soc_count_];
  node_memory_anchors_ = new NodeMemoryAnchors[soc_count_];
  for (uint16_t node = 0; node < soc_count_; ++node) {
    node_memory_anchors_[node].allocate_arrays(options);
  }
}

void SharedMemoryRepo::set_global_memory_anchors(
  uint64_t xml_size,
  const EngineOptions& options,
  bool reset_boundaries) {
  char* base = global_memory_.get_block();
  uint64_t total = 0;
  global_memory_anchors_.options_xml_length_ = xml_size;
  global_memory_anchors_.options_xml_ = base + sizeof(uint64_t);
  total += align_4kb(sizeof(uint64_t) + xml_size);
  put_global_memory_boundary(&total, "options_xml_boundary", reset_boundaries);

  global_memory_anchors_.master_status_memory_
    = reinterpret_cast<MasterEngineStatus*>(base + total);
  total += GlobalMemoryAnchors::kMasterStatusMemorySize;
  put_global_memory_boundary(&total, "master_status_memory_boundary", reset_boundaries);

  global_memory_anchors_.log_manager_memory_
    = reinterpret_cast<log::LogManagerControlBlock*>(base + total);
  total += GlobalMemoryAnchors::kLogManagerMemorySize;
  put_global_memory_boundary(&total, "log_manager_memory_boundary", reset_boundaries);

  global_memory_anchors_.meta_logger_memory_
    = reinterpret_cast<log::MetaLogControlBlock*>(base + total);
  total += GlobalMemoryAnchors::kMetaLoggerSize;
  put_global_memory_boundary(&total, "meta_logger_memory_boundary", reset_boundaries);

  global_memory_anchors_.restart_manager_memory_
    = reinterpret_cast<restart::RestartManagerControlBlock*>(base + total);
  total += GlobalMemoryAnchors::kRestartManagerMemorySize;
  put_global_memory_boundary(&total, "restart_manager_memory_boundary", reset_boundaries);

  global_memory_anchors_.savepoint_manager_memory_
    = reinterpret_cast<savepoint::SavepointManagerControlBlock*>(base + total);
  total += GlobalMemoryAnchors::kSavepointManagerMemorySize;
  put_global_memory_boundary(&total, "savepoint_manager_memory_boundary", reset_boundaries);

  global_memory_anchors_.snapshot_manager_memory_
    = reinterpret_cast<snapshot::SnapshotManagerControlBlock*>(base + total);
  total += GlobalMemoryAnchors::kSnapshotManagerMemorySize;
  put_global_memory_boundary(&total, "snapshot_manager_memory_boundary", reset_boundaries);

  global_memory_anchors_.storage_manager_memory_
    = reinterpret_cast<storage::StorageManagerControlBlock*>(base + total);
  total += GlobalMemoryAnchors::kStorageManagerMemorySize;
  put_global_memory_boundary(&total, "storage_manager_memory_boundary", reset_boundaries);

  global_memory_anchors_.xct_manager_memory_
    = reinterpret_cast<xct::XctManagerControlBlock*>(base + total);
  total += GlobalMemoryAnchors::kXctManagerMemorySize;
  put_global_memory_boundary(&total, "xct_manager_memory_boundary", reset_boundaries);

  global_memory_anchors_.partitioner_metadata_
    = reinterpret_cast<storage::PartitionerMetadata*>(base + total);
  total += align_4kb(sizeof(storage::PartitionerMetadata) * options.storage_.max_storages_);
  put_global_memory_boundary(&total, "partitioner_metadata_boundary", reset_boundaries);
  global_memory_anchors_.partitioner_data_ = base + total;
  total += static_cast<uint64_t>(options.storage_.partitioner_data_memory_mb_) << 20;
  put_global_memory_boundary(&total, "partitioner_data_boundary", reset_boundaries);

  global_memory_anchors_.storage_name_sort_memory_
    = reinterpret_cast<storage::StorageId*>(base + total);
  total += align_4kb(sizeof(storage::StorageId) * options.storage_.max_storages_);
  put_global_memory_boundary(&total, "storage_name_sort_memory_boundary", reset_boundaries);

  global_memory_anchors_.storage_memories_
    = reinterpret_cast<storage::StorageControlBlock*>(base + total);
  total += static_cast<uint64_t>(GlobalMemoryAnchors::kStorageMemorySize)
    * options.storage_.max_storages_;
  put_global_memory_boundary(&total, "storage_memories_boundary", reset_boundaries);

  global_memory_anchors_.user_memory_ = base + total;
  total += align_4kb(1024ULL * options.soc_.shared_user_memory_size_kb_);
  put_global_memory_boundary(&total, "user_memory_boundary", reset_boundaries);

  // we have to be super careful here. let's not use assertion.
  if (calculate_global_memory_size(xml_size, options) != total) {
    std::cerr << "[FOEDUS] global memory size doesn't match. bug?"
      << " allocated=" << calculate_global_memory_size(xml_size, options)
      << ", expected=" << total << std::endl;
  }

  if (options.memory_.rigorous_memory_boundary_check_) {
    // this might mean mprotect() multiple times when SOCs are emulated SOCs.
    // however, acquire_protect() is idempotent, hence it's fine.
    for (assorted::ProtectedBoundary* boundary : global_memory_anchors_.protected_boundaries_) {
      boundary->acquire_protect();
    }
    global_memory_anchors_.protected_boundaries_needs_release_ = true;
  }
}

uint64_t SharedMemoryRepo::calculate_global_memory_size(
  uint64_t xml_size,
  const EngineOptions& options) {
  const uint64_t kBoundarySize = sizeof(assorted::ProtectedBoundary);
  uint64_t total = 0;
  total += align_4kb(sizeof(xml_size) + xml_size) + kBoundarySize;  // options_xml_
  total += GlobalMemoryAnchors::kMasterStatusMemorySize + kBoundarySize;
  total += GlobalMemoryAnchors::kLogManagerMemorySize + kBoundarySize;
  total += GlobalMemoryAnchors::kMetaLoggerSize + kBoundarySize;
  total += GlobalMemoryAnchors::kRestartManagerMemorySize + kBoundarySize;
  total += GlobalMemoryAnchors::kSavepointManagerMemorySize + kBoundarySize;
  total += GlobalMemoryAnchors::kSnapshotManagerMemorySize + kBoundarySize;
  total += GlobalMemoryAnchors::kStorageManagerMemorySize + kBoundarySize;
  total += GlobalMemoryAnchors::kXctManagerMemorySize + kBoundarySize;
  total +=
    align_4kb(sizeof(storage::PartitionerMetadata) * options.storage_.max_storages_)
    + kBoundarySize;
  total +=
    (static_cast<uint64_t>(options.storage_.partitioner_data_memory_mb_) << 20)
     + kBoundarySize;
  total +=
    align_4kb(sizeof(storage::StorageId) * options.storage_.max_storages_)
    + kBoundarySize;
  total +=
    static_cast<uint64_t>(GlobalMemoryAnchors::kStorageMemorySize) * options.storage_.max_storages_
    + kBoundarySize;
  total += align_4kb(1024ULL * options.soc_.shared_user_memory_size_kb_) + kBoundarySize;
  return total;
}

void SharedMemoryRepo::set_node_memory_anchors(
  SocId node,
  const EngineOptions& options,
  bool reset_boundaries) {
  char* base = node_memories_[node].get_block();
  NodeMemoryAnchors& anchor = node_memory_anchors_[node];
  uint64_t total = 0;
  anchor.child_status_memory_ = reinterpret_cast<ChildEngineStatus*>(base);
  total += NodeMemoryAnchors::kChildStatusMemorySize;
  put_node_memory_boundary(node, &total, "node_child_status_memory_boundary", reset_boundaries);

  anchor.volatile_pool_status_ = reinterpret_cast<memory::PagePoolControlBlock*>(base + total);
  total += NodeMemoryAnchors::kPagePoolMemorySize;
  put_node_memory_boundary(node, &total, "node_volatile_pool_status_boundary", reset_boundaries);

  anchor.proc_manager_memory_ = reinterpret_cast<proc::ProcManagerControlBlock*>(base + total);
  total += NodeMemoryAnchors::kProcManagerMemorySize;
  put_node_memory_boundary(node, &total, "node_proc_manager_memory_boundary", reset_boundaries);

  anchor.proc_memory_ = reinterpret_cast<proc::ProcAndName*>(base + total);
  total += align_4kb(sizeof(proc::ProcAndName) * options.proc_.max_proc_count_);
  put_node_memory_boundary(node, &total, "node_proc_memory_boundary", reset_boundaries);

  anchor.proc_name_sort_memory_ = reinterpret_cast<proc::LocalProcId*>(base + total);
  total += align_4kb(sizeof(proc::LocalProcId) * options.proc_.max_proc_count_);
  put_node_memory_boundary(node, &total, "node_proc_name_sort_memory_boundary", reset_boundaries);

  anchor.log_reducer_memory_ = reinterpret_cast<snapshot::LogReducerControlBlock*>(base + total);
  total += NodeMemoryAnchors::kLogReducerMemorySize;
  put_node_memory_boundary(node, &total, "node_log_reducer_memory_boundary", reset_boundaries);

  anchor.log_reducer_root_info_pages_ = reinterpret_cast<storage::Page*>(base + total);
  total += options.storage_.max_storages_ * 4096ULL;
  put_node_memory_boundary(
    node,
    &total,
    "node_log_reducer_root_info_pages_boundary",
    reset_boundaries);

  for (uint16_t i = 0; i < options.log_.loggers_per_node_; ++i) {
    anchor.logger_memories_[i] = reinterpret_cast<log::LoggerControlBlock*>(base + total);
    total += NodeMemoryAnchors::kLoggerMemorySize;
    put_node_memory_boundary(node, &total, "node_logger_memories_boundary", reset_boundaries);
  }

  for (uint16_t i = 0; i < options.thread_.thread_count_per_group_; ++i) {
    ThreadMemoryAnchors& thread_anchor = anchor.thread_anchors_[i];
    thread_anchor.thread_memory_ = reinterpret_cast<thread::ThreadControlBlock*>(base + total);
    total += ThreadMemoryAnchors::kThreadMemorySize;
    put_node_memory_boundary(node, &total, "thread_memory_boundary", reset_boundaries);

    thread_anchor.task_input_memory_ = base + total;
    total += ThreadMemoryAnchors::kTaskInputMemorySize;
    put_node_memory_boundary(node, &total, "thread_task_input_memory_boundary", reset_boundaries);

    thread_anchor.task_output_memory_ = base + total;
    total += ThreadMemoryAnchors::kTaskOutputMemorySize;
    put_node_memory_boundary(node, &total, "thread_task_output_memory_boundary", reset_boundaries);

    thread_anchor.mcs_lock_memories_ = reinterpret_cast<xct::McsBlock*>(base + total);
    total += ThreadMemoryAnchors::kMcsLockMemorySize;
    put_node_memory_boundary(node, &total, "thread_mcs_lock_memories_boundary", reset_boundaries);

    thread_anchor.mcs_rw_lock_memories_ = reinterpret_cast<xct::McsRwBlock*>(base + total);
    total += ThreadMemoryAnchors::kMcsRwLockMemorySize;
    put_node_memory_boundary(
      node, &total, "thread_mcs_rw_lock_memories_boundary", reset_boundaries);
  }

  // This is larger than others (except volatile pool). we place this at the end.
  uint64_t reducer_buffer_size
    = static_cast<uint64_t>(options.snapshot_.log_reducer_buffer_mb_) << 20;
  anchor.log_reducer_buffers_[0] = base + total;
  anchor.log_reducer_buffers_[1] = base + total + (reducer_buffer_size / 2);
  total += reducer_buffer_size;
  put_node_memory_boundary(node, &total, "node_log_reducer_buffers_boundary", reset_boundaries);

  // Then volatile pool at the end. This is even bigger
  anchor.volatile_page_pool_ = base + total;
  total += (static_cast<uint64_t>(options.memory_.page_pool_size_mb_per_node_) << 20);
  put_node_memory_boundary(node, &total, "volatile_pool_boundary", reset_boundaries);

  // we have to be super careful here. let's not use assertion.
  if (total != calculate_node_memory_size(options)) {
    std::cerr << "[FOEDUS] node memory size doesn't match. bug?"
      << " allocated=" << calculate_node_memory_size(options)
      << ", expected=" << total << std::endl;
  }

  // same as global memory
  if (options.memory_.rigorous_memory_boundary_check_) {
    for (assorted::ProtectedBoundary* boundary : anchor.protected_boundaries_) {
      boundary->acquire_protect();
    }
    anchor.protected_boundaries_needs_release_ = true;
  }
}

uint64_t SharedMemoryRepo::calculate_node_memory_size(const EngineOptions& options) {
  const uint64_t kBoundarySize = sizeof(assorted::ProtectedBoundary);
  uint64_t total = 0;
  total += NodeMemoryAnchors::kChildStatusMemorySize + kBoundarySize;
  total += NodeMemoryAnchors::kPagePoolMemorySize + kBoundarySize;
  total += NodeMemoryAnchors::kProcManagerMemorySize + kBoundarySize;
  total += align_4kb(sizeof(proc::ProcAndName) * options.proc_.max_proc_count_) + kBoundarySize;
  total += align_4kb(sizeof(proc::LocalProcId) * options.proc_.max_proc_count_) + kBoundarySize;
  total += NodeMemoryAnchors::kLogReducerMemorySize + kBoundarySize;
  total += options.storage_.max_storages_ * 4096ULL + kBoundarySize;

  uint64_t loggers_per_node = options.log_.loggers_per_node_;
  total += loggers_per_node * (NodeMemoryAnchors::kLoggerMemorySize + kBoundarySize);

  uint64_t threads_per_node = options.thread_.thread_count_per_group_;
  total += threads_per_node * (ThreadMemoryAnchors::kThreadMemorySize + kBoundarySize);
  total += threads_per_node * (ThreadMemoryAnchors::kTaskInputMemorySize + kBoundarySize);
  total += threads_per_node * (ThreadMemoryAnchors::kTaskOutputMemorySize + kBoundarySize);
  total += threads_per_node * (ThreadMemoryAnchors::kMcsLockMemorySize + kBoundarySize);
  total += threads_per_node * (ThreadMemoryAnchors::kMcsRwLockMemorySize + kBoundarySize);

  total +=
    (static_cast<uint64_t>(options.snapshot_.log_reducer_buffer_mb_) << 20)
    + kBoundarySize;

  // Then volatile pool at the end.
  total +=
    (static_cast<uint64_t>(options.memory_.page_pool_size_mb_per_node_) << 20)
    + kBoundarySize;
  return total;
}

void SharedMemoryRepo::change_master_status(MasterEngineStatus::StatusCode new_status) {
  global_memory_anchors_.master_status_memory_->change_status_atomic(new_status);
}

MasterEngineStatus::StatusCode SharedMemoryRepo::get_master_status() const {
  return global_memory_anchors_.master_status_memory_->read_status_atomic();
}

void SharedMemoryRepo::change_child_status(SocId node, ChildEngineStatus::StatusCode new_status) {
  node_memory_anchors_[node].child_status_memory_->change_status_atomic(new_status);
}

ChildEngineStatus::StatusCode SharedMemoryRepo::get_child_status(SocId node) const {
  return node_memory_anchors_[node].child_status_memory_->read_status_atomic();
}


}  // namespace soc
}  // namespace foedus
