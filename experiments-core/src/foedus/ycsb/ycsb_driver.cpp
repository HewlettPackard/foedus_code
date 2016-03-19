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

#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/wait.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/assorted/zipfian_random.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/hash/hash_hashinate.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/numa_thread_scope.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/ycsb/ycsb.hpp"

namespace foedus {
namespace ycsb {
DEFINE_bool(fork_workers, false, "Whether to fork(2) worker threads in child processes rather"
    " than threads in the same process. This is required to scale up to 100+ cores.");
DEFINE_bool(exec_duplicates, false, "[Experimental] Whether to fork/exec(2) worker threads in child"
    " processes on replicated binaries. This is required to scale up to 16 sockets.");
DEFINE_bool(profile, false, "Whether to profile the execution with gperftools.");
DEFINE_bool(papi, false, "Whether to profile with PAPI.");
DEFINE_bool(high_priority, false, "Set high priority to threads. Needs 'rtprio 99' in limits.conf");
DEFINE_string(nvm_folder, "/dev/shm", "Full path of the device representing NVM.");
DEFINE_int32(volatile_pool_size, 8, "Size of volatile memory pool per NUMA node in GB.");
DEFINE_int32(snapshot_pool_size, 1, "Size of snapshot memory pool per NUMA node in MB.");
DEFINE_int32(reducer_buffer_size, 1, "Size of reducer's buffer per NUMA node in GB.");
DEFINE_int32(loggers_per_node, 1, "Number of log writers per numa node.");
DEFINE_int32(thread_per_node, 6, "Number of threads per NUMA node. 0 uses logical count");
DEFINE_int32(numa_nodes, 0, "Number of NUMA nodes. 0 uses physical count");
DEFINE_bool(use_numa_alloc, true, "Whether to use ::numa_alloc_interleaved()/::numa_alloc_onnode()"
  " to allocate memories. If false, we use usual posix_memalign() instead");
DEFINE_bool(interleave_numa_alloc, false, "Whether to use ::numa_alloc_interleaved()"
  " instead of ::numa_alloc_onnode()");
DEFINE_bool(mmap_hugepages, false, "Whether to use mmap for 1GB hugepages."
  " This requies special setup written in the readme.");
DEFINE_int32(log_buffer_mb, 256, "Size in MB of log buffer for each thread");
DEFINE_bool(null_log_device, false, "Whether to disable log writing.");
DEFINE_int64(duration_micro, 10000000, "Duration of benchmark in microseconds.");
DEFINE_int32(hot_threshold, -1, "Threshold to determine hot/cold pages,"
  " 0 (always hot, 2PL) - 256 (always cold, OCC).");
DEFINE_int32(rll_relative_threshold, -1, "Relative threshold to determine hot/cold pages in RLL,"
  " This value + hot_threshold becomes hot_threshold_for_retrospective_lock_list_."
  " Note: Most likely this value should be negative.");

// YCSB-specific options
DEFINE_string(workload, "F", "YCSB workload; choose A/B/C/D/E/F.");
DEFINE_int64(max_scan_length, 1000, "Maximum number of records to scan.");
DEFINE_bool(read_all_fields, true, "Read all or only one field(s) in read transactions.");
DEFINE_bool(write_all_fields, true, "Write all or only one field(s) in update transactions.");
DEFINE_int64(initial_table_size, 50, "The number of records to insert at loading.");
DEFINE_bool(random_inserts, false, "Allow inserting in others' key space (use random high bits).");
DEFINE_bool(use_string_keys, true, "Whether the keys should start from 'user'.");
DEFINE_bool(verify_loaded_data, true, "Whether to verify key length and value after loading.");
DEFINE_double(zipfian_theta, 0, "The theta value in Zipfian distribution, 0 < theta < 1."
  " Larger = more sckewed.");
DEFINE_int32(rmw_additional_reads, 10, "The number of reads in an RMW transaction.");
DEFINE_int32(reps_per_tx, 0, "The number of operations to repeat in each transaction."
  " For instance, setting this to 10 and running workload F means 'do 10 RMWs in each tx'."
  " Records are choosen by the corresponding RNG used by the transaction.");

// If this is enabled, the original YCSB implementation gives a fully ordered key across all
// threads. But that's hard to scale in high core counts. So we use [worker_id | local_count].
DEFINE_bool(ordered_inserts, false, "Whether to make the keys ordered, i.e., don't hash(keynum).");

DEFINE_bool(simple_int_keys, true, "Whether to use 8-byte interger key; ignores -ordered_inserts.");

// Generate all keys first, then sort them before inserting to the table (loading only).
// This is not in the spec; it makes masstree perform better.
DEFINE_bool(sort_load_keys, true, "Whether to sort the keys before loading.");

DEFINE_bool(sort_keys, true, "Whether to sort the keys used in workload F");
DEFINE_bool(distinct_keys, true, "Whether to make every key under access is different");

DEFINE_int32(extra_table_size, 0, "How many records to load in a second static user table.");
DEFINE_int32(extra_table_reads, 0, "How many reads to do in the extra table.");
DEFINE_int32(extra_table_rmws, 0, "How many RMWs to do in the extra table.");

DEFINE_bool(force_canonical_xlocks_in_precommit, true,
  "Whether precommit always releases all locks that violate canonical mode before taking X-locks");
DEFINE_bool(enable_retrospective_lock_list, true, "Whether to use RLL after aborts");
DEFINE_bool(extended_rw_lock, false, "whether to use the extended RW lock implementation");

DEFINE_bool(aggressive_release, true, "Enable aggressive lock-release to restore canonical mode");
DEFINE_bool(parallel_lock, false, "whether to take locks in parallel in precommit when"
    " we are not in canonical mode, using the async-lock interface.");
DEFINE_int32(parallel_lock_retries, 5, "How many times to try for parallel lock before giving up.");

DEFINE_bool(shifting_workload, false, "whether to run the shifting workloads.");


YcsbWorkload YcsbWorkloadA('A', 0,  50U,  100U, 0,    0);     // Workload A - 50% read, 50% update
YcsbWorkload YcsbWorkloadB('B', 0,  95U,  100U, 0,    0);     // Workload B - 95% read, 5% update
YcsbWorkload YcsbWorkloadC('C', 0,  100U, 0,    0,    0);     // Workload C - 100% read
YcsbWorkload YcsbWorkloadD('D', 5U, 100U, 0,    0,    0);     // Workload D - 95% read, 5% insert
YcsbWorkload YcsbWorkloadE('E', 5U, 0,    0,    100U, 0);     // Workload E - 5% insert, 95% scan

// Combine reps_per_tx and rmw_additional_reads to have "10R+10RMW" style transactions.
YcsbWorkload YcsbWorkloadF('F', 0,  0,    0,    0,    100U);  // Workload F - 100% RMW

// Extra workloads (not in spec)
YcsbWorkload YcsbWorkloadG('G', 0,  0,    5U,   100U, 0);     // Workload G - 5% update, 95% scan
YcsbWorkload YcsbWorkloadH('H', 0,  0,    0,    100U, 0);     // Workload H - 100% scan

int64_t max_scan_length() {
  return FLAGS_max_scan_length;
}

YcsbRecord::YcsbRecord(char value) {
  // So just write some arbitrary characters provided, no need to use rnd
  memset(data_, value, kFields * kFieldLength * sizeof(char));
}

// TODO(tzwang): make this field content random
void YcsbRecord::initialize_field(char *field) {
  memset(field, 'a', kFieldLength);
}

YcsbKey& YcsbKey::build(uint32_t high_bits, uint32_t low_bits) {
  uint64_t keynum = ((uint64_t)high_bits << 32) | low_bits;
  if (FLAGS_simple_int_keys) {
    // ignore ordered_insert as well
    data_.clear();
    data_.append(reinterpret_cast<char *>(&keynum), sizeof(uint64_t));
  } else {
    if (!FLAGS_ordered_inserts) {
      keynum = (uint64_t)foedus::storage::hash::hashinate(&keynum, sizeof(keynum));
    }
    int integer_length = kKeyMaxLength;
    if (FLAGS_use_string_keys) {
      data_ = kKeyPrefix;
      integer_length -= kKeyPrefixLength;
    }
    char keychar[kKeyMaxLength + 1];
    auto len = snprintf(keychar, integer_length + 1, "%lu", keynum);
    data_.append(keychar, len);
  }
  return *this;
}

YcsbClientChannel* get_channel(Engine* engine) {
  // Use the global user memory as a channel for synchronizing workers
  // (It's a global, flat space; so remember to +sizeof(YcsbClientChannel) if
  // need to put other things there)
  YcsbClientChannel* channel = reinterpret_cast<YcsbClientChannel*>(
    engine->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
  // It's the caller's responsibility to make sure this channel is initialize()ed
  return channel;
}

// We put each worker's local key counter in shared memory, each occupying a cacheline
// of bytes. So now the layout in the shared memory space is like:
//
// Offset: Content
// 0: the channel
// + align_up(sizeof(YcsbClientChannel), CACHELINE_SIZE): 1st worker's local key counter
// + CACHELINE_SIZE: 2nd worker's local key counter
// + CACHELINE_SIZE: 3rd worker's local key counter
// ... and so on...
uint32_t YcsbClientChannel::peek_local_user_key_counter(Engine* engine, uint32_t worker_id) {
  return get_local_key_counter(engine, worker_id)->user_key_counter_;
}

uint32_t YcsbClientChannel::peek_local_extra_key_counter(Engine* engine, uint32_t worker_id) {
  return get_local_key_counter(engine, worker_id)->extra_key_counter_;
}

PerWorkerCounter* get_local_key_counter(Engine* engine, uint32_t worker_id) {
  uintptr_t shm = reinterpret_cast<uintptr_t>(
    engine->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
  uintptr_t address =
    assorted::align<uintptr_t, assorted::kCachelineSize>(shm + sizeof(YcsbClientChannel));
  address += sizeof(PerWorkerCounter) * worker_id;
  return reinterpret_cast<PerWorkerCounter*>(address);
}

int driver_main(int argc, char **argv) {
  gflags::SetUsageMessage("YCSB implementation for FOEDUS");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fs::Path folder("/dev/shm/foedus_ycsb");
  if (fs::exists(folder)) {
    fs::remove_all(folder);
  }
  if (!fs::create_directories(folder)) {
    std::cerr << "Couldn't create " << folder << ". err=" << assorted::os_error();
    return 1;
  }

  EngineOptions options;

  fs::Path savepoint_path(folder);
  savepoint_path /= "savepoint.xml";
  options.savepoint_.savepoint_path_.assign(savepoint_path.string());
  ASSERT_ND(!fs::exists(savepoint_path));

  std::cout << "NUMA node count=" << static_cast<int>(options.thread_.group_count_) << std::endl;
  if (FLAGS_numa_nodes != 0) {
    std::cout << "numa_nodes specified:" << FLAGS_numa_nodes << std::endl;
    options.thread_.group_count_ = FLAGS_numa_nodes;
  }
  if (FLAGS_mmap_hugepages) {
    std::cout << "oh, mmap_hugepages is specified. " << std::endl;
    options.memory_.use_mmap_hugepages_ = true;
  } else if (!FLAGS_use_numa_alloc) {
    std::cout << "oh, use_numa_alloc is false. are you sure?" << std::endl;
    // this should be only for experimental purpose.
    // if everything is working correctly, numa_alloc_onnode must be the best
    options.memory_.use_numa_alloc_ = false;
  } else {
    if (FLAGS_interleave_numa_alloc) {
      std::cout << "oh, interleave_numa_alloc_ is true. are you sure?" << std::endl;
      // again, numa_alloc_onnode should be better than numa_alloc_interleaved
      options.memory_.interleave_numa_alloc_ = true;
    }
  }

  options.snapshot_.folder_path_pattern_ = "/dev/shm/foedus_ycsb/snapshot/node_$NODE$";
  options.log_.folder_path_pattern_ = "/dev/shm/foedus_ycsb/log/node_$NODE$/logger_$LOGGER$";
  options.log_.loggers_per_node_ = FLAGS_loggers_per_node;
  options.log_.flush_at_shutdown_ = false;
  options.snapshot_.snapshot_interval_milliseconds_ = 100000000U;

  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogInfo;
    // = debugging::DebuggingOptions::kDebugLogWarning;
  options.debugging_.verbose_modules_ = "";
  options.debugging_.verbose_log_level_ = -1;

  options.log_.log_buffer_kb_ = FLAGS_log_buffer_mb << 10;
  std::cout << "log_buffer_mb=" << FLAGS_log_buffer_mb << "MB per thread" << std::endl;
  options.log_.log_file_size_mb_ = 1 << 15;
  std::cout << "volatile_pool_size=" << FLAGS_volatile_pool_size << "GB per NUMA node" << std::endl;
  options.memory_.page_pool_size_mb_per_node_ = (FLAGS_volatile_pool_size) << 10;

  if (FLAGS_thread_per_node != 0) {
    std::cout << "thread_per_node=" << FLAGS_thread_per_node << std::endl;
    options.thread_.thread_count_per_group_ = FLAGS_thread_per_node;
  }

  if (FLAGS_null_log_device) {
    std::cout << "/dev/null log device" << std::endl;
    options.log_.emulation_.null_device_ = true;
  }

  if (FLAGS_high_priority) {
    std::cout << "Will set highest priority to worker threads" << std::endl;
    options.thread_.overwrite_thread_schedule_ = true;
    options.thread_.thread_policy_ = thread::kScheduleFifo;
    options.thread_.thread_priority_ = thread::kPriorityHighest;
  }

  if (FLAGS_fork_workers) {
    std::cout << "Will fork workers in child processes" << std::endl;
    options.soc_.soc_type_ = kChildForked;
  } else if (FLAGS_exec_duplicates) {
    std::cout << "Will duplicate binaries and exec workers in child processes" << std::endl;
    options.soc_.soc_type_ = kChildLocalSpawned;
  }

  if (FLAGS_hot_threshold > 256) {
    std::cout << "Hot page threshold is too large: " << FLAGS_hot_threshold
      << ". Choose a value between 0 and 256 (inclusive)." << std::endl;
    return 1;
  }
  options.storage_.hot_threshold_ = FLAGS_hot_threshold;
  std::cout << "Hot record threshold: " << options.storage_.hot_threshold_ << std::endl;

  options.xct_.hot_threshold_for_retrospective_lock_list_
    = std::max<int>(0, options.storage_.hot_threshold_ + FLAGS_rll_relative_threshold);
  std::cout << "RLL hot threshold: "
    << options.xct_.hot_threshold_for_retrospective_lock_list_ << std::endl;

  options.xct_.force_canonical_xlocks_in_precommit_ = FLAGS_force_canonical_xlocks_in_precommit;
  options.xct_.enable_retrospective_lock_list_ = FLAGS_enable_retrospective_lock_list;
  options.xct_.parallel_lock_ = FLAGS_parallel_lock;
  options.xct_.parallel_lock_retries_ = FLAGS_parallel_lock_retries;
  if (FLAGS_extended_rw_lock) {
    options.xct_.mcs_implementation_type_ = xct::XctOptions::kMcsImplementationTypeExtended;
  } else {
    options.xct_.mcs_implementation_type_ = xct::XctOptions::kMcsImplementationTypeSimple;
  }
  // TODO(Hideaki) Some option and its implementation for aggressive_release/parallel_lock
  std::cout
    << "force_canonical_xlocks_in_precommit: " << FLAGS_force_canonical_xlocks_in_precommit
    << " enable_retrospective_lock_list: " << FLAGS_enable_retrospective_lock_list
    << " mcs_implementation_type_: " << options.xct_.mcs_implementation_type_
    << " aggressive_release: " << FLAGS_aggressive_release
    << " parallel_lock: " << FLAGS_parallel_lock
    << " parallel_lock_retries: " << FLAGS_parallel_lock_retries
    << std::endl;

  std::cout << "sort keys before accessing: " << FLAGS_sort_keys << std::endl;

  // Get an engine, register procedures to run
  Engine engine(options);
  proc::ProcAndName load_proc("ycsb_load_task", ycsb_load_task);
  proc::ProcAndName work_proc("ycsb_client_task", ycsb_client_task);
  engine.get_proc_manager()->pre_register(load_proc);
  engine.get_proc_manager()->pre_register(work_proc);
#ifndef YCSB_HASH_STORAGE
  proc::ProcAndName load_verify_proc("ycsb_load_verify_task", ycsb_load_verify_task);
  engine.get_proc_manager()->pre_register(load_verify_proc);
#endif
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    // This driver will fire off loading and impersonate clients
    YcsbDriver driver(&engine);
    driver.run();  // this will wait for the loader thread to finish
    COERCE_ERROR(engine.uninitialize());
  }
  return 0;
}

ErrorStack YcsbDriver::run() {
  // Setup the channel so I can synchronize with workers and record nr_workers
  YcsbClientChannel* channel = get_channel(engine_);
  const EngineOptions& options = engine_->get_options();
  uint32_t total_thread_count = options.thread_.get_total_thread_count();
  channel->initialize(total_thread_count);
  int64_t initial_table_size = FLAGS_initial_table_size;
  if (initial_table_size > total_thread_count) {
    auto remainder = initial_table_size % total_thread_count;
    if (remainder) {
      initial_table_size -= remainder;
    }
  }
  LOG(INFO) << "Requested user table size: " << FLAGS_initial_table_size
    << ", will load " << initial_table_size << " records";

  int64_t extra_table_size = FLAGS_extra_table_size;
  if (extra_table_size > total_thread_count) {
    auto remainder = extra_table_size % total_thread_count;
    if (remainder) {
      extra_table_size -= remainder;
    }
  }
  LOG(INFO) << "Requested extra table size: " << FLAGS_extra_table_size
    << ", will load " << extra_table_size << " records";

  YcsbWorkload workload;
  if (FLAGS_workload == "A") {
    workload = YcsbWorkloadA;
  } else if (FLAGS_workload == "B") {
    workload = YcsbWorkloadB;
  } else if (FLAGS_workload == "C") {
    workload = YcsbWorkloadC;
  } else if (FLAGS_workload == "D") {
    workload = YcsbWorkloadD;
  } else if (FLAGS_workload == "E") {
    workload = YcsbWorkloadE;
  } else if (FLAGS_workload == "F") {
    workload = YcsbWorkloadF;
    workload.rmw_additional_reads_ = FLAGS_rmw_additional_reads;
    workload.extra_table_rmws_ = FLAGS_extra_table_rmws;
    workload.extra_table_reads_ = FLAGS_extra_table_reads;
  } else if (FLAGS_workload == "G") {
    workload = YcsbWorkloadG;
  } else if (FLAGS_workload == "H") {
    workload = YcsbWorkloadH;
  } else {
    COERCE_ERROR_CODE(kErrorCodeInvalidParameter);
  }

  workload.extra_table_size_ = extra_table_size;
  workload.reps_per_tx_ = FLAGS_reps_per_tx;
  workload.distinct_keys_ = FLAGS_distinct_keys;

  LOG(INFO)
    << "Workload -"
    << " insert: " << workload.insert_percent() << "%"
    << " read: " << workload.read_percent() << "%"
    << " update: " << workload.update_percent() << "%"
    << " scan: " << workload.scan_percent() << "%"
    << " rmw: " << workload.rmw_percent() << "%"
    << " rmw additional reads: " << workload.rmw_additional_reads_
    << " operations per transaction: " << workload.reps_per_tx_
    << " use distinct keys: " << workload.distinct_keys_
    << " extra table size: " << workload.extra_table_size_
    << " extra table rmws: " << workload.extra_table_rmws_
    << " extra table reads: " << workload.extra_table_reads_
    << " zipfian theta: " << FLAGS_zipfian_theta;

  // Create an empty table
  Epoch ep;
#ifdef YCSB_HASH_STORAGE
  LOG(INFO) << "Use hash table storage";
  storage::hash::HashMetadata meta("ycsb_user_table");
  const float kHashPreferredRecordsPerBin = 15.0;
  if (workload.insert_percent() == 0) {
    meta.set_capacity(initial_table_size, kHashPreferredRecordsPerBin);
  } else {
    // Don't support expanding record so far... *1.5 should be more than enough
    meta.set_capacity(initial_table_size * 1.5, kHashPreferredRecordsPerBin);
  }
  storage::hash::HashMetadata extra_meta("ycsb_extra_table");
  // We don't grow this table
  extra_meta.set_capacity(extra_table_size, kHashPreferredRecordsPerBin);
#else
  LOG(INFO) << "Use masstree storage";
  storage::masstree::MasstreeMetadata meta("ycsb_user_table", 100);
  if (workload.insert_percent() > 0) {
    meta.border_early_split_threshold_ = 80;
  }
  meta.snapshot_drop_volatile_pages_btree_levels_ = 0;
  meta.snapshot_drop_volatile_pages_layer_threshold_ = 8;

  storage::masstree::MasstreeMetadata extra_meta("ycsb_extra_table", 100);
  extra_meta.snapshot_drop_volatile_pages_btree_levels_ = 0;
  extra_meta.snapshot_drop_volatile_pages_layer_threshold_ = 8;
#endif

  // Keep volatile pages
  meta.snapshot_thresholds_.snapshot_keep_threshold_ = 0xFFFFFFFFU;
  extra_meta.snapshot_thresholds_.snapshot_keep_threshold_ = 0xFFFFFFFFU;
  COERCE_ERROR(engine_->get_storage_manager()->create_storage(&meta, &ep));
  COERCE_ERROR(engine_->get_storage_manager()->create_storage(&extra_meta, &ep));
  auto initial_user_records_per_thread = initial_table_size / total_thread_count;
  auto extra_records_per_thread = extra_table_size / total_thread_count;
  auto* thread_pool = engine_->get_thread_pool();
  // One loader per node
  std::vector< thread::ImpersonateSession > load_sessions;
  for (uint16_t node = 0; node < options.thread_.group_count_; node++) {
    YcsbLoadTask::Inputs inputs;
    inputs.load_node_ = node;
    if (initial_user_records_per_thread == 0) {
      if (node == 0) {
        // Let one thread in one node load them all if we don't
        // have at least one record per thread (note, not per loader).
        // FIXME(tzwang): worth it to spread records as widely as possible?
        inputs.user_records_per_thread_ = initial_table_size;
        inputs.user_table_spread_ = false;
      }
    } else {
      inputs.user_records_per_thread_ = initial_user_records_per_thread;
      inputs.user_table_spread_ = true;
    }

    if (extra_records_per_thread == 0) {
      if (node == 0) {
        inputs.extra_records_per_thread_ = extra_table_size;
        inputs.extra_table_spread_ = false;
      }
    } else {
      inputs.extra_records_per_thread_ = extra_records_per_thread;
      inputs.extra_table_spread_ = true;
    }

    inputs.sort_load_keys_ = FLAGS_sort_load_keys;
    thread::ImpersonateSession load_session;
    bool ret = thread_pool->impersonate_on_numa_node(
      node, "ycsb_load_task", &inputs, sizeof(inputs), &load_session);
    if (!ret) {
      LOG(FATAL) << "Couldn't impersonate";
    }
    load_sessions.emplace_back(std::move(load_session));
    if (initial_user_records_per_thread == 0 && extra_records_per_thread == 0) {
      break;
    }
  }
  // Wait for the load tasks to finish
  const uint64_t kIntervalMs = 10;
  for (uint32_t i = 0; i < load_sessions.size(); ++i) {
    while (load_sessions[i].is_running()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(kIntervalMs));
      assorted::memory_fence_acquire();
    }
    LOG(INFO) << "result[" << i << "]=" << load_sessions[i].get_result();
    load_sessions[i].release();
  }

#ifndef YCSB_HASH_STORAGE
  if (FLAGS_verify_loaded_data) {
    // Verify the loaded data
    thread::ImpersonateSession verify_session;
    auto ret = thread_pool->impersonate("ycsb_load_verify_task", nullptr, 0, &verify_session);
    if (!ret) {
      LOG(FATAL) << "Couldn't impersonate";
    }
    while (verify_session.is_running()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(kIntervalMs));
      assorted::memory_fence_acquire();
    }
    verify_session.release();
  }
#endif

  if (FLAGS_profile) {
    COERCE_ERROR(engine_->get_debug()->start_profile("ycsb.prof"));
  }
  if (FLAGS_papi) {
    engine_->get_debug()->start_papi_counters();
  }

  // Now try to start transaction worker threads
  uint32_t worker_id = 0;
  std::vector< thread::ImpersonateSession > sessions;
  std::vector< const YcsbClientTask::Outputs* > outputs;
  for (uint16_t node = 0; node < options.thread_.group_count_; node++) {
    for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ordinal++) {
      thread::ImpersonateSession session;
      YcsbClientTask::Inputs inputs;
      inputs.worker_id_ = worker_id;
      inputs.zipfian_theta_ = FLAGS_zipfian_theta;
      inputs.read_all_fields_ = FLAGS_read_all_fields;
      inputs.write_all_fields_ = FLAGS_write_all_fields;
      inputs.random_inserts_ = FLAGS_random_inserts;
      inputs.sort_keys_ = FLAGS_sort_keys;
      inputs.local_key_counter_ = get_local_key_counter(engine_, worker_id);
      inputs.output_bucketed_throughput_ = FLAGS_shifting_workload;
      if (initial_user_records_per_thread == 0) {
        if (node == 0 && ordinal == 0) {
          inputs.local_key_counter_->user_key_counter_ = initial_table_size;
        } else {
          inputs.local_key_counter_->user_key_counter_ = 0;
        }
      } else {
        inputs.local_key_counter_->user_key_counter_ = initial_user_records_per_thread;
      }
      if (extra_records_per_thread == 0) {
        if (node == 0 && ordinal == 0) {
          inputs.local_key_counter_->extra_key_counter_ = extra_table_size;
        } else {
          inputs.local_key_counter_->extra_key_counter_ = 0;
        }
      } else {
        inputs.local_key_counter_->extra_key_counter_ = extra_records_per_thread;
      }
      inputs.workload_ = workload;
      inputs.initial_table_size_ = initial_table_size;
      inputs.extra_table_size_ = extra_table_size;
      bool ret = thread_pool->impersonate_on_numa_node(
        node, "ycsb_client_task", &inputs, sizeof(inputs), &session);
      if (!ret) {
        LOG(FATAL) << "Couldn't impersonate";
      }
      outputs.push_back(
        reinterpret_cast<const YcsbClientTask::Outputs*>(session.get_raw_output_buffer()));
      sessions.emplace_back(std::move(session));
      worker_id++;
    }
  }

  // Make sure everyone has finished initialization
  while (channel->exit_nodes_ != 0) {}

  // Tell everybody to start
  channel->start_rendezvous_.signal();
  assorted::memory_fence_release();
  LOG(INFO) << "Started!";
  debugging::StopWatch duration;
  uint32_t sleep_interval_us = 1000000ULL;
  constexpr uint32_t kBucketIntervalUs = 10UL;  // 10 us
  constexpr uint64_t kSwitchIntervalUs = 10000UL;  // 10 ms
  std::unique_ptr<uint64_t> bucket_times;
  uint64_t* bucket_times_raw = nullptr;
  uint32_t max_bucket = 0;
  uint32_t shift_counter = 0;
  if (FLAGS_shifting_workload) {
    bucket_times.reset(new uint64_t[kMaxOutputBuckets]);
    bucket_times_raw = bucket_times.get();
    std::memset(bucket_times_raw, 0, sizeof(uint64_t) * kMaxOutputBuckets);

    // In shifting workload, we switch to next throughput bucket for every:
    sleep_interval_us = kBucketIntervalUs;
  }
  while (duration.peek_elapsed_ns() < static_cast<uint64_t>(FLAGS_duration_micro) * 1000ULL) {
    // wake up for each second to show intermediate results.
    uint64_t remaining_duration = FLAGS_duration_micro - duration.peek_elapsed_ns() / 1000ULL;
    remaining_duration = std::min<uint64_t>(remaining_duration, sleep_interval_us);
    std::this_thread::sleep_for(std::chrono::microseconds(remaining_duration));

    if (FLAGS_shifting_workload) {
      if (max_bucket >= kMaxOutputBuckets) {
        LOG(FATAL) << "WTF. exceeded kMaxOutputBuckets. " << channel->cur_output_bucket_;
      }

      // Remember, sleep_for with short duration (100 us) is VERY inaccurate.
      // we thus remember the timestamp of each bucket to plot a 2-D scatter chart.
      channel->cur_output_bucket_ = max_bucket;
      uint64_t elapsed_ns = duration.peek_elapsed_ns();
      bucket_times_raw[max_bucket] = elapsed_ns;

      // the switch/reset happens infreuqenty, so check them based on timer.
      uint32_t new_counter = elapsed_ns / (kSwitchIntervalUs * 1000ULL);
      uint32_t new_shift_counter = new_counter / 10;
      if (new_shift_counter != shift_counter) {
        // 0.05, 0.15, 0.25.. sec to shift wokrload
        LOG(INFO) << "Shifts workload at now=" << (elapsed_ns / 1000000000.0f) << "s";
        shift_counter = new_shift_counter;
        channel->shifted_workload_ = !channel->shifted_workload_;
      }
      // no need to reset when we change read -> rmws.
      bool should_reset = (!channel->shifted_workload_) && (max_bucket % 10 == 0);
      if (should_reset) {
        LOG(INFO) << "Resets HCC counter at now=" << (elapsed_ns / 1000000000.0f) << "s";
#ifdef YCSB_HASH_STORAGE
        // auto user_table = engine_->get_storage_manager()->get_hash("ycsb_user_table");
        auto extra_table = engine_->get_storage_manager()->get_hash("ycsb_extra_table");
#else
        // auto user_table = engine_->get_storage_manager()->get_masstree("ycsb_user_table");
        auto extra_table = engine_->get_storage_manager()->get_masstree("ycsb_extra_table");
#endif
        // No need to reset user table, which is always cold
        // COERCE_ERROR(user_table.hcc_reset_all_temperature_stat());
        COERCE_ERROR(extra_table.hcc_reset_all_temperature_stat());
      }

      // in shifting workload, omit the intermediate report.
      ++max_bucket;
      continue;
    }

    Result result;
    result.duration_sec_ = static_cast<double>(duration.peek_elapsed_ns()) / 1000000000;
    result.worker_count_ = total_thread_count;
    for (uint32_t i = 0; i < sessions.size(); ++i) {
      const YcsbClientTask::Outputs* output = outputs[i];
      result.processed_ += output->processed_;
      result.race_aborts_ += output->race_aborts_;
      result.lock_aborts_ += output->lock_aborts_;
      result.unexpected_aborts_ += output->unexpected_aborts_;
      result.largereadset_aborts_ += output->largereadset_aborts_;
      result.insert_conflict_aborts_ += output->insert_conflict_aborts_;
      result.total_scan_length_ += output->total_scan_length_;
      result.total_scans_ += output->total_scans_;
      result.snapshot_cache_hits_ += output->snapshot_cache_hits_;
      result.snapshot_cache_misses_ += output->snapshot_cache_misses_;
    }
    LOG(INFO) << "Intermediate report after " << result.duration_sec_ << " sec";
    LOG(INFO) << result;
  }
  duration.stop();

  if (FLAGS_profile) {
    engine_->get_debug()->stop_profile();
  }
  if (FLAGS_papi) {
    engine_->get_debug()->stop_papi_counters();
  }

  Result result;
  duration.stop();
  result.duration_sec_ = duration.elapsed_sec();
  result.worker_count_ = total_thread_count;
  result.papi_results_ = debugging::DebuggingSupports::describe_papi_counters(
    engine_->get_debug()->get_papi_counters());
  assorted::memory_fence_acquire();
  std::unique_ptr<YcsbClientTask::Outputs> sum_buckets;
  if (FLAGS_shifting_workload) {
    sum_buckets.reset(new YcsbClientTask::Outputs());
    std::memset(sum_buckets->bucketed_throughputs_, 0, sizeof(sum_buckets->bucketed_throughputs_));
  }
  for (uint32_t i = 0; i < sessions.size(); ++i) {
    const YcsbClientTask::Outputs* output = outputs[i];
    result.workers_[i].id_ = i;
    result.workers_[i].processed_ = output->processed_;
    result.workers_[i].race_aborts_ = output->race_aborts_;
    result.workers_[i].lock_aborts_ = output->lock_aborts_;
    result.workers_[i].unexpected_aborts_ = output->unexpected_aborts_;
    result.workers_[i].largereadset_aborts_ = output->largereadset_aborts_;
    result.workers_[i].insert_conflict_aborts_ = output->insert_conflict_aborts_;
    result.workers_[i].total_scan_length_ = output->total_scan_length_;
    result.workers_[i].total_scans_ = output->total_scans_;
    result.workers_[i].snapshot_cache_hits_ = output->snapshot_cache_hits_;
    result.workers_[i].snapshot_cache_misses_ = output->snapshot_cache_misses_;
    result.processed_ += output->processed_;
    result.race_aborts_ += output->race_aborts_;
    result.lock_aborts_ += output->lock_aborts_;
    result.unexpected_aborts_ += output->unexpected_aborts_;
    result.largereadset_aborts_ += output->largereadset_aborts_;
    result.insert_conflict_aborts_ += output->insert_conflict_aborts_;
    result.total_scan_length_ += output->total_scan_length_;
    result.total_scans_ += output->total_scans_;
    result.snapshot_cache_hits_ += output->snapshot_cache_hits_;
    result.snapshot_cache_misses_ += output->snapshot_cache_misses_;
    if (FLAGS_shifting_workload) {
      for (uint32_t j = 0; j < max_bucket; ++j) {
        sum_buckets->bucketed_throughputs_[j] += output->bucketed_throughputs_[j];
      }
    }
  }

  LOG(INFO) << "Shutting down...";

  // output the current memory state at the end
  LOG(INFO) << engine_->get_memory_manager()->dump_free_memory_stat();

  channel->stop_flag_.store(true);

  // Let's forcibly kill everything if it doesn't stop after 10x the experiment duration + 10 sec.
  const uint64_t session_timeout_us = FLAGS_duration_micro * 10 + 10000000ULL;
  for (uint32_t i = 0; i < sessions.size(); ++i) {
    bool ended = sessions[i].wait_for(session_timeout_us);
    if (ended) {
      LOG(INFO) << "result[" << i << "]=" << sessions[i].get_result();
      sessions[i].release();
    } else {
      LOG(FATAL) << "Ouch! the experiment worker-" << i << " doesn't stop after "
        << session_timeout_us << " microsec."
        << " We will forcibly/dirtily stop the process(s).";
    }
  }
  channel->uninitialize();

  // Let's debug out all pages if it's small
  if (FLAGS_initial_table_size < 1000U) {
#ifdef YCSB_HASH_STORAGE
    storage::hash::HashStorage the_storage(engine_, "ycsb_user_table");
#else
    storage::masstree::MasstreeStorage the_storage(engine_, "ycsb_user_table");
#endif
    CHECK_ERROR(the_storage.debugout_single_thread(engine_));
  }
  if (FLAGS_shifting_workload) {
#ifdef YCSB_HASH_STORAGE
    auto extra_table = engine_->get_storage_manager()->get_hash("ycsb_extra_table");
#else
    auto extra_table = engine_->get_storage_manager()->get_masstree("ycsb_extra_table");
#endif
    CHECK_ERROR(extra_table.debugout_single_thread(engine_));
  }
  // wait just for a bit to avoid mixing stdout
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  if (FLAGS_shifting_workload) {
    // Also wait a bit more to make sure the resetting is not going now
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
  for (uint32_t i = 0; i < result.worker_count_; ++i) {
    LOG(INFO) << result.workers_[i];
  }
  LOG(INFO) << "final result:" << result;
  if (FLAGS_papi) {
    LOG(INFO) << "PAPI results:";
    for (uint16_t i = 0; i < result.papi_results_.size(); ++i) {
      LOG(INFO) << result.papi_results_[i];
    }
  }
  if (FLAGS_profile) {
    std::cout << "Check out the profile result: pprof --pdf [binary] tpcc.prof > prof.pdf; "
      "okular prof.pdf" << std::endl;
  }

  if (FLAGS_shifting_workload) {
    LOG(INFO) << "Separately writing out bucketed throughputs...";
    // The file name is always "bucketed_throughputs.tsv" and "bucketed_aborts.tsv"
    const char* filenames[] = {
      "bucketed_throughputs.tsv",
      "bucketed_aborts.tsv",
      "bucketed_ratio.tsv",
    };
    for (uint32_t type = 0; type < 3U; ++type) {
      std::ofstream out;
      const char* filename = filenames[type];
      out.open(filename, std::ios_base::out | std::ios_base::trunc);
      if (!out.is_open()) {
        LOG(ERROR) << "Couldn't open " << filename << ". os_error= " << assorted::os_error();
      } else {
        out << "Time\tValue" << std::endl;
        uint64_t prev_value;
        switch (type) {
          case 0U:
            prev_value = sum_buckets->bucketed_throughputs_[0].throughput_;
            break;
          case 1U:
            prev_value = sum_buckets->bucketed_throughputs_[0].aborts_;
            break;
          default:
            prev_value = 0;
        }
        uint64_t prev_ns = bucket_times_raw[0];
        for (uint32_t j = 1; j < max_bucket; ++j) {
          if (bucket_times_raw[j] == prev_ns) {
            LOG(WARNING) << "?? 0ns elapsed?? " << prev_ns;
            bucket_times_raw[j] = prev_ns + 1;
          }
          uint64_t elapsed_ns = bucket_times_raw[j] - prev_ns;
          double value;
          if (type == 0U || type == 1U) {
            value = prev_value * 1000000000.0f / elapsed_ns;
          } else {
            // abort-ratio case simply gets abort-ratio from the previous entry itself
            value =
              static_cast<double>(sum_buckets->bucketed_throughputs_[j - 1].aborts_)
              /
              (sum_buckets->bucketed_throughputs_[j - 1].throughput_
              + sum_buckets->bucketed_throughputs_[j - 1].aborts_);
          }
          out << (prev_ns / 1000000000.0f) << "\t" << value << std::endl;
          if (type == 0U) {
            prev_value = sum_buckets->bucketed_throughputs_[j].throughput_;
          } else if (type == 1U) {
            prev_value = sum_buckets->bucketed_throughputs_[j].aborts_;
          }
          prev_ns = bucket_times_raw[j];
        }
        out.flush();
        out.close();
      }
    }

    // one more tsv that outputs accumulated throughputs
    std::ofstream out;
    const char* filename = "bucketed_accumulated.tsv";
    out.open(filename, std::ios_base::out | std::ios_base::trunc);
    if (!out.is_open()) {
      LOG(ERROR) << "Couldn't open " << filename << ". os_error= " << assorted::os_error();
    } else {
      out << "Time\tValue" << std::endl;
      uint64_t accumulated = 0;
      for (uint32_t j = 0; j < max_bucket; ++j) {
        accumulated += sum_buckets->bucketed_throughputs_[j].throughput_;
        out << (bucket_times_raw[j] / 1000000000.0f) << "\t" << accumulated << std::endl;
      }
      out.flush();
      out.close();
    }


    LOG(INFO) << "Wrote bucketed throughputs.";
  }

  return kRetOk;
}

std::ostream& operator<<(std::ostream& o, const YcsbDriver::Result& v) {
  double avg_scan_length = 0;
  if (v.total_scans_ > 0) {
    avg_scan_length = v.total_scan_length_ / static_cast<double>(v.total_scans_);
  }
  o << "<total_result>"
    << "<duration_sec_>" << v.duration_sec_ << "</duration_sec_>"
    << "<worker_count_>" << v.worker_count_ << "</worker_count_>"
    << "<processed_>" << v.processed_ << "</processed_>"
    << "<MTPS>" << ((v.processed_ / v.duration_sec_) / 1000000) << "</MTPS>"
    << "<race_aborts_>" << v.race_aborts_ << "</race_aborts_>"
    << "<lock_aborts_>" << v.lock_aborts_ << "</lock_aborts_>"
    << "<largereadset_aborts_>" << v.largereadset_aborts_ << "</largereadset_aborts_>"
    << "<insert_conflict_aborts_>" << v.insert_conflict_aborts_ << "</insert_conflict_aborts_>"
    << "<total_scan_length_>" << v.total_scan_length_ << "</total_scan_length_>"
    << "<total_scans_>" << v.total_scans_ << "</total_scans_>"
    << "<average_scan_length_>" << avg_scan_length << "</average_scan_length_>"
    << "<unexpected_aborts_>" << v.unexpected_aborts_ << "</unexpected_aborts_>"
    << "<snapshot_cache_hits_>" << v.snapshot_cache_hits_ << "</snapshot_cache_hits_>"
    << "<snapshot_cache_misses_>" << v.snapshot_cache_misses_ << "</snapshot_cache_misses_>"
    << "</total_result>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const YcsbDriver::WorkerResult& v) {
  double avg_scan_length = 0;
  if (v.total_scans_ > 0) {
    avg_scan_length = v.total_scan_length_ / static_cast<double>(v.total_scans_);
  }
  o << "  <worker_><id>" << v.id_ << "</id>"
    << "<txn>" << v.processed_ << "</txn>"
    << "<raceab>" << v.race_aborts_ << "</raceab>"
    << "<lockab>" << v.lock_aborts_ << "</raceab>"
    << "<rsetab>" << v.largereadset_aborts_ << "</rsetab>"
    << "<insab>"  << v.insert_conflict_aborts_ << "</insab>"
    << "<scanlen>" << v.total_scan_length_ << "</scanlen>"
    << "<scans>" << v.total_scans_ << "</scans>"
    << "<avgscans>" << avg_scan_length << "</avgscans>"
    << "<unexab>" << v.unexpected_aborts_ << "</unexab>"
    << "<sphit>" << v.snapshot_cache_hits_ << "</sphit>"
    << "<spmis>" << v.snapshot_cache_misses_ << "</spmis>"
    << "</worker>";
  return o;
}

}  // namespace ycsb
}  // namespace foedus
