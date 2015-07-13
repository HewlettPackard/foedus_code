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
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/thread/numa_thread_scope.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"
#include "foedus/ycsb/ycsb.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace ycsb {
DEFINE_string(nvm_folder, "/dev/shm", "Full path of the device representing NVM.");
DEFINE_int32(volatile_pool_size, 1, "Size of volatile memory pool per NUMA node in GB.");
DEFINE_int32(snapshot_pool_size, 2048, "Size of snapshot memory pool per NUMA node in MB.");
DEFINE_int32(reducer_buffer_size, 1, "Size of reducer's buffer per NUMA node in GB.");
DEFINE_int32(loggers_per_node, 1, "Number of log writers per numa node.");
DEFINE_int32(thread_per_node, 1, "Number of threads per NUMA node. 0 uses logical count");
DEFINE_int32(numa_nodes, 0, "Number of NUMA nodes. 0 uses physical count");
DEFINE_bool(use_numa_alloc, true, "Whether to use ::numa_alloc_interleaved()/::numa_alloc_onnode()"
  " to allocate memories. If false, we use usual posix_memalign() instead");
DEFINE_bool(interleave_numa_alloc, false, "Whether to use ::numa_alloc_interleaved()"
  " instead of ::numa_alloc_onnode()");
DEFINE_bool(mmap_hugepages, false, "Whether to use mmap for 1GB hugepages."
  " This requies special setup written in the readme.");
DEFINE_int32(log_buffer_mb, 512, "Size in MB of log buffer for each thread");
DEFINE_bool(null_log_device, false, "Whether to disable log writing.");
DEFINE_int64(duration_micro, 1000000, "Duration of benchmark in microseconds.");
DEFINE_string(workload, "A", "YCSB workload; choose A/B/C/D/E.");
DEFINE_string(non_scan_table_type, "hash", "Storage type (hash or masstree) of user_table for non-scan transactions.");


YcsbWorkload YcsbWorkloadA('A', 50U,  100U, 0,    0);     // Workload A - 50% read, 50% update
YcsbWorkload YcsbWorkloadB('B', 95U,  100U, 0,    0);     // Workload B - 95% read, 5% update
YcsbWorkload YcsbWorkloadC('C', 100U, 0,    0,    0);     // Workload C - 100% read
YcsbWorkload YcsbWorkloadD('D', 95U,  0,    100U, 0);     // Workload D - 95% read, 5% insert
YcsbWorkload YcsbWorkloadE('E', 0,    0,    5U,   100U);  // Workload E - 5% insert, 95% scan

YcsbRecord::YcsbRecord(char value) {
  // So just write some arbitrary characters provided, no need to use rnd
  memset(data_, value, kFields * kFieldLength * sizeof(char));
}

YcsbKey::YcsbKey() {
  size_ = 0;
  memset(data_, '\0', kKeyMaxLength);
  sprintf(data_, "%s", kKeyPrefix.data());
}

YcsbKey YcsbKey::next(uint32_t worker_id, uint32_t* local_key_counter) {
  auto low = (*local_key_counter)++;
  return build(worker_id, low);
}

YcsbKey YcsbKey::build(uint32_t high_bits, uint32_t low_bits) {
  uint64_t keynum = ((uint64_t)high_bits << 32) | low_bits;
  auto n = sprintf(data_ + kKeyPrefixLength, "%lu", keynum);
  assert(n > 0);
  n += kKeyPrefixLength;
  assert(n <= kKeyMaxLength);
  memset(data_ + n, '\0', kKeyMaxLength - n);
  size_ = n;
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

  // Get an engine, register procedures to run
  Engine engine(options);
  proc::ProcAndName load_proc("ycsb_load_task", ycsb_load_task);
  proc::ProcAndName work_proc("ycsb_client_task", ycsb_client_task);
  engine.get_proc_manager()->pre_register(load_proc);
  engine.get_proc_manager()->pre_register(work_proc);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    // This driver will fire off loading and impersonate clients
    YcsbDriver driver(&engine);
    driver.run(); // this will wait for the loader thread to finish
    COERCE_ERROR(engine.uninitialize());
  }
  return 0;
}

ErrorStack YcsbDriver::run()
{
  // Setup the channel so I can synchronize with workers and record nr_workers
  YcsbClientChannel* channel = get_channel(engine_);
  channel->initialize();

  // Figure out we have to run how many worker threads first.
  // The loader needs it to generate keys.
  const EngineOptions& options = engine_->get_options();
  for (uint16_t node = 0; node < options.thread_.group_count_; node++) {
    for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ordinal++) {
      // XXX: this should be drawn from numa map actually?
      channel->nr_workers_++;
    }
  }

  auto* thread_pool = engine_->get_thread_pool();
  thread::ImpersonateSession load_session;
  bool ret = thread_pool->impersonate("ycsb_load_task", &channel->nr_workers_,
                                      sizeof(uint32_t), &load_session);
  if (not ret) {
    LOG(FATAL) << "Couldn't impersonate";
  }

  // Wait for the load task to finish
  // TODO: parallelize this
  const uint64_t kIntervalMs = 10;
  while (load_session.is_running()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(kIntervalMs));
    assorted::memory_fence_acquire();
  }
  const std::pair<uint32_t, uint32_t> start_key_pair = *reinterpret_cast<const std::pair<uint32_t, uint32_t>* >(load_session.get_raw_output_buffer());
  ASSERT_ND(start_key_pair.second);
  load_session.release();  // Release the loader session, making the thread available again

  // Now try to start transaction worker threads
  uint32_t worker_id = 0;
  std::vector< thread::ImpersonateSession > worker_sessions;
  for (uint16_t node = 0; node < options.thread_.group_count_; node++) {
    for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ordinal++) {
      thread::ImpersonateSession session;
      YcsbClientTask::Inputs inputs;
      //inputs.worker_id_ = (node << 8U) + ordinal;
      inputs.worker_id_ = worker_id;

      if (worker_id < start_key_pair.first) {
        inputs.local_key_counter_ = start_key_pair.second;
      } else {
        inputs.local_key_counter_ = start_key_pair.second - 1;
      }

      if (FLAGS_workload == "A") {
        inputs.workload_ = YcsbWorkloadA;
      } else if (FLAGS_workload == "B") {
        inputs.workload_ = YcsbWorkloadB;
      } else if (FLAGS_workload == "C") {
        inputs.workload_ = YcsbWorkloadC;
      } else if (FLAGS_workload == "D") {
        inputs.workload_ = YcsbWorkloadD;
      } else if (FLAGS_workload == "E") {
        inputs.workload_ = YcsbWorkloadE;
      }
      bool ret = thread_pool->impersonate_on_numa_node(node, "ycsb_client_task", &inputs, sizeof(inputs), &session);
      if (not ret) {
        LOG(FATAL) << "Couldn't impersonate";
      }
      worker_sessions.emplace_back(std::move(session));
      LOG(INFO) << "Thread: " << node << " " << ordinal << " " << inputs.worker_id_;
    }
  }

  // Tell everybody to start
  channel->start_rendezvous_.signal();
  assorted::memory_fence_release();
  LOG(INFO) << "Started!";
  debugging::StopWatch duration;
  while (duration.peek_elapsed_ns() < static_cast<uint64_t>(FLAGS_duration_micro) * 1000ULL) {
    // Wait for workers to finish
    //for (auto &session : worker_sessions) {
    //}
    //LOG(INFO) << engine_->get_memory_manager()->dump_free_memory_stat();
  }
  duration.stop();

  LOG(INFO) << "Shutting down...";

  // output the current memory state at the end
  LOG(INFO) << engine_->get_memory_manager()->dump_free_memory_stat();

  channel->stop_flag_.store(true);

  for (uint32_t i = 0; i < worker_sessions.size(); i++) {
    auto& session = worker_sessions[i];
    LOG(INFO) << "result[" << i << "]=" << session.get_result();
    session.release();
  }
  channel->uninitialize();
  return kRetOk;
}

}  // namespace ycsb
}  // namespace foedus
