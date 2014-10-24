/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_driver.hpp"

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
#include "foedus/memory/engine_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/thread/numa_thread_scope.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"
#include "foedus/tpcc/tpcc.hpp"
#include "foedus/tpcc/tpcc_client.hpp"
#include "foedus/tpcc/tpcc_load.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace tpcc {
DEFINE_bool(fork_workers, false, "Whether to fork(2) worker threads in child processes rather"
    " than threads in the same process. This is required to scale up to 100+ cores.");
DEFINE_bool(take_snapshot, false, "Whether to run a log gleaner after loading data.");
DEFINE_string(nvm_folder, "/nvmmnt", "Full path of the device representing NVM.");
DEFINE_bool(exec_duplicates, false, "[Experimental] Whether to fork/exec(2) worker threads in child"
    " processes on replicated binaries. This is required to scale up to 16 sockets.");
DEFINE_bool(profile, false, "Whether to profile the execution with gperftools.");
DEFINE_bool(papi, false, "Whether to profile with PAPI.");
DEFINE_int32(volatile_pool_size, 4, "Size of volatile memory pool per NUMA node in GB.");
DEFINE_int32(loggers_per_node, 1, "Number of log writers per numa node.");
DEFINE_int32(neworder_remote_percent, 1, "Percent of each orderline that is inserted to remote"
  " warehouse. The default value is 1 (which means a little bit less than 10% of an order has some"
  " remote orderline). This corresponds to H-Store's neworder_multip/neworder_multip_mix in"
  " tpcc.properties.");
DEFINE_int32(payment_remote_percent, 15, "Percent of each payment that is inserted to remote"
  " warehouse. The default value is 15. This corresponds to H-Store's payment_multip/"
  "payment_multip_mix in tpcc.properties.");
DEFINE_bool(single_thread_test, false, "Whether to run a single-threaded sanity test.");
DEFINE_int32(thread_per_node, 4, "Number of threads per NUMA node. 0 uses logical count");
DEFINE_int32(numa_nodes, 2, "Number of NUMA nodes. 0 uses physical count");
DEFINE_bool(use_numa_alloc, true, "Whether to use ::numa_alloc_interleaved()/::numa_alloc_onnode()"
  " to allocate memories. If false, we use usual posix_memalign() instead");
DEFINE_bool(interleave_numa_alloc, false, "Whether to use ::numa_alloc_interleaved()"
  " instead of ::numa_alloc_onnode()");
DEFINE_bool(mmap_hugepages, false, "Whether to use mmap for 1GB hugepages."
  " This requies special setup written in the readme.");
DEFINE_int32(log_buffer_mb, 128, "Size in MB of log buffer for each thread");
DEFINE_bool(null_log_device, false, "Whether to disable log writing.");
DEFINE_bool(high_priority, false, "Set high priority to threads. Needs 'rtprio 99' in limits.conf");
DEFINE_int32(warehouses, 8, "Number of warehouses.");
DEFINE_int64(duration_micro, 5000000, "Duration of benchmark in microseconds.");

TpccDriver::Result TpccDriver::run() {
  const EngineOptions& options = engine_->get_options();
  LOG(INFO) << engine_->get_memory_manager()->dump_free_memory_stat();
  assign_wids();
  assign_iids();

  {
    // first, create empty tables. this is done in single thread
    ErrorStack create_result = create_all(engine_, FLAGS_warehouses);
    LOG(INFO) << "creator_result=" << create_result;
    if (create_result.is_error()) {
      COERCE_ERROR(create_result);
      return Result();
    }
  }

  auto* thread_pool = engine_->get_thread_pool();
  {
    // Initialize timestamp (for date columns)
    time_t t_clock;
    ::time(&t_clock);
    const char* timestamp = ::ctime(&t_clock);  // NOLINT(runtime/threadsafe_fn) no race here
    ASSERT_ND(timestamp);

    // then, load data into the tables.
    // this takes long, so it's parallelized.
    std::vector< thread::ImpersonateSession > sessions;
    for (uint16_t node = 0; node < options.thread_.group_count_; ++node) {
      for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ++ordinal) {
        uint16_t count = sessions.size();
        TpccLoadTask::Inputs inputs;
        inputs.total_warehouses_ = FLAGS_warehouses;
        inputs.timestamp_ = timestamp;
        inputs.from_wid_ = from_wids_[count];
        inputs.to_wid_ = to_wids_[count];
        inputs.from_iid_ = from_iids_[count];
        inputs.to_iid_ = to_iids_[count];
        thread::ImpersonateSession session;
        bool ret = thread_pool->impersonate_on_numa_node(
          node,
          "tpcc_load_task",
          &inputs,
          sizeof(inputs),
          &session);
        if (!ret) {
          LOG(FATAL) << "Couldn't impersonate";
        }
        sessions.emplace_back(std::move(session));
      }
    }

    bool had_error = false;
    for (uint16_t i = 0; i < sessions.size(); ++i) {
      LOG(INFO) << "loader_result[" << i << "]=" << sessions[i].get_result();
      if (sessions[i].get_result().is_error()) {
        had_error = true;
      }
      sessions[i].release();
    }

    if (had_error) {
      LOG(ERROR) << "Failed data load";
      return Result();
    }
    LOG(INFO) << "Completed data load";
  }


  // Verify the loaded data. this is done in single thread
  Wid total_warehouses = FLAGS_warehouses;
  ErrorStack finishup_result = thread_pool->impersonate_synchronous(
    "tpcc_finishup_task",
    &total_warehouses,
    sizeof(total_warehouses));
  LOG(INFO) << "finish_result=" << finishup_result;
  if (finishup_result.is_error()) {
    COERCE_ERROR(finishup_result);
    return Result();
  }

  LOG(INFO) << engine_->get_memory_manager()->dump_free_memory_stat();

  LOG(INFO) << "neworder_remote_percent=" << FLAGS_neworder_remote_percent;
  LOG(INFO) << "payment_remote_percent=" << FLAGS_payment_remote_percent;


  if (FLAGS_take_snapshot) {
    LOG(INFO) << "Now taking a snapshot...";
    debugging::StopWatch watch;
    engine_->get_snapshot_manager()->trigger_snapshot_immediate(true);
    watch.stop();
    LOG(INFO) << "Took a snapshot in " << watch.elapsed_ms() << "ms";
  }

  TpccClientChannel* channel = reinterpret_cast<TpccClientChannel*>(
    engine_->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
  channel->initialize();

  std::vector< thread::ImpersonateSession > sessions;
  std::vector< const TpccClientTask::Outputs* > outputs;

  for (uint16_t node = 0; node < options.thread_.group_count_; ++node) {
    for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ++ordinal) {
      uint16_t global_ordinal = options.thread_.thread_count_per_group_ * node + ordinal;
      TpccClientTask::Inputs inputs;
      inputs.worker_id_ = (node << 8U) + ordinal;
      inputs.total_warehouses_ = FLAGS_warehouses;
      inputs.from_wid_ = from_wids_[global_ordinal];
      inputs.to_wid_ = to_wids_[global_ordinal];
      inputs.neworder_remote_percent_ = FLAGS_neworder_remote_percent;
      inputs.payment_remote_percent_ = FLAGS_payment_remote_percent;
      thread::ImpersonateSession session;
      bool ret = thread_pool->impersonate_on_numa_node(
        node,
        "tpcc_client_task",
        &inputs,
        sizeof(inputs),
        &session);
      if (!ret) {
        LOG(FATAL) << "Couldn't impersonate";
      }
      outputs.push_back(
        reinterpret_cast<const TpccClientTask::Outputs*>(session.get_raw_output_buffer()));
      sessions.emplace_back(std::move(session));
    }
  }
  LOG(INFO) << "okay, launched all worker threads. waiting for completion of warmup...";
  uint32_t total_thread_count = options.thread_.get_total_thread_count();
  while (channel->warmup_complete_counter_.load() < total_thread_count) {
    LOG(INFO) << "Waiting for warmup completion... done=" << channel->warmup_complete_counter_
      << "/" << total_thread_count;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  LOG(INFO) << "All warmup done!";
  if (FLAGS_profile) {
    COERCE_ERROR(engine_->get_debug()->start_profile("tpcc.prof"));
  }
  if (FLAGS_papi) {
    engine_->get_debug()->start_papi_counters();
  }
  channel->start_rendezvous_.signal();
  assorted::memory_fence_release();
  LOG(INFO) << "Started!";
  debugging::StopWatch duration;
  while (duration.peek_elapsed_ns() < static_cast<uint64_t>(FLAGS_duration_micro) * 1000ULL) {
    // wake up for each second to show intermediate results.
    uint64_t remaining_duration = FLAGS_duration_micro - duration.peek_elapsed_ns() / 1000ULL;
    remaining_duration = std::min<uint64_t>(remaining_duration, 1000000ULL);
    std::this_thread::sleep_for(std::chrono::microseconds(remaining_duration));
    Result result;
    result.duration_sec_ = static_cast<double>(duration.peek_elapsed_ns()) / 1000000000;
    result.worker_count_ = total_thread_count;
    for (uint32_t i = 0; i < sessions.size(); ++i) {
      const TpccClientTask::Outputs* output = outputs[i];
      result.processed_ += output->processed_;
      result.race_aborts_ += output->race_aborts_;
      result.unexpected_aborts_ += output->unexpected_aborts_;
      result.largereadset_aborts_ += output->largereadset_aborts_;
      result.user_requested_aborts_ += output->user_requested_aborts_;
    }
    LOG(INFO) << "Intermediate report after " << result.duration_sec_ << " sec";
    LOG(INFO) << result;
    LOG(INFO) << engine_->get_memory_manager()->dump_free_memory_stat();
  }
  LOG(INFO) << "Experiment ended.";

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
  for (uint32_t i = 0; i < sessions.size(); ++i) {
    const TpccClientTask::Outputs* output = outputs[i];
    result.workers_[i].id_ = i;
    result.workers_[i].processed_ = output->processed_;
    result.workers_[i].race_aborts_ = output->race_aborts_;
    result.workers_[i].unexpected_aborts_ = output->unexpected_aborts_;
    result.workers_[i].largereadset_aborts_ = output->largereadset_aborts_;
    result.workers_[i].user_requested_aborts_ = output->user_requested_aborts_;
    result.processed_ += output->processed_;
    result.race_aborts_ += output->race_aborts_;
    result.unexpected_aborts_ += output->unexpected_aborts_;
    result.largereadset_aborts_ += output->largereadset_aborts_;
    result.user_requested_aborts_ += output->user_requested_aborts_;
  }
  LOG(INFO) << "Shutting down...";

  // output the current memory state at the end
  LOG(INFO) << engine_->get_memory_manager()->dump_free_memory_stat();

  channel->stop_flag_.store(true);

  for (uint32_t i = 0; i < sessions.size(); ++i) {
    LOG(INFO) << "result[" << i << "]=" << sessions[i].get_result();
    sessions[i].release();
  }
  channel->uninitialize();
  return result;
}

template <typename T>
void assign_ids(
  uint64_t total_count,
  const EngineOptions& options,
  std::vector<T>* from_ids,
  std::vector<T>* to_ids) {
  // divide warehouses/items into threads as even as possible.
  // we explicitly specify which nodes to take which WID and assign it in the later execution
  // as a DORA-like partitioning.
  ASSERT_ND(from_ids->size() == 0);
  ASSERT_ND(to_ids->size() == 0);
  const uint16_t total_thread_count = options.thread_.get_total_thread_count();
  const float wids_per_thread = static_cast<float>(total_count) / total_thread_count;
  uint64_t assigned = 0;
  uint64_t min_assignments = 0xFFFFFFFFFFFFFFFFULL;
  uint64_t max_assignments = 0;
  for (uint16_t node = 0; node < options.thread_.group_count_; ++node) {
    for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ++ordinal) {
      uint64_t wids;
      if (node == options.thread_.group_count_ &&
        ordinal == options.thread_.thread_count_per_group_) {
        // all the remaining
        wids = total_count - assigned;
        ASSERT_ND(wids < wids_per_thread + 2);  // not too skewed
      } else {
        uint16_t thread_count = from_ids->size();
        wids = static_cast<uint64_t>(wids_per_thread * (thread_count + 1) - assigned);
      }
      min_assignments = std::min<uint64_t>(min_assignments, wids);
      max_assignments = std::max<uint64_t>(max_assignments, wids);
      from_ids->push_back(assigned);
      to_ids->push_back(assigned + wids);
      assigned += wids;
    }
  }
  ASSERT_ND(from_ids->size() == total_thread_count);
  ASSERT_ND(to_ids->size() == total_thread_count);
  ASSERT_ND(to_ids->back() == total_count);
  LOG(INFO) << "Assignments, min=" << min_assignments << ", max=" << max_assignments
    << ", threads=" << total_thread_count << ", total_count=" << total_count;
}

void TpccDriver::assign_wids() {
  assign_ids<Wid>(FLAGS_warehouses, engine_->get_options(), &from_wids_, &to_wids_);
}
void TpccDriver::assign_iids() {
  assign_ids<Iid>(kItems, engine_->get_options(), &from_iids_, &to_iids_);
}

/** This method just constructs options and gives it to engine object. Nothing more */
int driver_main(int argc, char **argv) {
  std::vector< proc::ProcAndName > procs;
  procs.emplace_back("tpcc_client_task", tpcc_client_task);
  procs.emplace_back("tpcc_finishup_task", tpcc_finishup_task);
  procs.emplace_back("tpcc_load_task", tpcc_load_task);
  {
    // In case the main() was called for exec()-style SOC engines.
    soc::SocManager::trap_spawned_soc_main(procs);
  }
  gflags::SetUsageMessage("TPC-C implementation for FOEDUS");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fs::Path folder("/dev/shm/foedus_tpcc");
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

  options.snapshot_.folder_path_pattern_ = "/dev/shm/foedus_tpcc/snapshot/node_$NODE$";
  options.log_.folder_path_pattern_ = "/dev/shm/foedus_tpcc/log/node_$NODE$/logger_$LOGGER$";
  options.log_.loggers_per_node_ = FLAGS_loggers_per_node;
  options.log_.flush_at_shutdown_ = false;
  options.snapshot_.snapshot_interval_milliseconds_ = 100000000U;

  if (FLAGS_take_snapshot) {
    std::cout << "Will take snapshot after initial data load." << std::endl;
    FLAGS_null_log_device = false;

    options.snapshot_.log_mapper_io_buffer_mb_ = 1 << 8;
    options.snapshot_.log_reducer_buffer_mb_ = 1 << 11;
    options.snapshot_.snapshot_writer_page_pool_size_mb_ = 1 << 10;
    options.snapshot_.snapshot_writer_intermediate_pool_size_mb_ = 1 << 8;
    options.cache_.snapshot_cache_size_mb_per_node_ = 1 << 13;

    fs::Path nvm_folder(FLAGS_nvm_folder);
    if (!fs::exists(nvm_folder)) {
      std::cerr << "The NVM-folder " << nvm_folder << " not mounted yet";
      return 1;
    }

    fs::Path tpcc_folder(nvm_folder);
    tpcc_folder /= "foedus_tpcc";
    if (fs::exists(tpcc_folder)) {
      fs::remove_all(tpcc_folder);
    }
    if (!fs::create_directories(tpcc_folder)) {
      std::cerr << "Couldn't create " << tpcc_folder << ". err=" << assorted::os_error();
      return 1;
    }

    savepoint_path = tpcc_folder;
    savepoint_path /= "savepoint.xml";
    if (fs::exists(savepoint_path)) {
      fs::remove(savepoint_path);
    }
    ASSERT_ND(!fs::exists(savepoint_path));
    options.savepoint_.savepoint_path_.assign(savepoint_path.string());

    fs::Path snapshot_folder(tpcc_folder);
    snapshot_folder /= "snapshot";
    if (fs::exists(snapshot_folder)) {
      fs::remove_all(snapshot_folder);
    }
    fs::Path snapshot_pattern(snapshot_folder);
    snapshot_pattern /= "node_$NODE$";
    options.snapshot_.folder_path_pattern_.assign(snapshot_pattern.string());

    fs::Path log_folder(tpcc_folder);
    log_folder /= "log";
    if (fs::exists(log_folder)) {
      fs::remove_all(log_folder);
    }
    fs::Path log_pattern(log_folder);
    log_pattern /= "node_$NODE$/logger_$LOGGER$";
    options.log_.folder_path_pattern_.assign(log_pattern.string());
  }

  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogInfo;
    // = debugging::DebuggingOptions::kDebugLogWarning;
  options.debugging_.verbose_modules_ = "";
  options.debugging_.verbose_log_level_ = -1;

  options.log_.log_buffer_kb_ = FLAGS_log_buffer_mb << 10;
  std::cout << "log_buffer_mb=" << FLAGS_log_buffer_mb << "MB per thread" << std::endl;
  options.log_.log_file_size_mb_ = 1 << 10;
  std::cout << "volatile_pool_size=" << FLAGS_volatile_pool_size << "GB per NUMA node" << std::endl;
  options.memory_.page_pool_size_mb_per_node_ = (FLAGS_volatile_pool_size) << 10;
  options.cache_.snapshot_cache_size_mb_per_node_ = 1 << 10;

  if (FLAGS_thread_per_node != 0) {
    std::cout << "thread_per_node=" << FLAGS_thread_per_node << std::endl;
    options.thread_.thread_count_per_group_ = FLAGS_thread_per_node;
  }

  if (FLAGS_null_log_device) {
    std::cout << "/dev/null log device" << std::endl;
    options.log_.emulation_.null_device_ = true;
  }

  if (FLAGS_single_thread_test) {
    FLAGS_warehouses = 1;
    options.log_.log_buffer_kb_ = 1 << 16;
    options.log_.log_file_size_mb_ = 1 << 10;
    options.log_.loggers_per_node_ = 1;
    options.memory_.page_pool_size_mb_per_node_ = 1 << 12;
    options.cache_.snapshot_cache_size_mb_per_node_ = 1 << 12;
    options.thread_.group_count_ = 1;
    options.thread_.thread_count_per_group_ = 1;
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

  TpccDriver::Result result;
  {
    Engine engine(options);
    for (const proc::ProcAndName& proc : procs) {
      engine.get_proc_manager()->pre_register(proc);
    }
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      TpccDriver driver(&engine);
      result = driver.run();
      COERCE_ERROR(engine.uninitialize());
    }
  }

  // wait just for a bit to avoid mixing stdout
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
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
    std::cout << "Check out the profile result: pprof --pdf tpcc tpcc.prof > prof.pdf; "
      "okular prof.pdf" << std::endl;
  }

  return 0;
}

std::ostream& operator<<(std::ostream& o, const TpccDriver::Result& v) {
  o << "<total_result>"
    << "<duration_sec_>" << v.duration_sec_ << "</duration_sec_>"
    << "<worker_count_>" << v.worker_count_ << "</worker_count_>"
    << "<processed_>" << v.processed_ << "</processed_>"
    << "<MTPS>" << ((v.processed_ / v.duration_sec_) / 1000000) << "</MTPS>"
    << "<user_requested_aborts_>" << v.user_requested_aborts_ << "</user_requested_aborts_>"
    << "<race_aborts_>" << v.race_aborts_ << "</race_aborts_>"
    << "<largereadset_aborts_>" << v.largereadset_aborts_ << "</largereadset_aborts_>"
    << "<unexpected_aborts_>" << v.unexpected_aborts_ << "</unexpected_aborts_>";
  o << "</total_result>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const TpccDriver::WorkerResult& v) {
  o << "  <worker_><id>" << v.id_ << "</id>"
    << "<txn>" << v.processed_ << "</txn>"
    << "<usrab>" << v.user_requested_aborts_ << "</usrab>"
    << "<raceab>" << v.race_aborts_ << "</raceab>"
    << "<rsetab>" << v.largereadset_aborts_ << "</rsetab>"
    << "<unexab>" << v.unexpected_aborts_ << "</unexab>"
    << "</worker>";
  return o;
}

}  // namespace tpcc
}  // namespace foedus
