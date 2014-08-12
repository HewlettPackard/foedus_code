/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_driver.hpp"

#include <fcntl.h>
#include <time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/tpcc/tpcc.hpp"
#include "foedus/tpcc/tpcc_client.hpp"
#include "foedus/tpcc/tpcc_load.hpp"

namespace foedus {
namespace tpcc {
DEFINE_bool(profile, false, "Whether to profile the execution with gperftools.");
DEFINE_int32(volatile_pool_size, 8, "Size of volatile memory pool per NUMA node in GB.");
DEFINE_bool(ignore_volatile_size_warning, false, "Ignores warning on volatile_pool_size setting.");
DEFINE_int32(loggers_per_node, 1, "Number of log writers per numa node.");
DEFINE_int32(neworder_remote_percent, 1, "Percent of each orderline that is inserted to remote"
  " warehouse. The default value is 1 (which means a little bit less than 10% of an order has some"
  " remote orderline). This corresponds to H-Store's neworder_multip/neworder_multip_mix in"
  " tpcc.properties.");
DEFINE_int32(payment_remote_percent, 15, "Percent of each payment that is inserted to remote"
  " warehouse. The default value is 15. This corresponds to H-Store's payment_multip/"
  "payment_multip_mix in tpcc.properties.");
DEFINE_bool(single_thread_test, true, "Whether to run a single-threaded sanity test.");
DEFINE_int32(warehouses, 4, "Number of warehouses.");
DEFINE_int64(duration_micro, 5000000, "Duration of benchmark in microseconds.");

TpccDriver::Result TpccDriver::run() {
  const EngineOptions& options = engine_->get_options();
  LOG(INFO) << engine_->get_memory_manager().dump_free_memory_stat();
  assign_wids();
  assign_iids();

  {
    // first, create empty tables. this is done in single thread
    TpccCreateTask creater(FLAGS_warehouses);
    thread::ImpersonateSession creater_session = engine_->get_thread_pool().impersonate(&creater);
    if (!creater_session.is_valid()) {
      COERCE_ERROR(creater_session.invalid_cause_);
      return Result();
    }
    LOG(INFO) << "creator_result=" << creater_session.get_result();
    if (creater_session.get_result().is_error()) {
      COERCE_ERROR(creater_session.get_result());
      return Result();
    }

    storages_ = creater.get_storages();
    storages_.assert_initialized();
  }

  auto& thread_pool = engine_->get_thread_pool();
  {
    // Initialize timestamp (for date columns)
    time_t t_clock;
    ::time(&t_clock);
    const char* timestamp = ::ctime(&t_clock);  // NOLINT(runtime/threadsafe_fn) no race here
    ASSERT_ND(timestamp);

    // then, load data into the tables.
    // this takes long, so it's parallelized.
    std::vector< TpccLoadTask* > tasks;
    std::vector< thread::ImpersonateSession > sessions;
    for (uint16_t node = 0; node < options.thread_.group_count_; ++node) {
      for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ++ordinal) {
        uint16_t count = tasks.size();
        tasks.push_back(new TpccLoadTask(
          FLAGS_warehouses,
          storages_,
          timestamp,
          from_wids_[count],
          to_wids_[count],
          from_iids_[count],
          to_iids_[count]));
        sessions.emplace_back(thread_pool.impersonate_on_numa_node(tasks.back(), node));
        if (!sessions.back().is_valid()) {
          COERCE_ERROR(sessions.back().invalid_cause_);
        }
      }
    }

    bool had_error = false;
    for (uint16_t i = 0; i < sessions.size(); ++i) {
      LOG(INFO) << "loader_result[" << i << "]=" << sessions[i].get_result();
      if (sessions[i].get_result().is_error()) {
        had_error = true;
      }
      delete tasks[i];
    }

    if (had_error) {
      LOG(ERROR) << "Failed data load";
      return Result();
    }
    LOG(INFO) << "Completed data load";
  }


  {
    // first, create empty tables. this is done in single thread
    TpccFinishupTask finishup(FLAGS_warehouses, storages_);
    thread::ImpersonateSession finish_session = thread_pool.impersonate(&finishup);
    if (!finish_session.is_valid()) {
      COERCE_ERROR(finish_session.invalid_cause_);
      return Result();
    }
    LOG(INFO) << "finiish_result=" << finish_session.get_result();
    if (finish_session.get_result().is_error()) {
      COERCE_ERROR(finish_session.get_result());
      return Result();
    }
  }

  LOG(INFO) << engine_->get_memory_manager().dump_free_memory_stat();

  LOG(INFO) << "neworder_remote_percent=" << FLAGS_neworder_remote_percent;
  LOG(INFO) << "payment_remote_percent=" << FLAGS_payment_remote_percent;
  std::vector< thread::ImpersonateSession > sessions;
  for (uint16_t node = 0; node < options.thread_.group_count_; ++node) {
    memory::ScopedNumaPreferred scope(node);
    for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ++ordinal) {
      uint16_t global_ordinal = clients_.size();
      clients_.push_back(new TpccClientTask(
        (node << 8U) + ordinal,
        FLAGS_warehouses,
        from_wids_[global_ordinal],
        to_wids_[global_ordinal],
        FLAGS_neworder_remote_percent,
        FLAGS_payment_remote_percent,
        storages_,
        &start_rendezvous_));
      sessions.emplace_back(thread_pool.impersonate_on_numa_node(clients_.back(), node));
      if (!sessions.back().is_valid()) {
        COERCE_ERROR(sessions.back().invalid_cause_);
      }
    }
  }
  LOG(INFO) << "okay, launched all worker threads";

  // make sure all threads are done with random number generation
  std::this_thread::sleep_for(std::chrono::seconds(3));
  if (FLAGS_profile) {
    COERCE_ERROR(engine_->get_debug().start_profile("tpcc.prof"));
  }
  start_rendezvous_.signal();  // GO!
  LOG(INFO) << "Started!";
  std::this_thread::sleep_for(std::chrono::microseconds(FLAGS_duration_micro));
  LOG(INFO) << "Experiment ended.";

  Result result;
  assorted::memory_fence_acquire();
  for (auto* client : clients_) {
    result.processed_ += client->get_processed();
    result.race_aborts_ += client->get_race_aborts();
    result.unexpected_aborts_ += client->get_unexpected_aborts();
    result.largereadset_aborts_ += client->get_largereadset_aborts();
    result.user_requested_aborts_ += client->get_user_requested_aborts();
  }
  if (FLAGS_profile) {
    engine_->get_debug().stop_profile();
  }
  LOG(INFO) << "Shutting down...";

  assorted::memory_fence_release();
  for (auto* client : clients_) {
    client->request_stop();
  }
  assorted::memory_fence_release();

  LOG(INFO) << "Total thread count=" << clients_.size();
  for (uint16_t i = 0; i < sessions.size(); ++i) {
    LOG(INFO) << "result[" << i << "]=" << sessions[i].get_result();
    delete clients_[i];
  }
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

int driver_main(int argc, char **argv) {
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
  options.savepoint_.savepoint_path_ = savepoint_path.string();
  ASSERT_ND(!fs::exists(savepoint_path));

  LOG(INFO) << "NUMA node count=" << static_cast<int>(options.thread_.group_count_);
  options.snapshot_.folder_path_pattern_ = "/dev/shm/foedus_tpcc/snapshot/node_$NODE$";
  options.log_.folder_path_pattern_ = "/dev/shm/foedus_tpcc/log/node_$NODE$/logger_$LOGGER$";
  options.log_.loggers_per_node_ = FLAGS_loggers_per_node;
  options.log_.flush_at_shutdown_ = false;
  options.xct_.max_read_set_size_ = 1U << 18;
  options.xct_.max_write_set_size_ = 1U << 16;
  options.snapshot_.snapshot_interval_milliseconds_ = 100000000U;
  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogInfo;
    // = debugging::DebuggingOptions::kDebugLogWarning;
  options.debugging_.verbose_modules_ = "";
  options.debugging_.verbose_log_level_ = -1;

  options.log_.log_buffer_kb_ = 1 << 18;  // 256MB * 16 cores = 4 GB. nothing.
  options.log_.log_file_size_mb_ = 1 << 10;
  LOG(INFO) << "volatile_pool_size=" << FLAGS_volatile_pool_size << "GB per NUMA node";
  options.memory_.page_pool_size_mb_per_node_ = (FLAGS_volatile_pool_size) << 10;
  options.cache_.snapshot_cache_size_mb_per_node_ = 1 << 10;

  if (FLAGS_single_thread_test) {
    FLAGS_warehouses = 1;
    options.log_.log_buffer_kb_ = 1 << 16;
    options.log_.log_file_size_mb_ = 1 << 10;
    options.memory_.page_pool_size_mb_per_node_ = 1 << 12;
    options.cache_.snapshot_cache_size_mb_per_node_ = 1 << 12;
    options.thread_.group_count_ = 1;
    options.thread_.thread_count_per_group_ = 1;
  }

  if (!FLAGS_ignore_volatile_size_warning) {
    if (FLAGS_volatile_pool_size < FLAGS_warehouses * 4 / options.thread_.group_count_) {
      LOG(FATAL) << "You have specified: warehouses=" << FLAGS_warehouses << ", which is "
        << (static_cast<float>(FLAGS_warehouses) / options.thread_.group_count_) << " warehouses"
        << " per NUMA node. You should specify at least "
        << (FLAGS_warehouses * 4 / options.thread_.group_count_) << "GB for volatile_pool_size.";
    }
  }

  TpccDriver::Result result;
  {
    Engine engine(options);
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
  LOG(INFO) << result;
  if (FLAGS_profile) {
    LOG(INFO) << "Check out the profile result: pprof --pdf tpcc tpcc.prof > prof.pdf; "
      "okular prof.pdf";
  }
  return 0;
}

std::ostream& operator<<(std::ostream& o, const TpccDriver::Result& v) {
  o << "<total_result>"
    << "<processed_>" << v.processed_ << "</processed_>"
    << "<MTPS>" << (static_cast<double>(v.processed_) / FLAGS_duration_micro) << "</MTPS>"
    << "<user_requested_aborts_>" << v.user_requested_aborts_ << "</user_requested_aborts_>"
    << "<race_aborts_>" << v.race_aborts_ << "</race_aborts_>"
    << "<largereadset_aborts_>" << v.largereadset_aborts_ << "</largereadset_aborts_>"
    << "<unexpected_aborts_>" << v.unexpected_aborts_ << "</unexpected_aborts_>"
    << "</total_result>";
  return o;
}

}  // namespace tpcc
}  // namespace foedus
