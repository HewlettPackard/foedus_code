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
#include "foedus/sssp/sssp_driver.hpp"

#include <numa.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
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
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/sssp/sssp_client.hpp"
#include "foedus/sssp/sssp_common.hpp"
#include "foedus/sssp/sssp_load.hpp"
#include "foedus/thread/numa_thread_scope.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace sssp {

DEFINE_bool(htt, true, "Whether the machine runs hyper-threaded cores. Used only for sizing");
DEFINE_int32(duration, 10, "Duration of the experiments in seconds.");
DEFINE_int32(p_x, 1, "Number of partitions in x direction. Increase this to enlarge data");
DEFINE_int32(p_y, 1, "Number of partitions in y direction. Increase this to enlarge data");
DEFINE_bool(profile, false, "Whether to profile the execution with gperftools.");
DEFINE_int32(navigation_per_node, 0, "Workers per node to spend for navigational queries.");
DEFINE_int32(analysis_per_node, 1, "Workers per node to spend for analytic queries.");
DEFINE_int32(numa_nodes, 1, "Number of NUMA nodes. 0 uses physical count");

SsspDriver::Result SsspDriver::run() {
  const EngineOptions& options = engine_->get_options();
  LOG(INFO) << engine_->get_memory_manager()->dump_free_memory_stat();

  const uint32_t total_partitions = FLAGS_p_x * FLAGS_p_y;
  const uint32_t sockets = options.thread_.group_count_;
  const uint32_t cores_per_socket = options.thread_.thread_count_per_group_;
  const uint32_t partitions_per_socket = assorted::int_div_ceil(total_partitions, sockets);
  {
    // first, create empty tables. this is done in single thread
    ErrorStack create_result = create_all(engine_, total_partitions);
    LOG(INFO) << "creator_result=" << create_result;
    if (create_result.is_error()) {
      COERCE_ERROR(create_result);
      return Result();
    }
  }

  auto* thread_pool = engine_->get_thread_pool();
  {
    // load data into the tables. this takes long, so it's parallelized.
    // The way we parallelize is based on partition and socket.
    // For navigational query, we want to make sure partitions have
    // primary "owner" socket and placed there.
    std::vector< thread::ImpersonateSession > sessions;
    for (uint16_t socket = 0; socket < sockets; ++socket) {
      const uint32_t socket_from_partition = partitions_per_socket * socket;
      uint32_t socket_to_partition = socket_from_partition + partitions_per_socket;
      // In case #partitions is not a multiply of #sockets
      if (socket_to_partition > total_partitions) {
        socket_to_partition = total_partitions;
      }
      const uint32_t socket_partitions = socket_to_partition - socket_from_partition;
      if (socket_partitions == 0) {
        break;
      }

      const uint32_t partitions_per_core
        = assorted::int_div_ceil(socket_partitions, cores_per_socket);
      for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ++ordinal) {
        const uint32_t core_from_partition = socket_from_partition + partitions_per_core * ordinal;
        uint32_t core_to_partition = core_from_partition + partitions_per_core;
        if (core_to_partition > socket_to_partition) {
          core_to_partition = socket_to_partition;
        }
        if (core_from_partition == core_to_partition) {
          break;
        }

        SsspLoadTask::Inputs inputs;
        inputs.max_node_id_ = kNodesPerPartition * total_partitions - 1U;
        inputs.partition_from_ = core_from_partition;
        inputs.partition_to_ = core_to_partition;
        inputs.max_px_ = FLAGS_p_x;
        inputs.max_py_ = FLAGS_p_y;
        thread::ImpersonateSession session;
        bool ret = thread_pool->impersonate_on_numa_node(
          socket,
          "sssp_load_task",
          &inputs,
          sizeof(inputs),
          &session);
        if (!ret) {
          LOG(FATAL) << "Couldn't impersonate";
        }
        sessions.emplace_back(std::move(session));
      }
    }

    const uint64_t kMaxWaitMs = 60 * 1000;
    const uint64_t kIntervalMs = 10;
    uint64_t wait_count = 0;
    for (uint16_t i = 0; i < sessions.size();) {
      assorted::memory_fence_acquire();
      if (wait_count * kIntervalMs > kMaxWaitMs) {
        LOG(FATAL) << "Data population is taking much longer than expected. Quiting.";
      }
      if (sessions[i].is_running()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kIntervalMs));
        ++wait_count;
        continue;
      }
      LOG(INFO) << "loader_result[" << i << "]=" << sessions[i].get_result();
      if (sessions[i].get_result().is_error()) {
        LOG(FATAL) << "Failed data load " << sessions[i].get_result();
      }
      sessions[i].release();
      ++i;
    }

    LOG(INFO) << "Completed data load";
  }

  SsspClientChannel* channel = reinterpret_cast<SsspClientChannel*>(
    engine_->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
  channel->initialize();

  std::vector< thread::ImpersonateSession > sessions;
  std::vector< const SsspClientTask::Outputs* > outputs;

  // Now launch workers. we have 2 types of workers, navigational and analytic
  // For navigational workers, we do the same partitioning again, but
  // this time we have fewer cores per socket.
  uint16_t navigational_count = 0;
  uint16_t analytic_count = 0;
  const uint16_t analytic_leader_index = FLAGS_navigation_per_node;  // socket-0, first anl worker
  for (uint16_t socket = 0; socket < sockets; ++socket) {
    uint32_t socket_from_partition = partitions_per_socket * socket;
    uint32_t socket_to_partition = socket_from_partition + partitions_per_socket;
    if (socket_to_partition > total_partitions) {
      socket_to_partition = total_partitions;
    }
    const uint32_t socket_partitions = socket_to_partition - socket_from_partition;
    if (socket_partitions == 0) {
      // In this case this socket covers all partitions.
      socket_from_partition = 0;
      socket_to_partition = total_partitions;
    }
    const uint32_t nav_partitions_per_core
      = FLAGS_navigation_per_node == 0
        ? 0U
        : assorted::int_div_ceil(socket_partitions, FLAGS_navigation_per_node);
    for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ++ordinal) {
      SsspClientTask::Inputs inputs;
      inputs.worker_id_ = (socket << 8U) + ordinal;
      inputs.sockets_count_ = sockets;
      inputs.my_socket_ = socket;
      inputs.max_node_id_ = kNodesPerPartition * total_partitions - 1U;
      inputs.max_px_ = FLAGS_p_x;
      inputs.max_py_ = FLAGS_p_y;
      inputs.analytic_workers_per_socket_ = FLAGS_analysis_per_node;
      inputs.navigational_workers_per_socket_ = FLAGS_navigation_per_node;
      inputs.analytic_my_worker_index_per_socket_ = 0;
      inputs.analytic_stripe_x_size_ = FLAGS_analysis_per_node;
      inputs.analytic_stripe_y_size_ = sockets;
      inputs.analytic_stripe_x_count_
        = assorted::int_div_ceil(FLAGS_p_x * kPartitionSize, inputs.analytic_stripe_x_size_);
      inputs.analytic_stripe_y_count_
        = assorted::int_div_ceil(FLAGS_p_y * kPartitionSize, inputs.analytic_stripe_y_size_);
      inputs.analytic_stripe_max_axis_
        = inputs.analytic_stripe_x_count_ + inputs.analytic_stripe_y_count_ - 1U;
      inputs.analytic_total_stripe_count_
        = inputs.analytic_stripe_max_axis_ * inputs.analytic_stripe_max_axis_;
      inputs.analytic_stripes_per_l1_
        = assorted::int_div_ceil(inputs.analytic_total_stripe_count_, kL1VersionFactors);
      uint64_t output_size
        = inputs.analytic_total_stripe_count_ * sizeof(VersionCounter)
          + sizeof(SsspClientTask::Outputs);
      if (output_size > soc::ThreadMemoryAnchors::kTaskOutputMemorySize) {
        LOG(FATAL) << "Ohhh, number of stripes is too large.";
      }
      if (ordinal >= static_cast<uint16_t>(FLAGS_navigation_per_node)) {
        inputs.navigational_ = false;
        inputs.analytic_leader_ = (analytic_count == 0);
        inputs.buddy_index_ = analytic_count;
        ++analytic_count;
        inputs.analytic_my_worker_index_per_socket_ = ordinal - FLAGS_navigation_per_node;
        inputs.nav_partition_from_ = 0;
        inputs.nav_partition_to_ = 0;
      } else {
        inputs.navigational_ = true;
        inputs.analytic_leader_ = false;

        inputs.buddy_index_ = navigational_count;
        ++navigational_count;

        // A bit special rule here.
        // We assign navigational workers to as specific partitions as possible,
        // which would increase access locality (even for socket-local access, L1/L2 is limited).
        // However, there might not be as many partitions as cores.
        // In that case, some workers are assigned to cover all partition range in the socket.
        // This doesn't cause any race. The queries are read-only.
        uint32_t from_partition = socket_from_partition + nav_partitions_per_core * ordinal;
        uint32_t to_partition = from_partition + nav_partitions_per_core;
        if (to_partition > socket_to_partition) {
          to_partition = socket_to_partition;
        }
        if (from_partition == to_partition) {
          from_partition = socket_from_partition;
          to_partition = socket_to_partition;
        }

        inputs.nav_partition_from_ = from_partition;
        inputs.nav_partition_to_ = to_partition;
      }
      thread::ImpersonateSession session;
      bool ret = thread_pool->impersonate_on_numa_node(
        socket,
        "sssp_client_task",
        &inputs,
        sizeof(inputs),
        &session);
      if (!ret) {
        LOG(FATAL) << "Couldn't impersonate";
      }
      outputs.push_back(
        reinterpret_cast<const SsspClientTask::Outputs*>(session.get_raw_output_buffer()));
      sessions.emplace_back(std::move(session));
    }
  }
  LOG(INFO) << "okay, launched all worker threads. waiting for completion of warmup...";
  if (FLAGS_profile) {
    COERCE_ERROR(engine_->get_debug()->start_profile("sssp.prof"));
  }
  channel->start_rendezvous_.signal();
  assorted::memory_fence_release();
  LOG(INFO) << "Started!";
  debugging::StopWatch duration;
  while (duration.peek_elapsed_ns() / 1000000000ULL < static_cast<uint64_t>(FLAGS_duration)) {
    // wake up for each second to show intermediate results.
    uint64_t remaining_ms = FLAGS_duration * 1000ULL - duration.peek_elapsed_ns() / 1000000ULL;
    remaining_ms = std::min<uint64_t>(remaining_ms, 1000ULL);
    std::this_thread::sleep_for(std::chrono::milliseconds(remaining_ms));
    Result result;
    result.duration_sec_ = static_cast<double>(duration.peek_elapsed_ns()) / 1000000000;
    result.navigation_worker_count_ = FLAGS_navigation_per_node * sockets;
    const SsspClientTask::Outputs* analytic_output = outputs[analytic_leader_index];
    result.analysis_result_.processed_ = analytic_output->analytic_processed_;
    result.analysis_result_.total_microseconds_ = analytic_output->analytic_total_microseconds_;
    for (uint32_t i = 0; i < sessions.size(); ++i) {
      const SsspClientTask::Outputs* output = outputs[i];
      result.navigation_workers_[i].processed_ += output->navigational_processed_;
      result.navigation_total_ += output->navigational_processed_;
      ASSERT_ND(static_cast<int>(SsspDriver::kAnalyticAbortTypes)
        == static_cast<int>(SsspClientTask::kAnalyticAbortTypes));
      for (uint32_t j = 0; j < SsspDriver::kAnalyticAbortTypes; ++j) {
        result.analysis_result_.total_aborts_[j] += output->analytic_aborts_[j];
      }
    }
    LOG(INFO) << "Intermediate report after " << result.duration_sec_ << " sec";
    LOG(INFO) << result;
    LOG(INFO) << engine_->get_memory_manager()->dump_free_memory_stat();
  }
  LOG(INFO) << "Experiment ended.";

  if (FLAGS_profile) {
    engine_->get_debug()->stop_profile();
  }

  Result result;
  duration.stop();
  result.duration_sec_ = duration.elapsed_sec();
  result.navigation_worker_count_ = FLAGS_navigation_per_node * sockets;
  assorted::memory_fence_acquire();
  const SsspClientTask::Outputs* analytic_output = outputs[analytic_leader_index];
  result.analysis_result_.processed_ = analytic_output->analytic_processed_;
  result.analysis_result_.total_microseconds_ = analytic_output->analytic_total_microseconds_;
  for (uint32_t i = 0; i < sessions.size(); ++i) {
    const SsspClientTask::Outputs* output = outputs[i];
    result.navigation_workers_[i].processed_ = output->navigational_processed_;
    result.navigation_total_ += output->navigational_processed_;
    for (uint32_t j = 0; j < SsspDriver::kAnalyticAbortTypes; ++j) {
      result.analysis_result_.total_aborts_[j] += output->analytic_aborts_[j];
    }
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



/** This method just constructs options and gives it to engine object. Nothing more */
int driver_main(int argc, char **argv) {
  std::vector< proc::ProcAndName > procs;
  procs.emplace_back("sssp_client_task", sssp_client_task);
  procs.emplace_back("sssp_load_task", sssp_load_task);
  gflags::SetUsageMessage("SSSP query engine");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (::numa_available() < 0) {
    LOG(ERROR) << "NUMA feature is not available on this machine. Exitting";
    return 1;
  }

  fs::Path folder("/dev/shm/foedus_sssp");
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

  options.snapshot_.folder_path_pattern_ = "/dev/shm/foedus_sssp/snapshot/node_$NODE$";
  options.log_.folder_path_pattern_ = "/dev/shm/foedus_sssp/log/node_$NODE$/logger_$LOGGER$";
  options.log_.loggers_per_node_ = 1;
  options.log_.flush_at_shutdown_ = false;
  // This experiment turns off log write to do a fair comparison with Graphlab
  options.log_.emulation_.null_device_ = true;
  options.xct_.max_read_set_size_ = 1U << 20;  // for navigational queries, we need many more
  options.snapshot_.snapshot_interval_milliseconds_ = 100000000U;

  options.debugging_.debug_log_min_threshold_ = debugging::DebuggingOptions::kDebugLogInfo;
  options.debugging_.verbose_modules_ = "";
  options.debugging_.verbose_log_level_ = -1;

  options.log_.log_buffer_kb_ = 1U << 20;
  options.log_.log_file_size_mb_ = 1 << 15;

  uint32_t total_per_node = (FLAGS_navigation_per_node + FLAGS_analysis_per_node);
  std::cout << "navigation_per_node=" << FLAGS_navigation_per_node
    << ", analysis_per_node=" << FLAGS_analysis_per_node
    << ", total_per_node=" << total_per_node << std::endl;
  options.thread_.thread_count_per_group_ = total_per_node;

  // this program automatically adjusts volatile pool size.
  const uint32_t kVolatilePoolSizeBuffer = 3;
  uint64_t total_volatile_pool_bytes
    = kVolatilePoolSizeBuffer * sizeof(Partition) * FLAGS_p_x * FLAGS_p_y;
  uint64_t volatile_pool_bytes_per_socket
    = assorted::int_div_ceil(total_volatile_pool_bytes, options.thread_.group_count_);
  uint64_t volatile_pool_mb_per_socket
    = assorted::int_div_ceil(volatile_pool_bytes_per_socket, 1ULL << 20);

  // Let's put 100MB per thread. They will grab something initially.
  volatile_pool_mb_per_socket += options.thread_.thread_count_per_group_ * 100;

  std::cout << "volatile_pool_size="
    << volatile_pool_mb_per_socket << "MB per NUMA node" << std::endl;
  options.memory_.page_pool_size_mb_per_node_ = volatile_pool_mb_per_socket;

  SsspDriver::Result result;
  {
    Engine engine(options);
    for (const proc::ProcAndName& proc : procs) {
      engine.get_proc_manager()->pre_register(proc);
    }
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      SsspDriver driver(&engine);
      result = driver.run();
      COERCE_ERROR(engine.uninitialize());
    }
  }

  // wait just for a bit to avoid mixing stdout
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  for (uint32_t i = 0; i < result.navigation_worker_count_; ++i) {
    LOG(INFO) << result.navigation_workers_[i];
  }
  LOG(INFO) << "final result:" << result;
  if (FLAGS_profile) {
    std::cout << "Check out the profile result: pprof --pdf sssp sssp.prof > prof.pdf; "
      "okular prof.pdf" << std::endl;
  }

  return 0;
}

std::ostream& operator<<(std::ostream& o, const SsspDriver::Result& v) {
  o << "<total_result>"
    << "<duration_sec_>" << v.duration_sec_ << "</duration_sec_>"
    << v.analysis_result_
    << "<navigation_worker_count_>" << v.navigation_worker_count_ << "</navigation_worker_count_>"
    << "<NaviMTPS>" << ((v.navigation_total_ / v.duration_sec_) / 1000000) << "</NaviMTPS>"
    << "<navi_total>" << v.navigation_total_ << "</navi_total>"
    << "</total_result>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const SsspDriver::NavigationWorkerResult& v) {
  o << "  <worker_><id>" << v.id_ << "</id>"
    << "<txn>" << v.processed_ << "</txn>"
    << "</worker>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const SsspDriver::AnalysisResult& v) {
  o << "<analytic><processed>" << v.processed_ << "</processed>"
    << "<total_microseconds>" << v.total_microseconds_ << "</total_microseconds>"
    << "<ave_us>" << (static_cast<double>(v.total_microseconds_) / v.processed_)
    << "</ave_us>"
    << "<total_aborts>";
  for (uint32_t j = 0; j < SsspDriver::kAnalyticAbortTypes; ++j) {
    o << "<type_" << j << ">" << v.total_aborts_[j] << "</type_" << j << ">";
  }
  o << "</total_aborts>"
    << "</analytic>";
  return o;
}


}  // namespace sssp
}  // namespace foedus

int main(int argc, char **argv) {
  return foedus::sssp::driver_main(argc, argv);
}
