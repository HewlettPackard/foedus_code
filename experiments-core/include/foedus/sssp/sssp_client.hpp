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
#ifndef FOEDUS_SSSP_SSSP_CLIENT_HPP_
#define FOEDUS_SSSP_SSSP_CLIENT_HPP_

#include <stdint.h>
#include <time.h>

#include <atomic>
#include <cstring>
#include <set>
#include <string>
#include <vector>

#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/fixed_string.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/soc/shared_rendezvous.hpp"
#include "foedus/sssp/sssp_common.hpp"
#include "foedus/sssp/sssp_schema.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/rendezvous_impl.hpp"

namespace foedus {
namespace sssp {
class DijkstraHashtable;
class DijkstraMinheap;

/**
 * Channel between the driver process/thread and clients process/thread.
 * If the driver spawns client processes, this is allocated in shared memory.
 */
struct SsspClientChannel {
  void initialize() {
    start_rendezvous_.initialize();
    exit_nodes_.store(0);
    stop_flag_.store(false);
  }
  void uninitialize() {
    start_rendezvous_.uninitialize();
  }
  /** This is fired when warmup_complete_counter_ becomes the total worker count. */
  soc::SharedRendezvous start_rendezvous_;
  std::atomic<uint16_t> exit_nodes_;
  std::atomic<bool> stop_flag_;
};

/**
 * Invoke SsspClientTask, which defines Inputs/Outputs.
 */
ErrorStack sssp_client_task(const proc::ProcArguments& args);

/**
 * @brief The worker thread to run transactions in the experiment.
 * @details
 * Each client is assigned for either navigational queries or analytic queries.
 */
class SsspClientTask {
 public:
  enum Constants {
    kRandomSeed = 123456,
  };
  struct Inputs {
    /** unique ID of this worker from 0 to #workers-1. */
    uint32_t worker_id_;
    /**
     * A globally unique index of this worker among the same type (nav/anl) of workers.
     * Navigation workers get partitions assigned based on this index.
     * Analytic workers communicate with other workers using this index.
     */
    uint16_t buddy_index_;

    /**
      * Whether this client is assigned for navigational queries.
      * Analytic queries if false.
      */
    bool navigational_;
    /**
     * Whether this client is the only \e leader for analytic queries.
     * First analytic client in first socket is the only "leader" that
     * initiates analytic query execution. Most things are decentralized,
     * so its only job is to announce the begin/end of each query,
     * and remember the time taken.
     */
    bool analytic_leader_;
    /** Maximum number of partitions in x axis */
    uint32_t max_px_;
    /** Maximum number of partitions in y axis */
    uint32_t max_py_;
    /** Maximal node-ID possible */
    NodeId max_node_id_;

    /**
      * Only for navigational worker.
      * Inclusive beginning of partition ID assigned for this worker.
      */
    uint32_t nav_partition_from_;
    /**
      * Only for navigational worker.
      * Exclusive end of partition ID assigned for this worker.
      */
    uint32_t nav_partition_to_;
  };

  struct Outputs {
    /** How many navigational queries processed so far */
    uint64_t navigational_processed_;
    /** [Only analytic-leader] How many analytic queries processed so far. */
    uint64_t analytic_processed_;
    /** [Only analytic-leader] Total microseconds to process the analytic queries */
    uint64_t analytic_total_microseconds_;

    void init() {
      navigational_processed_ = 0;
      analytic_processed_ = 0;
      analytic_total_microseconds_ = 0;
    }
  };

  SsspClientTask(const Inputs& inputs, Outputs* outputs)
    : inputs_(inputs),
      outputs_(outputs),
      rnd_(kRandomSeed + inputs.worker_id_) {
    outputs_->init();
  }
  ~SsspClientTask() {}

  ErrorStack run(thread::Thread* context);
  ErrorStack run_impl_navigational();
  ErrorStack run_impl_analytic();

  uint32_t get_worker_id() const { return inputs_.worker_id_; }

  bool is_stop_requested() const {
    assorted::memory_fence_acquire();
    return channel_->stop_flag_.load();
  }

 private:
  const Inputs inputs_;

  SsspClientChannel* channel_;

  SsspStorages      storages_;

  /** set at the beginning of run() for convenience */
  thread::Thread*   context_;
  Engine*           engine_;
  Outputs* const    outputs_;


  /** thread local random. */
  assorted::UniformRandom rnd_;

  /** Process one SSSP navitational query. */
  ErrorCode do_navigation(
    NodeId source_id,
    NodeId dest_id,
    DijkstraHashtable* hashtable,
    DijkstraMinheap* minheap);

  /**
   * Main loop of all analytic workers.
   */
  ErrorCode loop_analytic();
  /**
   *
   */
  ErrorCode do_analytic_leader();
};
}  // namespace sssp
}  // namespace foedus

#endif  // FOEDUS_SSSP_SSSP_CLIENT_HPP_
