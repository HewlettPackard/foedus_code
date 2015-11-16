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
#ifndef FOEDUS_TPCE_TPCE_CLIENT_HPP_
#define FOEDUS_TPCE_TPCE_CLIENT_HPP_

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
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/tpce/tpce.hpp"
#include "foedus/tpce/tpce_schema.hpp"

namespace foedus {
namespace tpce {
/**
 * Channel between the driver process/thread and clients process/thread.
 * If the driver spawns client processes, this is allocated in shared memory.
 */
struct TpceClientChannel {
  void initialize() {
    start_rendezvous_.initialize();
    warmup_complete_counter_.store(0);
    exit_nodes_.store(0);
    stop_flag_.store(false);
    preload_snapshot_pages_.store(false);
  }
  void uninitialize() {
    start_rendezvous_.uninitialize();
  }
  /** This is fired when warmup_complete_counter_ becomes the total worker count. */
  soc::SharedRendezvous start_rendezvous_;
  std::atomic<uint32_t> warmup_complete_counter_;
  std::atomic<uint16_t> exit_nodes_;
  std::atomic<bool> stop_flag_;
  std::atomic<bool> preload_snapshot_pages_;
};

/**
 * Invoke TpceClientTask, which defines Inputs/Outputs.
 */
ErrorStack tpce_client_task(const proc::ProcArguments& args);

/**
 * @brief The worker thread to run transactions in the experiment.
 * @details
 * This is the canonical TPCE workload which use as the default experiment.
 * We also have various focused/modified workload to evaluate specific aspects.
 */
class TpceClientTask {
 public:
  enum Constants {
    kRandomSeed = 123456,
    kRandomCount = 1 << 16,
    /** on average only 3. surely won't be more than this number */
    kMaxCidsPerLname = 128,
  };
  struct Inputs {
    TpceScale   scale_;
    PartitionT  worker_id_;
  };
  struct Outputs {
    /** How many transactions processed so far*/
    uint64_t processed_;

    // statistics
    uint32_t user_requested_aborts_;
    uint32_t race_aborts_;
    /** this is usually up to 1 because we stop execution as soon as this happens */
    uint32_t unexpected_aborts_;
    uint32_t largereadset_aborts_;

    uint64_t snapshot_cache_hits_;
    uint64_t snapshot_cache_misses_;
  };
  TpceClientTask(const Inputs& inputs, Outputs* outputs)
    : scale_(inputs.scale_),
      worker_id_(inputs.worker_id_),
      outputs_(outputs),
      rnd_(kRandomSeed + inputs.worker_id_) {
    outputs_->processed_ = 0;
    outputs_->user_requested_aborts_ = 0;
    outputs_->race_aborts_ = 0;
    outputs_->unexpected_aborts_ = 0;
    outputs_->largereadset_aborts_ = 0;
  }
  ~TpceClientTask() {}

  ErrorStack run(thread::Thread* context);
  ErrorStack run_impl(thread::Thread* context);

  uint32_t get_worker_id() const { return worker_id_; }
  uint32_t get_user_requested_aborts() const { return outputs_->user_requested_aborts_; }
  uint32_t increment_user_requested_aborts() { return ++outputs_->user_requested_aborts_; }
  uint32_t get_race_aborts() const { return outputs_->race_aborts_; }
  uint32_t increment_race_aborts() { return ++outputs_->race_aborts_; }
  uint32_t get_unexpected_aborts() const { return outputs_->unexpected_aborts_; }
  uint32_t increment_unexpected_aborts() { return ++outputs_->unexpected_aborts_; }
  uint32_t get_largereadset_aborts() const { return outputs_->largereadset_aborts_; }
  uint32_t increment_largereadset_aborts() { return ++outputs_->largereadset_aborts_; }

  bool is_stop_requested() const {
    assorted::memory_fence_acquire();
    return channel_->stop_flag_.load();
  }

  uint64_t get_processed() const { return outputs_->processed_; }

 private:
  const TpceScale   scale_;
  /** unique ID of this worker from 0 to #workers-1. */
  const PartitionT  worker_id_;

  TpceClientChannel* channel_;

  TpceStorages      storages_;

  /** set at the beginning of run() for convenience */
  thread::Thread*   context_;
  Engine*           engine_;
  Outputs* const    outputs_;

  /** thread local random. */
  assorted::UniformRandom rnd_;

  /** Run the TPCE TradeOrder transaction. Implemented in tpce_trade_order.cpp. */
  ErrorCode do_trade_order();
  /** Run the TPCE TradeUpdate transaction. Implemented in tpce_trade_update.cpp. */
  ErrorCode do_trade_update();

  ErrorStack warmup(thread::Thread* context);
};
}  // namespace tpce
}  // namespace foedus

#endif  // FOEDUS_TPCE_TPCE_CLIENT_HPP_
