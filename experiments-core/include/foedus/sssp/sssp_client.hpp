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
#include "foedus/sssp/sssp_client_chime.hpp"
#include "foedus/sssp/sssp_common.hpp"
#include "foedus/sssp/sssp_hashtable.hpp"
#include "foedus/sssp/sssp_scheduler.hpp"
#include "foedus/sssp/sssp_schema.hpp"
#include "foedus/storage/array/array_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace sssp {

class DijkstraMinheap;

/**
 * Channel between the driver process/thread and clients process/thread.
 * If the driver spawns client processes, this is allocated in shared memory.
 */
struct SsspClientChannel {
  enum AnalyticQueryState {
    kAnalyticInvalid = 0,
    /**
     * Each worker is now resetting the state for next query.
     * Once all workers are ready, moves on to kAnalyticStarted.
     */
    kAnalyticPreparing,
    /**
     * Each worker is now processing one query.
     */
    kAnalyticStarted,
    /**
     * All workers converged their assigned nodes.
     */
    kAnalyticCompleted,
    kAnalyticStopping,
  };

  enum Constants {
    kMaxAnalyticWorkers = 512,
  };

  void initialize() {
    start_rendezvous_.initialize();
    exit_nodes_.store(0);
    stop_flag_.store(false);

    analytic_state_.store(kAnalyticInvalid);
    analytic_thread_ids_setup_.store(0);
    std::memset(analytic_thread_ids_, 0, sizeof(analytic_thread_ids_));
  }
  void uninitialize() {
    start_rendezvous_.uninitialize();
  }
  /** This is fired when warmup_complete_counter_ becomes the total worker count. */
  soc::SharedRendezvous start_rendezvous_;
  std::atomic<uint16_t> exit_nodes_;
  std::atomic<bool> stop_flag_;

  char                padding_[256];

  std::atomic<uint32_t> analytic_state_;

  char                padding2_[256];
  /**
   * Used during kAnalyticPreparing.
   * Number of workers that become ready.
   * When this becomes the total number of analytic workers, we move on to kAnalyticStarted.
   */
  std::atomic<uint32_t> analytic_prepared_clients_;


  char                padding3_[256];

  /**
   * At startup, each analytic worker must to find other "buddy" workers
   * because they have to communicate each other.
   * Index is buddy_index, the value is the ThreadId of the analytic worker
   * of the buddy_index.
   */
  thread::ThreadId    analytic_thread_ids_[kMaxAnalyticWorkers];

  /**
   * How many analytic workers have setup their analytic_thread_ids_ entries so far.
   * Once this value becomes the total number of analytic workers, we start processing.
   */
  std::atomic<uint32_t> analytic_thread_ids_setup_;

  char                padding4_[256];

  /**
   * An increasing counter incremented by the chime.
   * This is occasionally read by analytic workers to declare
   * since when they are waiting for a task.
   */
  std::atomic<AnalyticEpoch> analytic_epoch_;

  char                padding5_[256 - 4];

  /**
   * Set to true when no analytic workers have a task.
   */
  std::atomic<bool>   analytic_query_ended_;

  // so far hard-coded to be node-0
  // NodeId              analytic_source_id_;
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
    kMaxBuddies = 512,
    kAnalyticAbortTypes = 4,
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

    /** number of sockets */
    uint16_t sockets_count_;

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

    uint16_t analytic_workers_per_socket_;
    uint16_t navigational_workers_per_socket_;

    uint16_t my_socket_;
    uint16_t analytic_my_worker_index_per_socket_;

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

    /**
     * The number of blocks one \e stripe covers in x-direction.
     * A stripe is a rectangular collection of blocks in which each analytic
     * worker is responsible for exactly one block.
     *  \li For x-axis, a stripe has analytic_workers_per_socket_ blocks.
     *  \li For y-axis, a stripe has #-of-socket blocks.
     *
     * The block assignment in stripes is self-explanatory.
     *
     * The choice of x/y could be arbitrary and could be separate from
     * analytic_workers_per_socket_ and sockets_count_, but this is easier to remember.
     */
    uint32_t analytic_stripe_x_size_;
    /** As said above, same as sockets_count_ */
    uint32_t analytic_stripe_y_size_;

    /**
     * How many stripes  _might_ exist for x-direction.
     * ceil(max_px_ * kPartitionSize / analytic_stripe_x_size_).
     * You might think every stripe we give a number must have x below this,
     * \b but it might be larger than that (but, those stripes are always dummies). See below.
     */
    uint32_t analytic_stripe_x_count_;
    /**
     * How many stripes _might_ exist for y-direction.
     * ceil(max_py_ * kPartitionSize / #-of-socket).
     * You might think every stripe we give a number must have y below this,
     * \b but it might be larger than that (but, those stripes are always dummies). See below.
     */
    uint32_t analytic_stripe_y_count_;

    /**
     * @brief analytic_stripe_max_axis_ = analytic_stripe_x_count_ + analytic_stripe_y_count_ - 1.
     * @details
     * The numbering among stripes is a bit tricky. It looks like as follows:
     * 0 1 3 6 ...
     * 2 4 7 ...
     * 5 8 ...
     * 9 ...
     *
     * The numbering is based on the distance from source-node (in this query, 0).
     * This simplifies the scheduling to finalize nodes closer to the source first.
     *
     * @par Simplified stripe numbering
     * To further simplify, we do one lazy thing that slightly overuses memory.
     * We don't want to worry about:
     *  A. stripe's x-axis is much smaller than y-axis:
     * 0 1
     * 2 3
     * 4 5
     * 6 ...
     *  B. stripe's y-axis is much smaller than x-axis (fig. omitted)
     *  C. The given stripe is in right-bottom half of the matrix:
     * 0 1 3
     * 2 4 6
     * 5 7 8
     * When stripe=0~5, calculating x/y from stripe is trivial.
     * When strip=6~8, doing so needs to think about x-axis/y-axis length.
     *
     * To forget about them, we also give numbers to stripes that are never actually used.
     * Observe that all \e real stripes are on a diagonal line of a square whose size is up to
     * y = 0, x = analytic_stripe_x_count_ + analytic_stripe_y_count_ - 1
     * (in above example x = 3 + 3 - 1 = 5. )
     * We thus imaginally consider diagonal lines whose sizes are \b always of that size.
     * resulting in:
     *  [imaginary]   [ real ]   [imaginary]
     *              |  0  5 10 | 15 20
     *           1  |  6 11 16 | 21
     *        2  7  | 12 17 22 |
     *              ------------
     *     3  8 13  | 18 23
     *  4  9 14 19  | 24
     *
     * Here, cells on the left plane separated by "|" (eg 1, 2, 7) are imaginary stripes.
     * Also, cells on the right plane separated by another "|" (eg 15, 21) are also imaginary.
     *
     * This is our stripe numbering scheme. All existing stripes are placed between the two "|"s.
     * Thus, given a stripe index "z" of an existing stripe,
     *  y = z%5
     *  x = z/5 - y
     *
     * On the other hand, when given x and y, z is
     *  z = (x+y)*5 + y
     *
     * Just replace "5" with analytic_stripe_max_axis_ in general. Easy, isn't.
     *
     * One drawback: there are many stripes that never actually exist: eg 1, 7, 15, ...
     * This only affects the size of L2 version counter arrays below.
     * In all other places, considering them won't hurt anything.
     */
    uint32_t analytic_stripe_max_axis_;
    /** analytic_stripe_max_axis_^2. an auxiliary var for handiness */
    uint32_t analytic_total_stripe_count_;
    /**
     * Number of stripes one L1 version covers.
     */
    uint32_t analytic_stripes_per_l1_;
  };

  /**
   * Outputs of the tasks. Because this is allocated in shared memory,
   * we also use this object as communication channel between workers.
   */
  struct Outputs {
    /** How many navigational queries processed so far */
    uint64_t navigational_processed_;
    /** [Only analytic-leader] How many analytic queries processed so far. */
    uint64_t analytic_processed_;
    /** [Only analytic-leader] Total microseconds to process the analytic queries */
    uint64_t analytic_total_microseconds_;
    uint64_t analytic_buddy_index_;  // just for sanity check.

    char  padding_[256 - 32];

    /**
     * See sssp_client_chime.hpp.
     */
    std::atomic< AnalyticEpoch > analytic_clean_since_;
    char  padding1_[256 - 4];

    /**
     * See sssp_client_chime.hpp.
     */
    std::atomic< AnalyticEpoch > analytic_clean_upto_;
    char  padding2_[256 - 4];

    /**
     * Abort-retries in this worker.
     * Nav queries have no aborts.
     */
    uint32_t analytic_aborts_[kAnalyticAbortTypes];
    char  padding3_[256 - sizeof(analytic_aborts_)];


    /**
     * Layer-1 version counters.
     * This has a fixed number of elements.
     */
    VersionCounter  analytic_l1_versions_[kL1VersionFactors];

    /**
     * Layer-2 version counters.
     * This is actually of a dynamic size. so, "sizeof(Outputs)" underestimates the size!
     * Same as the number of blocks this worker is responsible for, or stripe_count.
     */
    VersionCounter  analytic_l2_versions_[64];

    void init() {
      navigational_processed_ = 0;
      analytic_processed_ = 0;
      analytic_total_microseconds_ = 0;
      analytic_buddy_index_ = 0;
      std::memset(analytic_aborts_, 0, sizeof(analytic_aborts_));
    }

    /** Additional initialization for \b each analytic query */
    void init_analytic_query(uint32_t stripe_count);

    /**
     * This is called by arbitrary workers.
     * Atomically increments L2 and then L1 cuonter.
     */
    void increment_l2_then_l1(uint32_t stripe, uint32_t stripes_per_l1);
  };

  SsspClientTask(const Inputs& inputs, Outputs* outputs)
    : inputs_(inputs),
      outputs_(outputs),
      rnd_(kRandomSeed + inputs.worker_id_) {
    outputs_->init();
  }
  ~SsspClientTask() {}

  ErrorStack run(thread::Thread* context);

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
  xct::XctManager*  xct_manager_;
  Outputs* const    outputs_;


  /** thread local random. */
  assorted::UniformRandom rnd_;

  /** Used in both queries */
  DijkstraHashtable hashtable_;

  /**
   * Started only by analytic leader. otherwise unused.
   */
  SsspAnalyticChime analytic_chime_;

  storage::array::ArrayOffset analytic_tmp_node_ids_[kNodesPerBlock];
  Node                  analytic_tmp_nodes_[kNodesPerBlock];
  const void*           analytic_tmp_nodes_addresses_[kNodesPerBlock];
  VertexBfData          analytic_tmp_bf_records_[kNodesPerBlock];

  /**
   * Points to all analytic workers' Outputs object on shared memory.
   * Index is buddy_index.
   */
  Outputs*          analytic_other_outputs_[kMaxBuddies];

  ErrorStack run_impl_navigational();
  ErrorStack run_impl_analytic();

  /** Process one SSSP navitational query. */
  ErrorCode do_navigation(
    NodeId source_id,
    NodeId dest_id,
    DijkstraMinheap* minheap);

  /// Sub-routines of run_impl_analytic()
  ErrorStack do_analytic();
  // this is only done by analytic leader
  ErrorStack analytic_initial_relax();
  ErrorCode analytic_relax_block_retrieve_topology();
  ErrorStack analytic_relax_block(uint32_t stripe);
  ErrorCode analytic_apply_own_block(uint32_t own_block);
  ErrorCode analytic_apply_foreign_blocks(uint32_t own_block);
  void      analytic_relax_calculate(NodeId node_id_offset);
  void      analytic_relax_calculate_recurse(uint32_t n, NodeId node_id_offset);
  ErrorStack analytic_write_result();

  /**
   * Gives x-y position as a stripe. See "Simplified stripe numbering" above.
   */
  inline void to_stripe_xy_from_stripe(
    uint32_t stripe,
    uint32_t* stripe_x,
    uint32_t* stripe_y) const {
    ASSERT_ND(stripe < inputs_.analytic_total_stripe_count_);
    *stripe_y = stripe % inputs_.analytic_stripe_max_axis_;
    uint32_t diagonal = stripe / inputs_.analytic_stripe_max_axis_;
    ASSERT_ND(diagonal >= (*stripe_y));
    *stripe_x = diagonal - (*stripe_y);
  }

  inline uint32_t to_my_block_from_stripe(uint32_t stripe) const {
    uint32_t stripe_x;
    uint32_t stripe_y;
    to_stripe_xy_from_stripe(stripe, &stripe_x, &stripe_y);
    const uint32_t bx
      = stripe_x * inputs_.analytic_stripe_x_size_ + inputs_.analytic_my_worker_index_per_socket_;
    const uint32_t by
      = stripe_y * inputs_.analytic_stripe_y_size_ + inputs_.my_socket_;

    const uint32_t px = bx / kPartitionSize;
    const uint32_t in_partition_bx = bx % kPartitionSize;
    ASSERT_ND(px < inputs_.max_px_);
    const uint32_t py = by / kPartitionSize;
    const uint32_t in_partition_by = by % kPartitionSize;
    ASSERT_ND(py < inputs_.max_py_);

    const uint64_t pid = px + py * inputs_.max_px_;
#ifndef NDEBUG
    uint32_t px_again, py_again;
    to_px_py(pid, inputs_.max_px_, &px_again, &py_again);
    ASSERT_ND(px_again == px);
    ASSERT_ND(py_again == py);
#endif  // NDEBUG

    const uint32_t in_partition_block = in_partition_bx + in_partition_by * kPartitionSize;
    const uint32_t block = pid * kBlocksPerPartition + in_partition_block;
    ASSERT_ND(block < kBlocksPerPartition * inputs_.max_px_ * inputs_.max_py_);
    return block;
  }
  inline void to_stripe_and_owner_from_block(
    uint32_t block,
    uint32_t* stripe,
    uint32_t* owner_buddy_index) const {
    const uint32_t pid = block / kBlocksPerPartition;
    uint32_t px, py;
    to_px_py(pid, inputs_.max_px_, &px, &py);

    const uint32_t in_partition_block = block % kBlocksPerPartition;
    const uint32_t in_partition_bx = in_partition_block % kPartitionSize;
    const uint32_t in_partition_by = in_partition_block / kPartitionSize;
    const uint32_t bx = px * kPartitionSize + in_partition_bx;
    const uint32_t by = py * kPartitionSize + in_partition_by;

    const uint32_t stripe_x = bx / inputs_.analytic_stripe_x_size_;
    const uint32_t in_stripe_bx = bx % inputs_.analytic_stripe_x_size_;
    ASSERT_ND(stripe_x < inputs_.analytic_stripe_x_count_);
    const uint32_t stripe_y = by / inputs_.analytic_stripe_y_size_;
    const uint32_t in_stripe_by = by % inputs_.analytic_stripe_y_size_;
    ASSERT_ND(stripe_y < inputs_.analytic_stripe_y_count_);

    *stripe = (stripe_x + stripe_y) * inputs_.analytic_stripe_max_axis_ + stripe_y;
    ASSERT_ND(*stripe < inputs_.analytic_total_stripe_count_);

    const uint32_t analytic_worker_index_per_socket = in_stripe_bx;
    const uint32_t socket = in_stripe_by;
    *owner_buddy_index
      = analytic_worker_index_per_socket + socket * inputs_.analytic_workers_per_socket_;
  }
};
}  // namespace sssp
}  // namespace foedus

#endif  // FOEDUS_SSSP_SSSP_CLIENT_HPP_
