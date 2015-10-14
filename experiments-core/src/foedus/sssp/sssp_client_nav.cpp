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
#include "foedus/sssp/sssp_client.hpp"

#include <glog/logging.h>

#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/sssp/sssp_hashtable.hpp"
#include "foedus/sssp/sssp_minheap.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace sssp {
/**
 * Maximum number of relaxation for Dijkstra.
 * The navigational queries pick relatively closer node pairs.
 * Thus, this should be sufficient.
 */
const uint32_t kNavMaxNeighbors = 1U << 15;

ErrorStack SsspClientTask::run_impl_navigational() {
  outputs_->init();
  DijkstraHashtable hashtable;
  WRAP_ERROR_CODE(hashtable.create_memory(context_->get_numa_node()));

  // This minheap is a lazy-version of minheap where we don't decrease priority
  // of existing records. As a result, we might need many more entries.
  DijkstraMinheap minheap;
  minheap.reserve(kNavMaxNeighbors * 32);  // * 32 for that

  channel_->start_rendezvous_.wait();
  LOG(INFO) << "SSSP Client-" << get_worker_id() << " started processing navigational queries "
    << " from_p=" << inputs_.nav_partition_from_
    << " to_p=" << inputs_.nav_partition_to_;

  const uint32_t assigned_partition_count = inputs_.nav_partition_to_ - inputs_.nav_partition_from_;
  while (!is_stop_requested()) {
    // Let's determine the source node first
    uint32_t p = inputs_.nav_partition_from_ + (rnd_.next_uint32() % assigned_partition_count);
    uint32_t in_partition_node = rnd_.next_uint32() % kNodesPerPartition;
    NodeId source_id = in_partition_node + p * kNodesPerPartition;

    // Then, determine the destination node. So far we blindly add 64 (2 blocks away).
    // If this results in more than max_node_id_, we must retry.
    NodeId dest_id = source_id + 64;
    if (UNLIKELY(dest_id >= inputs_.max_node_id_)) {
      continue;
    }

    // Also, this occasionally picks a too-far node for a navigational query.
    // It happens when adding 64 changes its "by". Let's retry in that case too.
    if ((in_partition_node % kPartitionSize) >= kPartitionSize - 64) {
      continue;
    }

    WRAP_ERROR_CODE(do_navigation(source_id, dest_id, &hashtable, &minheap));
    ++outputs_->navigational_processed_;
  }

  hashtable.release_memory();
  return kRetOk;
}

ErrorCode SsspClientTask::do_navigation(
  NodeId source_id,
  NodeId dest_id,
  DijkstraHashtable* hashtable,
  DijkstraMinheap* minheap) {
  hashtable->clean();  // reuse the hashtable everytime. just clean it
  minheap->clean();  // same above
  xct::XctManager* xct_manager = context_->get_engine()->get_xct_manager();

  CHECK_ERROR_CODE(xct_manager->begin_xct(context_, xct::kSerializable));

  // Good old Dijkstra
  NodeId from_id = source_id;
  Node from_node;
  uint32_t from_distance = 0;
  uint32_t visited_nodes = 0;
  while (true) {
    CHECK_ERROR_CODE(storages_.vertex_data_.get_record(context_, from_id, &from_node));
    ASSERT_ND(from_node.id_ == from_id);

    // Relax from the from_id node
    for (uint32_t e = 0; e < from_node.edge_count_; ++e) {
      const Edge* edge = from_node.edges_ + e;
      DijkstraHashtable::Record* record = hashtable->get_or_create(edge->to_);
      ASSERT_ND(hashtable->get(edge->to_));
      if (record->value_.distance_ == 0
        || record->value_.distance_ > from_distance + edge->mileage_) {
        record->value_.distance_ = from_distance + edge->mileage_;
        record->value_.previous_ = from_id;
        minheap->push(DijkstraEntry(record->value_.distance_, edge->to_));
        DVLOG(3) << "push:" << edge->to_ << " dist=" << record->value_.distance_;
      }
    }

    // Which node will be the next top?
    while (true) {  // because we might have lazy-delete retry
      DijkstraEntry top = minheap->pop();
      DVLOG(2) << "pop:" << top.node_id_ << " dist=" << top.distance_;
      // Check whether this is an active record
      const DijkstraHashtable::Record* record = hashtable->get(top.node_id_);
      ASSERT_ND(top.distance_ >= record->value_.distance_);
      ASSERT_ND(record->value_.distance_ != 0);
      if (top.distance_ > record->value_.distance_) {
        continue;  // lazy-delete
      }

      from_id = top.node_id_;
      from_distance = top.distance_;
      break;
    }

    ASSERT_ND(from_distance > 0);
    if (from_id == dest_id) {
      // We got the answer!
      break;
    }

    ++visited_nodes;
    if (visited_nodes >= kNavMaxNeighbors) {
      LOG(ERROR) << "This query had to visit more than " << kNavMaxNeighbors
        << " nodes. Is this really a navigational query?";
      break;
    }
  }

  // In this experiment, no race abort is expected because there is no concurrent write.
  // In general, we should check the result and retry if aborted.
  Epoch commit_epoch;
  CHECK_ERROR_CODE(xct_manager->precommit_xct(context_, &commit_epoch));

  return kErrorCodeOk;
}

}  // namespace sssp
}  // namespace foedus
