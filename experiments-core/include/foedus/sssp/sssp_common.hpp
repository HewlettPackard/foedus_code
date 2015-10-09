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
#ifndef FOEDUS_EXPERIMENTS_SSSP_COMMON_HPP_
#define FOEDUS_EXPERIMENTS_SSSP_COMMON_HPP_

#include <stdint.h>

namespace foedus {
namespace sssp {

/**
 * Max number of edges from each node.
 * Unlike other params, this is hardcoded to make the sizeof() trivial below.
 */
const uint64_t kMaxEdges = 20;

/** x-y size of one block. */
const uint64_t kBlockSize = 1U << 10;
/** how many nodes we generate per block */
const uint64_t kNodesPerBlock = 1U << 5;

/** how many blocks per block in both x and y directions. */
// const uint32_t kPartitionSize = 1U << 10;
const uint64_t kPartitionSize = 1U << 1;  // for trivial test data

/** how many blocks per block in total. */
const uint64_t kBlocksPerPartition = kPartitionSize * kPartitionSize;
/** how many nodes per partition. */
const uint64_t kNodesPerPartition = kBlocksPerPartition * kNodesPerBlock;

typedef uint32_t NodeId;
typedef uint32_t Mileage;

struct Coordinate {
  uint32_t  x_;
  uint32_t  y_;
  bool operator==(const Coordinate& other) const {
    return x_ == other.x_ && y_ == other.y_;
  }
  bool operator<(const Coordinate& other) const {
    if (x_ != other.x_) {
      return x_ < other.x_;
    } else {
      return y_ < other.y_;
    }
  }
  template <typename DIST>
  DIST calc_distance_sq(const Coordinate& to) const {
    DIST sq =
      static_cast<DIST>(x_ - to.x_) * (x_ - to.x_)
      + static_cast<DIST>(y_ - to.y_) * (y_ - to.y_);
    return sq;
  }
};

struct Edge {
  NodeId  to_;
  Mileage mileage_;
  bool operator<(const Edge& other) const {
    return to_ < other.to_;
  }
};

struct Node {
  Coordinate coordinate_;
  NodeId    id_;
  uint32_t  edge_count_;
  /** Edges from this node. */
  Edge      edges_[kMaxEdges];
};

struct EdgeCandidate {
  EdgeCandidate() {}
  EdgeCandidate(const Node& from, const Node& to) {
    to_ = to.id_;
    // This is a neighbor, so uint32_t should't cause an overflow
    distance_sq_ = from.coordinate_.calc_distance_sq<Mileage>(to.coordinate_);
  }

  NodeId  to_;
  uint32_t distance_sq_;

  bool operator<(const EdgeCandidate& other) const {
    if (distance_sq_ != other.distance_sq_) {
      return distance_sq_ < other.distance_sq_;
    } else {
      return to_ < other.to_;
    }
  }
};

struct Block {
  void init(uint32_t bx, uint32_t by) {
    bx_ = bx;
    by_ = by;
    x_ = bx * kBlockSize;
    y_ = by * kBlockSize;
  }

  /** x_ / kBlockSize */
  uint32_t bx_;
  /** y_ / kBlockSize */
  uint32_t by_;
  uint32_t x_;
  uint32_t y_;
  Node nodes_[kNodesPerBlock];
};

struct Partition {
  void init(uint32_t px, uint32_t py, uint32_t pid) {
    px_ = px;
    py_ = py;
    pid_ = pid;
    x_ = px * kBlockSize * kPartitionSize;
    y_ = py * kBlockSize * kPartitionSize;
  }

  /** x_ / (kBlockSize * kPartitionSize) */
  uint32_t px_;
  /** y_ / (kBlockSize * kPartitionSize) */
  uint32_t py_;
  /** Partition ID. px_ */
  uint32_t pid_;
  uint32_t x_;
  uint32_t y_;
  Block blocks_[kBlocksPerPartition];
};

const uint64_t kPartitionAlignedByteSize = ((sizeof(Partition) >> 21) + 1ULL) << 21;

}  // namespace sssp
}  // namespace foedus

#endif  // FOEDUS_EXPERIMENTS_SSSP_COMMON_HPP_
