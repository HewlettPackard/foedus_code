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
#ifndef FOEDUS_SSSP_SSSP_LOAD_SCHEMA_HPP_
#define FOEDUS_SSSP_SSSP_LOAD_SCHEMA_HPP_

#include <stdint.h>

#include "foedus/fwd.hpp"
#include "foedus/sssp/sssp_common.hpp"
#include "foedus/storage/array/array_storage.hpp"

/**
 * @file sssp_schema.hpp
 * @brief Definition of SSSP schema.
 * @details
 * We only use two storages; \e vertex_data and \e vertex_bf.
 */
namespace foedus {
namespace sssp {

/** Packages all storages in SSSP */
struct SsspStorages {
  SsspStorages();

  void assert_initialized();
  void initialize_tables(Engine* engine);

  /**
   * vertex_data is an array storage that simply stores foedus::Sssp::Node.
   * Index is node-id.
   * A navigational query (pair-wise shortest path) only uses this storage.
   */
  storage::array::ArrayStorage  vertex_data_;
  /**
   * vertex_bf is another array storage with the same index.
   * It stores VertexBfData below, which is the current progress of analysis query.
   */
  storage::array::ArrayStorage  vertex_bf_;
};

const uint32_t kNullDistance = 0;
const uint64_t kEmptyVertexBfData = 0;

/**
 * Represents the current state of a given node for the parallelized Bellman-Ford.
 */
struct VertexBfData {
  VertexBfData() : distance_(kNullDistance), pred_node_(0) {
  }
  /**
   * The currently-known minimal distance from the source node.
   * 0 (kNullDistance) means we haven't reached this node yet.
   */
  uint32_t distance_;
  /**
   * The predecessor in the path that gives the distance_.
   * When distance_ is 0, this is undefined.
   */
  NodeId   pred_node_;
};

}  // namespace sssp
}  // namespace foedus

static_assert(sizeof(foedus::sssp::VertexBfData) == sizeof(uint64_t), "VertexBfData size");

#endif  // FOEDUS_SSSP_SSSP_LOAD_SCHEMA_HPP_
