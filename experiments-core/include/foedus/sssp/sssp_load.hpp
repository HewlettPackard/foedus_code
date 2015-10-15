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
#ifndef FOEDUS_SSSP_SSSP_LOAD_HPP_
#define FOEDUS_SSSP_SSSP_LOAD_HPP_

#include <stdint.h>

#include <string>

#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/sssp/sssp_common.hpp"
#include "foedus/sssp/sssp_schema.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace sssp {
ErrorStack create_all(Engine* engine, uint32_t total_partitions);

/**
 * @brief Main class of data load for SSSP.
 * @details
 * We just use the binary version of the data files generated from datagen.
 * Because we are using the same header, we can simply reinterpret_cast.
 */
class SsspLoadTask {
 public:
  struct Inputs {
    /** Maximum number of partitions in x axis. duh, this is not actually max, but count. */
    uint32_t max_px_;
    /** Maximum number of partitions in y axis. duh, this is not actually max, but count. */
    uint32_t max_py_;
    /** Maximal node-ID possible */
    NodeId max_node_id_;

    /**
      * Inclusive beginning of partition ID assigned for this loader.
      * Due to my laziness, we don't parallelize more granular than one partition.
      */
    uint32_t partition_from_;
    /**
      * Exclusive end of partition ID assigned for this loader.
      */
    uint32_t partition_to_;
  };
  explicit SsspLoadTask(const Inputs& inputs) : inputs_(inputs) {}
  ErrorStack          run(thread::Thread* context);

  ErrorStack          load_tables();

 private:
  enum Constants {
    kCommitBatch = 500,
  };

  const Inputs inputs_;
  SsspStorages storages_;

  Engine* engine_;
  thread::Thread* context_;
  xct::XctManager* xct_manager_;

  ErrorCode  commit_if_full();

  /** Loads the vertex_data storage. for the given partition. */
  ErrorStack load_vertex_data(uint32_t p, Partition* buffer);

  /** Loads the vertex_bf storage with empty data. */
  ErrorStack load_vertex_bf();
};

/**
 * Load data into SSSP tables.
 * Input is SsspLoadTask::Inputs, not output.
 */
ErrorStack sssp_load_task(const proc::ProcArguments& args);

}  // namespace sssp
}  // namespace foedus


#endif  // FOEDUS_SSSP_SSSP_LOAD_HPP_
