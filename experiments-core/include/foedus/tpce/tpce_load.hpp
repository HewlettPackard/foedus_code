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
#ifndef FOEDUS_TPCE_TPCE_LOAD_HPP_
#define FOEDUS_TPCE_TPCE_LOAD_HPP_

#include <stdint.h>

#include <string>

#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/tpce/tpce_schema.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace tpce {
/**
 * @brief Just creates empty tables.
 */
ErrorStack create_all(Engine* engine, const TpceScale& scale);

ErrorStack create_array(
  Engine* engine,
  const storage::StorageName& name,
  bool keep_all_volatile_pages,
  uint32_t payload_size,
  uint64_t array_size);
ErrorStack create_hash(
  Engine* engine,
  const storage::StorageName& name,
  bool keep_all_volatile_pages,
  uint64_t expected_records,
  double preferred_records_per_bin);
ErrorStack create_masstree(
  Engine* engine,
  const storage::StorageName& name,
  bool keep_all_volatile_pages,
  float border_fill_factor,
  storage::masstree::Layer min_layer_hint);
ErrorStack create_sequential(Engine* engine, const storage::StorageName& name);

class TpceFinishupTask {
 public:
  struct Inputs {
    TpceScale scale_;
    bool  skip_verify_;
    bool  fatify_masstree_;
  };
  explicit TpceFinishupTask(const Inputs &inputs) : inputs_(inputs) {}
  ErrorStack          run(thread::Thread* context);

 private:
  const Inputs inputs_;
  TpceStorages storages_;
};

/**
 * @brief Verify tables after data loading.
 * Only input is total_warehouses(Wid). No output.
 */
ErrorStack tpce_finishup_task(const proc::ProcArguments& args);

/**
 * @brief Main class of data load for TPC-C.
 * @details
 * Acknowledgement:
 * Some of the following source came from TpceOverBkDB by A. Fedorova:
 *   http://www.cs.sfu.ca/~fedorova/Teaching/CMPT886/Spring2007/benchtools.html
 * Several things have been changed to adjust it for C++ and FOEDUS.
 */
class TpceLoadTask {
 public:
  struct Inputs {
    TpceScale scale_;
    PartitionT partition_id_;
  };
  TpceLoadTask(
    const TpceScale& scale,
    PartitionT partition_id)
    : scale_(scale), partition_id_(partition_id) {}
  ErrorStack          run(thread::Thread* context);

  ErrorStack          load_tables();

 private:
  enum Constants {
    kCommitBatch = 500,
  };

  const TpceScale   scale_;
  const PartitionT  partition_id_;
  TpceStorages storages_;

  Engine* engine_;
  thread::Thread* context_;
  xct::XctManager* xct_manager_;

  assorted::UniformRandom rnd_;

  ErrorCode  commit_if_full();

  /** Loads the Trade table. */
  ErrorStack load_trades();

  /** Loads the TradeType table. */
  ErrorStack load_trade_types();
  ErrorStack load_trade_types_one_row(
    uint16_t index,
    const char* id,
    const char* name,
    bool is_sell,
    bool is_mrkt);
};

/**
 * Load data into TPCE tables.
 * Input is TpceLoadTask::Inputs, not output.
 */
ErrorStack tpce_load_task(const proc::ProcArguments& args);

}  // namespace tpce
}  // namespace foedus


#endif  // FOEDUS_TPCE_TPCE_LOAD_HPP_
