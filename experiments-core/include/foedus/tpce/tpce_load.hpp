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
#include "foedus/tpce/tpce_scale.hpp"
#include "foedus/tpce/tpce_schema.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace tpce {
/**
 * @brief Just creates empty tables.
 */
ErrorStack create_all(Engine* engine, Wid total_warehouses);

ErrorStack create_array(
  Engine* engine,
  const storage::StorageName& name,
  bool keep_all_volatile_pages,
  uint32_t payload_size,
  uint64_t array_size);
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
    Wid   total_warehouses_;
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
    Wid total_warehouses_;
    Wid from_wid_;
    Wid to_wid_;
    Iid from_iid_;
    Iid to_iid_;
  };
  TpceLoadTask(
    Wid total_warehouses,
    Wid from_wid,
    Wid to_wid,
    Iid from_iid,
    Iid to_iid)
    : total_warehouses_(total_warehouses),
      from_wid_(from_wid),
      to_wid_(to_wid),
      from_iid_(from_iid),
      to_iid_(to_iid) {}
  ErrorStack          run(thread::Thread* context);

  ErrorStack          load_tables();

 private:
  enum Constants {
    kCommitBatch = 500,
  };

  const Wid total_warehouses_;
  TpceStorages storages_;
  /** inclusive beginning of responsible wid */
  const Wid from_wid_;
  /** exclusive end of responsible wid */
  const Wid to_wid_;
  /** inclusive beginning of responsible iid. */
  const Iid from_iid_;
  /** exclusive end of responsible iid. */
  const Iid to_iid_;

  Engine* engine_;
  thread::Thread* context_;
  xct::XctManager* xct_manager_;

  assorted::UniformRandom rnd_;
  memory::AlignedMemory customer_secondary_keys_buffer_;

  void      random_orig(bool *orig);

  Cid       get_permutation(bool* cid_array);

  ErrorCode  commit_if_full();

  /** Loads the Item table. */
  ErrorStack load_items();

  /** Loads the Warehouse table. */
  ErrorStack load_warehouses();

  /** Loads the Customer Table */
  ErrorStack load_customers();

  /**
  * Loads Customer Table.
  * Also inserts corresponding history record.
  * @param[in] wid warehouse id
  * @param[in] did district id
  */
  ErrorStack load_customers_in_district(Wid wid, Did did);

  /** Loads the Orders and Order_Line Tables */
  ErrorStack load_orders();

  /**
  *  Loads the Orders table.
  *  Also loads the orderLine table on the fly.
  *  @param[in] w_id warehouse id
  *  @param[in] d_id district id
  */
  ErrorStack load_orders_in_district(Wid wid, Did did);

  /** Loads the Stock table. */
  ErrorStack load_stocks();

  /** Loads the District table. */
  ErrorStack load_districts();

  void       make_address(char *str1, char *str2, char *city, char *state, char *zip);

  /** Make a string of letter */
  int32_t    make_alpha_string(int32_t min, int32_t max, char *str);

  /** Make a string of letter */
  int32_t    make_number_string(int32_t min, int32_t max, char *str);
};

/**
 * Load data into TPCE tables.
 * Input is TpceLoadTask::Inputs, not output.
 */
ErrorStack tpce_load_task(const proc::ProcArguments& args);

}  // namespace tpce
}  // namespace foedus


#endif  // FOEDUS_TPCE_TPCE_LOAD_HPP_
