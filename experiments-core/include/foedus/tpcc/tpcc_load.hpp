/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_TPCC_TPCC_LOAD_HPP_
#define FOEDUS_TPCC_TPCC_LOAD_HPP_

#include <stdint.h>

#include <string>

#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/impersonate_task.hpp"
#include "foedus/tpcc/tpcc_scale.hpp"
#include "foedus/tpcc/tpcc_schema.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace tpcc {
/**
 * @brief Main class of data load for TPC-C.
 * @details
 * Acknowledgement:
 * Some of the following source came from TpccOverBkDB by A. Fedorova:
 *   http://www.cs.sfu.ca/~fedorova/Teaching/CMPT886/Spring2007/benchtools.html
 * Several things have been changed to adjust it for C++ and FOEDUS.
 */
class TpccLoadTask : public thread::ImpersonateTask {
 public:
  ErrorStack          run(thread::Thread* context);

  ErrorStack          load_tables();
  const TpccStorages& get_storages() const { return storages_; }

 private:
  enum Constants {
    kCommitBatch = 100,
  };

  Engine* engine_;
  thread::Thread* context_;
  xct::XctManager* xct_manager_;
  /** timestamp for date fields. */
  char* timestamp_;

  TpccStorages storages_;

  assorted::UniformRandom rnd_;

  void      random_orig(bool *orig);

  Cid       get_permutation(bool* cid_array);

  ErrorStack create_tables();
  ErrorStack create_array(
    const std::string& name,
    uint32_t payload_size,
    uint64_t array_size,
    storage::array::ArrayStorage** storage);
  ErrorStack create_masstree(
    const std::string& name,
    storage::masstree::MasstreeStorage** storage);
  ErrorStack create_sequential(
    const std::string& name,
    storage::sequential::SequentialStorage** storage);

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

  ErrorStack load_neworders();

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
}  // namespace tpcc
}  // namespace foedus


#endif  // FOEDUS_TPCC_TPCC_LOAD_HPP_
