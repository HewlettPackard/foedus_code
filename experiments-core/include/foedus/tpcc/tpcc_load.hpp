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
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/impersonate_task.hpp"
#include "foedus/tpcc/tpcc_scale.hpp"
#include "foedus/tpcc/tpcc_schema.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace tpcc {
/**
 * @brief Just creates empty tables.
 */
class TpccCreateTask : public thread::ImpersonateTask {
 public:
  explicit TpccCreateTask(Wid total_warehouses) : total_warehouses_(total_warehouses) {}
  ErrorStack          run(thread::Thread* context);

  const TpccStorages& get_storages() const { return storages_; }

 private:
  const Wid     total_warehouses_;
  TpccStorages  storages_;

  ErrorStack create_array(
    thread::Thread* context,
    const storage::StorageName& name,
    uint32_t payload_size,
    uint64_t array_size,
    storage::array::ArrayStorage** storage);
  ErrorStack create_masstree(
    thread::Thread* context,
    const storage::StorageName& name,
    float border_fill_factor,
    storage::masstree::MasstreeStorage** storage);
  ErrorStack create_sequential(
    thread::Thread* context,
    const storage::StorageName& name,
    storage::sequential::SequentialStorage** storage);
};
/**
 * @brief Verify tables after data loading.
 */
class TpccFinishupTask : public thread::ImpersonateTask {
 public:
  explicit TpccFinishupTask(Wid total_warehouses, const TpccStorages& storages)
    : total_warehouses_(total_warehouses), storages_(storages) {}
  ErrorStack          run(thread::Thread* context);

 private:
  const Wid total_warehouses_;
  const TpccStorages storages_;
};
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
  TpccLoadTask(
    Wid total_warehouses,
    const TpccStorages& storages,
    const char* timestamp,
    Wid from_wid,
    Wid to_wid,
    Iid from_iid,
    Iid to_iid)
    : total_warehouses_(total_warehouses),
      storages_(storages),
      timestamp_(timestamp),
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
  const TpccStorages storages_;
  /** timestamp for date fields. */
  const char* timestamp_;
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
}  // namespace tpcc
}  // namespace foedus


#endif  // FOEDUS_TPCC_TPCC_LOAD_HPP_
