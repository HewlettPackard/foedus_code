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
#include "foedus/proc/proc_id.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/tpcc/tpcc_scale.hpp"
#include "foedus/tpcc/tpcc_schema.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace tpcc {
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
  float border_fill_factor);
ErrorStack create_sequential(Engine* engine, const storage::StorageName& name);

struct TpccFinishupInput {
  Wid   total_warehouses_;
  bool  skip_verify_;
};

class TpccFinishupTask {
 public:
  explicit TpccFinishupTask(const TpccFinishupInput &input)
    : total_warehouses_(input.total_warehouses_), skip_verify_(input.skip_verify_) {}
  ErrorStack          run(thread::Thread* context);

 private:
  const Wid total_warehouses_;
  const bool skip_verify_;
  TpccStorages storages_;
};

/**
 * @brief Verify tables after data loading.
 * Only input is total_warehouses(Wid). No output.
 */
ErrorStack tpcc_finishup_task(const proc::ProcArguments& args);

/**
 * @brief Main class of data load for TPC-C.
 * @details
 * Acknowledgement:
 * Some of the following source came from TpccOverBkDB by A. Fedorova:
 *   http://www.cs.sfu.ca/~fedorova/Teaching/CMPT886/Spring2007/benchtools.html
 * Several things have been changed to adjust it for C++ and FOEDUS.
 */
class TpccLoadTask {
 public:
  struct Inputs {
    Wid total_warehouses_;
    bool olap_mode_;
    assorted::FixedString<28> timestamp_;
    Wid from_wid_;
    Wid to_wid_;
    Iid from_iid_;
    Iid to_iid_;
  };
  TpccLoadTask(
    Wid total_warehouses,
    bool olap_mode,
    const assorted::FixedString<28>& timestamp,
    Wid from_wid,
    Wid to_wid,
    Iid from_iid,
    Iid to_iid)
    : total_warehouses_(total_warehouses),
      olap_mode_(olap_mode),
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
  const bool olap_mode_;
  TpccStorages storages_;
  /** timestamp for date fields. */
  const assorted::FixedString<28> timestamp_;
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
 * Load data into TPCC tables.
 * Input is TpccLoadTask::Inputs, not output.
 */
ErrorStack tpcc_load_task(const proc::ProcArguments& args);

}  // namespace tpcc
}  // namespace foedus


#endif  // FOEDUS_TPCC_TPCC_LOAD_HPP_
