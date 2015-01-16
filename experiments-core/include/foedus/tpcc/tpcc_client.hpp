/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_TPCC_TPCC_CLIENT_HPP_
#define FOEDUS_TPCC_TPCC_CLIENT_HPP_

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
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/tpcc/tpcc.hpp"
#include "foedus/tpcc/tpcc_scale.hpp"
#include "foedus/tpcc/tpcc_schema.hpp"

namespace foedus {
namespace tpcc {
/**
 * Channel between the driver process/thread and clients process/thread.
 * If the driver spawns client processes, this is allocated in shared memory.
 */
struct TpccClientChannel {
  void initialize() {
    start_rendezvous_.initialize();
    warmup_complete_counter_.store(0);
    exit_nodes_.store(0);
    stop_flag_.store(false);
    preload_snapshot_pages_.store(false);
  }
  void uninitialize() {
    start_rendezvous_.uninitialize();
  }
  /** This is fired when warmup_complete_counter_ becomes the total worker count. */
  soc::SharedRendezvous start_rendezvous_;
  std::atomic<uint32_t> warmup_complete_counter_;
  std::atomic<uint16_t> exit_nodes_;
  std::atomic<bool> stop_flag_;
  std::atomic<bool> preload_snapshot_pages_;
};

/**
 * Invoke TpccClientTask, which defines Inputs/Outputs.
 */
ErrorStack tpcc_client_task(const proc::ProcArguments& args);

/**
 * @brief The worker thread to run transactions in the experiment.
 * @details
 * This is the canonical TPCC workload which use as the default experiment.
 * We also have various focused/modified workload to evaluate specific aspects.
 */
class TpccClientTask {
 public:
  enum Constants {
    kRandomSeed = 123456,
    kRandomCount = 1 << 16,
    /** on average only 3. surely won't be more than this number */
    kMaxCidsPerLname = 128,
  };
  struct Inputs {
    uint32_t worker_id_;
    Wid total_warehouses_;
    Wid from_wid_;
    Wid to_wid_;
    uint16_t neworder_remote_percent_;
    uint16_t payment_remote_percent_;
    bool olap_mode_;
    bool dirty_read_mode_;
  };
  struct Outputs {
    /** How many transactions processed so far*/
    uint64_t processed_;

    // statistics
    uint32_t user_requested_aborts_;
    uint32_t race_aborts_;
    /** this is usually up to 1 because we stop execution as soon as this happens */
    uint32_t unexpected_aborts_;
    uint32_t largereadset_aborts_;

    uint64_t snapshot_cache_hits_;
    uint64_t snapshot_cache_misses_;
  };
  TpccClientTask(const Inputs& inputs, Outputs* outputs)
    : worker_id_(inputs.worker_id_),
      total_warehouses_(inputs.total_warehouses_),
      from_wid_(inputs.from_wid_),
      to_wid_(inputs.to_wid_),
      olap_mode_(inputs.olap_mode_),
      dirty_read_mode_(inputs.dirty_read_mode_),
      outputs_(outputs),
      neworder_remote_percent_(inputs.neworder_remote_percent_),
      payment_remote_percent_(inputs.payment_remote_percent_),
      rnd_(kRandomSeed + inputs.worker_id_) {
    outputs_->processed_ = 0;
    outputs_->user_requested_aborts_ = 0;
    outputs_->race_aborts_ = 0;
    outputs_->unexpected_aborts_ = 0;
    outputs_->largereadset_aborts_ = 0;
//    std::memset(stat_wids_, 0, sizeof(stat_wids_));
//    std::memset(stat_dids_, 0, sizeof(stat_dids_));
  }
  ~TpccClientTask() {}

  ErrorStack run(thread::Thread* context);
  ErrorStack run_impl(thread::Thread* context);

  uint32_t get_worker_id() const { return worker_id_; }
  uint32_t get_user_requested_aborts() const { return outputs_->user_requested_aborts_; }
  uint32_t increment_user_requested_aborts() { return ++outputs_->user_requested_aborts_; }
  uint32_t get_race_aborts() const { return outputs_->race_aborts_; }
  uint32_t increment_race_aborts() { return ++outputs_->race_aborts_; }
  uint32_t get_unexpected_aborts() const { return outputs_->unexpected_aborts_; }
  uint32_t increment_unexpected_aborts() { return ++outputs_->unexpected_aborts_; }
  uint32_t get_largereadset_aborts() const { return outputs_->largereadset_aborts_; }
  uint32_t increment_largereadset_aborts() { return ++outputs_->largereadset_aborts_; }

  bool is_stop_requested() const {
    assorted::memory_fence_acquire();
    return channel_->stop_flag_.load();
  }

  uint64_t get_processed() const { return outputs_->processed_; }


//  uint32_t    debug_wdcid_access_[kCustomers * kDistricts * kMaxWarehouses];
//  uint32_t    debug_wdid_access_[kDistricts * kMaxWarehouses];

//  uint32_t  stat_wids_[kMaxWarehouses];
//  uint32_t  stat_dids_[kDistricts];

 private:
  /** unique ID of this worker from 0 to #workers-1. */
  const uint32_t    worker_id_;
  const Wid         total_warehouses_;

  /** inclusive beginning of "home" wid */
  const Wid from_wid_;
  /** exclusive end of "home" wid */
  const Wid to_wid_;

  /** Set to true only when compiled and run in OLAP_MODE */
  const bool olap_mode_;

  /** Set to true only when compiled and run in OLAP_MODE and also given dirty_read=true */
  const bool dirty_read_mode_;

  TpccClientChannel* channel_;

  TpccStorages      storages_;

  /** set at the beginning of run() for convenience */
  thread::Thread*   context_;
  Engine*           engine_;
  Outputs* const    outputs_;


  /**
   * Percent of each orderline that is inserted to remote warehouse.
   * The default value is 1 (which means a little bit less than 10% of an order has some remote
   * orderline). This corresponds to H-Store's neworder_multip/neworder_multip_mix in
   * tpcc.properties.
   */
  const uint16_t    neworder_remote_percent_;

  /**
   * Percent of each payment that is inserted to remote warehouse. The default value is 5.
   * This corresponds to H-Store's payment_multip/payment_multip_mix in tpcc.properties.
   */
  const uint16_t    payment_remote_percent_;


  /** thread local random. */
  assorted::UniformRandom rnd_;

  /** Updates timestring_ only per second. */
  uint64_t    previous_timestring_update_;

  char        ctime_buffer_[64];

  assorted::FixedString<28> timestring_;

  Cid     tmp_cids_[kMaxCidsPerLname];

  // For neworder. these are for showing results on stdout (part of the spec, kind of)
  char        output_bg_[kMaxOlCount];
  uint32_t    output_prices_[kMaxOlCount];
  char        output_item_names_[kMaxOlCount][24];
  uint32_t    output_quantities_[kMaxOlCount];
  double      output_amounts_[kMaxOlCount];
  double      output_total_;

  void      update_timestring_if_needed();

  /** Run the TPCC Neworder transaction. Implemented in tpcc_neworder.cpp. */
  ErrorCode do_neworder(Wid wid);
  ErrorCode do_neworder_create_orderlines(
    Wid wid,
    Did did,
    Oid oid,
    double w_tax,
    double d_tax,
    double c_discount,
    bool will_rollback,
    Ol ol_cnt,
    bool* all_local_warehouse);

  /** Run the TPCC Payment transaction. Implemented in tpcc_payment.cpp. */
  ErrorCode do_payment(Wid wid);

  /** Run the TPCC Neworder transaction. Implemented in tpcc_order_status.cpp. */
  ErrorCode do_order_status(Wid wid);
  ErrorCode get_last_orderid_by_customer(Wid wid, Did did, Cid cid, Oid* oid);

  /** Run the TPCC Neworder transaction. Implemented in tpcc_delivery.cpp. */
  ErrorCode do_delivery(Wid wid);

  /** Run the TPCC Neworder transaction. Implemented in tpcc_stock_level.cpp. */
  ErrorCode do_stock_level(Wid wid);


  /** slightly special. Search 60% by last name (take midpoint), 40% by ID. */
  ErrorCode lookup_customer_by_id_or_name(Wid wid, Did did, Cid *cid);
  ErrorCode lookup_customer_by_name(Wid wid, Did did, const char* lname, Cid *cid);

  /**
   * SELECT SUM(ol_amount) FROM ORDERLINE WHERE wid/did/oid=..
   * UPDATE ORDERLINE SET DELIVERY_D=delivery_time WHERE wid/did/oid=..
   * Implemented in tpcc_delivery.cpp.
   */
  ErrorCode update_orderline_delivery_dates(
    Wid wid,
    Did did,
    Oid oid,
    const char* delivery_date,
    uint32_t delivery_date_len,
    uint64_t* ol_amount_total,
    uint32_t* ol_count);


  /**
    * SELECT TOP 1 OID FROM NEWORDER WHERE WID=wid AND DID=did ORDER BY OID
    * then delete it from NEWORDER, returning the OID (kErrorCodeStrKeyNotFound if no record found).
    * Implemented in tpcc_delivery.cpp.
    */
  ErrorCode pop_neworder(Wid wid, Did did, Oid* oid);

  Did get_random_district_id() ALWAYS_INLINE { return rnd_.uniform_within(0, kDistricts - 1); }
  Wid get_random_warehouse_id() ALWAYS_INLINE {
    return rnd_.uniform_within(0, total_warehouses_ - 1);
  }

  ErrorStack warmup(thread::Thread* context);
};
}  // namespace tpcc
}  // namespace foedus

#endif  // FOEDUS_TPCC_TPCC_CLIENT_HPP_
