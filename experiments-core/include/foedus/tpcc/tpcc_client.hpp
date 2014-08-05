/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_TPCC_TPCC_CLIENT_HPP_
#define FOEDUS_TPCC_TPCC_CLIENT_HPP_

#include <stdint.h>
#include <time.h>

#include <cstring>
#include <set>
#include <string>
#include <vector>

#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/impersonate_task.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/tpcc/tpcc.hpp"
#include "foedus/tpcc/tpcc_scale.hpp"
#include "foedus/tpcc/tpcc_schema.hpp"

namespace foedus {
namespace tpcc {
/**
 * @brief The worker thread to run transactions in the experiment.
 * @details
 * This is the canonical TPCC workload which use as the default experiment.
 * We also have various focused/modified workload to evaluate specific aspects.
 */
class TpccClientTask : public thread::ImpersonateTask {
 public:
  enum Constants {
    kRandomSeed = 123456,
    kRandomCount = 1 << 16,
    /** on average only 3. surely won't be more than this number */
    kMaxCidsPerLname = 128,
  };
  TpccClientTask(
    uint32_t worker_id,
    uint16_t neworder_remote_percent,
    uint16_t payment_remote_percent,
    TpccStorages storages,
    thread::Rendezvous* start_rendezvous)
    : worker_id_(worker_id),
      neworder_remote_percent_(neworder_remote_percent),
      payment_remote_percent_(payment_remote_percent),
      rnd_(kRandomSeed + worker_id),
      processed_(0) {
    storages_ = storages;
    stop_requrested_ = false;
    start_rendezvous_ = start_rendezvous;
    user_requested_aborts_ = 0;
    race_aborts_ = 0;
    unexpected_aborts_ = 0;
  }

  ErrorStack run(thread::Thread* context);

  uint32_t get_user_requested_aborts() const { return user_requested_aborts_; }
  uint32_t increment_user_requested_aborts() { return ++user_requested_aborts_; }
  uint32_t get_race_aborts() const { return race_aborts_; }
  uint32_t increment_race_aborts() { return ++race_aborts_; }
  uint32_t get_unexpected_aborts() const { return unexpected_aborts_; }
  uint32_t increment_unexpected_aborts() { return ++unexpected_aborts_; }

  void request_stop() { stop_requrested_ = true; }

  uint64_t get_processed() const { return processed_; }

 private:
  /** unique ID of this worker from 0 to #workers-1. */
  const uint32_t    worker_id_;

  thread::Rendezvous* start_rendezvous_;

  TpccStorages      storages_;

  bool              stop_requrested_;

  /** set at the beginning of run() for convenience */
  thread::Thread*   context_;
  Engine*           engine_;


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


  memory::AlignedMemory numbers_;
  /** thread local random. */
  assorted::UniformRandom rnd_;

  /** How many transactions processed so far*/
  uint64_t processed_;

  // statistics
  uint32_t user_requested_aborts_;
  uint32_t race_aborts_;
  /** this is usually up to 1 because we stop execution as soon as this happens */
  uint32_t unexpected_aborts_;

  /** Updates timestring_ only per second. */
  uint64_t    previous_timestring_update_;
  std::string timestring_;

  Cid     tmp_cids_[kMaxCidsPerLname];

  // For neworder. these are for showing results on stdout (part of the spec, kind of)
  char        output_bg_[kMaxOlCount];
  uint32_t    output_prices_[kMaxOlCount];
  char        output_item_names_[kMaxOlCount][25];
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
    uint64_t* ol_amount_total,
    uint32_t* ol_count);


  /**
    * SELECT TOP 1 OID FROM NEWORDER WHERE WID=wid AND DID=did ORDER BY OID
    * then delete it from NEWORDER, returning the OID (kErrorCodeStrKeyNotFound if no record found).
    * Implemented in tpcc_delivery.cpp.
    */
  ErrorCode pop_neworder(Wid wid, Did did, Oid* oid);

  Did get_random_district_id() ALWAYS_INLINE { return rnd_.uniform_within(0, kDistricts - 1); }
  Wid get_random_warehouse_id() ALWAYS_INLINE { return rnd_.uniform_within(0, kWarehouses - 1); }
};
}  // namespace tpcc
}  // namespace foedus

#endif  // FOEDUS_TPCC_TPCC_CLIENT_HPP_
