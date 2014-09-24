/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_client.hpp"

#include <string.h>
#include <glog/logging.h>

#include <string>

#include "foedus/compiler.hpp"
#include "foedus/engine.hpp"
#include "foedus/assorted/cacheline.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace tpcc {

// Here, we read next_o_id optimistically, so the OID might be already taken!
// we thus have to expect KeyAlreadyExists error. We convert it to RaceAbort error.
// In other places, KeyAlreadyExists should not happen.
#define CHECK_ALREADY_EXISTS(x)\
{\
  foedus::ErrorCode __e = x;\
  if (__e == kErrorCodeStrKeyAlreadyExists) {\
    return kErrorCodeXctRaceAbort;\
  } else if (__e != kErrorCodeOk) {\
    return __e;\
  }\
}

ErrorCode TpccClientTask::do_neworder(Wid wid) {
  const Did did = get_random_district_id();
  const Wdid wdid = combine_wdid(wid, did);
  const Cid cid = rnd_.non_uniform_within(1023, 0, kCustomers - 1);
  const Wdcid wdcid = combine_wdcid(wdid, cid);
  const Ol ol_cnt = rnd_.uniform_within(kMinOlCount, kMaxOlCount);

  // New-order transaction has the "1% random rollback" rule.
  const bool will_rollback = (rnd_.uniform_within(1, 100) == 1);

  auto neworders = storages_.neworders_;
  auto orders = storages_.orders_;
  auto orders_secondary = storages_.orders_secondary_;

  // SELECT TAX from WAREHOUSE
  double w_tax;

  CHECK_ERROR_CODE(storages_.warehouses_static_.get_record_primitive(
    context_,
    wid,
    &w_tax,
    offsetof(WarehouseStaticData, tax_)));

  // SELECT TAX FROM DISTRICT
  double d_tax;
  CHECK_ERROR_CODE(storages_.districts_static_.get_record_primitive(
    context_,
    wdid,
    &d_tax,
    offsetof(DistrictStaticData, tax_)));

  // UPDATE DISTRICT SET next_o_id=next_o_id+1
  Oid oid = 1;
  CHECK_ERROR_CODE(storages_.districts_next_oid_.increment_record<Oid>(context_, wdid, &oid, 0));
  ASSERT_ND(oid >= kOrders);
  Wdoid wdoid = combine_wdoid(wdid, oid);

  // SELECT DISCOUNT from CUSTOMER
  double c_discount;
  const uint16_t c_offset = offsetof(CustomerStaticData, discount_);
  CHECK_ERROR_CODE(storages_.customers_static_.get_record_primitive<double>(
    context_,
    wdcid,
    &c_discount,
    c_offset));

  bool all_local_warehouse = true;
  output_total_ = 0.0;
  CHECK_ERROR_CODE(do_neworder_create_orderlines(
    wid,
    did,
    oid,
    w_tax,
    d_tax,
    c_discount,
    will_rollback,
    ol_cnt,
    &all_local_warehouse));

  // INSERT INTO ORDERS and NEW_ORDERS
  OrderData o_data;
  o_data.all_local_ = all_local_warehouse ? 1 : 0;
  o_data.cid_ = cid;
  o_data.carrier_id_ = 0;
  std::memcpy(o_data.entry_d_, timestring_.data(), timestring_.size());
  if (timestring_.size() < sizeof(o_data.entry_d_)) {
    std::memset(
      o_data.entry_d_ + timestring_.size(),
      0,
      sizeof(o_data.entry_d_) - timestring_.size());
  }
  o_data.ol_cnt_ = ol_cnt;

  CHECK_ALREADY_EXISTS(orders.insert_record_normalized(context_, wdoid, &o_data, sizeof(o_data)));
  CHECK_ALREADY_EXISTS(neworders.insert_record_normalized(context_, wdoid));
  Wdcoid wdcoid = combine_wdcoid(wdcid, oid);
  CHECK_ALREADY_EXISTS(orders_secondary.insert_record_normalized(context_, wdcoid));

  // show output on console
  DVLOG(3) << "Neworder: : wid=" << wid << ", did=" << did << ", oid=" << oid
    << ", cid=" << cid << ", ol_cnt=" << ol_cnt << ", total=" << output_total_
    << ". ol[0]=" << output_bg_[0]
    << "." << std::string(output_item_names_[0], sizeof(output_item_names_[0]))
    << ".$" << output_prices_[0]
    << "*" << output_quantities_[0] << "." << output_amounts_[0];
  Epoch ep;
  return engine_->get_xct_manager()->precommit_xct(context_, &ep);
}

const char* kOriginalStr = "original";
inline uint64_t as_int_aligned(const char* aligned_str) {
  const uint64_t* str = reinterpret_cast<const uint64_t*>(ASSUME_ALIGNED(aligned_str, 8));
  return *str;
}
// "original" is just 8 bytes. let's exploit it.
const uint64_t kOriginalInt = as_int_aligned(kOriginalStr);

ErrorCode TpccClientTask::do_neworder_create_orderlines(
  Wid wid,
  Did did,
  Oid oid,
  double w_tax,
  double d_tax,
  double c_discount,
  bool will_rollback,
  Ol ol_cnt,
  bool* all_local_warehouse) {
  Wdid wdid = combine_wdid(wid, did);
  Wdoid wdoid = combine_wdoid(wdid, oid);
  // INSERT INTO ORDERLINE with random item.

  // first, determine list of StockID (WID+IID) and parameters.
  uint32_t quantities[kOlMax];
  storage::array::ArrayOffset iids[kOlMax];
  storage::array::ArrayOffset sids[kOlMax];
  for (Ol ol = 1; ol <= ol_cnt; ++ol) {
    quantities[ol - 1] = rnd_.uniform_within(1, 10);
    iids[ol - 1] = rnd_.non_uniform_within(8191, 0, kItems - 1);
    if (will_rollback) {
        DVLOG(2) << "NewOrder: 1% random rollback happened.";
        return kErrorCodeXctUserAbort;
    }

    // only 1% has different wid for supplier.
    bool remote_warehouse = (rnd_.uniform_within(1, 100) <= neworder_remote_percent_);
    uint32_t supply_wid;
    if (remote_warehouse && total_warehouses_ > 1U) {
        supply_wid = rnd_.uniform_within_except(0, total_warehouses_ - 1, wid);
        *all_local_warehouse = false;
    } else {
        supply_wid = wid;
    }
    sids[ol - 1] = combine_sid(supply_wid, static_cast<Iid>(iids[ol - 1]));
  }

  // then, read stock/item in a batched way so that we can parallelize cacheline prefetches
  // SELECT ... FROM ITEM WHERE IID=iid
  const void* i_data_address[kOlMax];
  CHECK_ERROR_CODE(storages_.items_.get_record_payload_batch(
      context_,
      ol_cnt,
      iids,
      i_data_address));
  // SELECT ... FROM STOCK WHERE WID=supply_wid AND IID=iid
  // then UPDATE quantity and remote count
  storage::Record* s_records[kOlMax];
  CHECK_ERROR_CODE(storages_.stocks_.get_record_for_write_batch(
      context_,
      ol_cnt,
      sids,
      s_records));

  // prefetch required columns. note that the first 64bytes are already prefetched
  for (Ol ol = 1; ol <= ol_cnt; ++ol) {
    // prefetch second half of item
    assorted::prefetch_cacheline(reinterpret_cast<const char*>(i_data_address[ol - 1]) + 64);
    // prefetch required columns of stock
    const StockData* s_data = reinterpret_cast<const StockData*>(s_records[ol - 1]->payload_);
    assorted::prefetch_cacheline(s_data->dist_data_[did]);
    assorted::prefetch_cacheline(s_data->data_);
  }

  const uint16_t s_quantity_offset = offsetof(StockData, quantity_);
  const uint16_t s_remote_offset = offsetof(StockData, remote_cnt_);
  for (Ol ol = 1; ol <= ol_cnt; ++ol) {
    const ItemData* i_data = reinterpret_cast<const ItemData*>(i_data_address[ol - 1]);
    const StockData* s_data = reinterpret_cast<const StockData*>(s_records[ol - 1]->payload_);
    uint32_t quantity = quantities[ol - 1];
    uint32_t new_quantity = s_data->quantity_;
    if (new_quantity > quantity) {
        new_quantity -= quantity;
    } else {
        new_quantity += (91U - quantity);
    }

    Wid supply_wid = extract_wid_from_sid(sids[ol - 1]);
    if (supply_wid != wid) {
        // in this case we are also incrementing remote cnt
      CHECK_ERROR_CODE(storages_.stocks_.overwrite_record_primitive<uint32_t>(
        context_,
        sids[ol - 1],
        s_records[ol - 1],
        s_data->remote_cnt_ + 1,
        s_remote_offset));
    }
    // overwrite quantity
    CHECK_ERROR_CODE(storages_.stocks_.overwrite_record_primitive<uint32_t>(
      context_,
      sids[ol - 1],
      s_records[ol - 1],
      new_quantity,
      s_quantity_offset));

    OrderlineData ol_data;
    ol_data.amount_ = quantity * i_data->price_ * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
    std::memcpy(ol_data.dist_info_, s_data->dist_data_[did], sizeof(ol_data.dist_info_));
    ol_data.iid_ = iids[ol - 1];
    ol_data.quantity_ = quantity;
    ol_data.supply_wid_ = supply_wid;

    Wdol wdol = combine_wdol(wdoid, ol);
    CHECK_ALREADY_EXISTS(storages_.orderlines_.insert_record_normalized(
      context_,
      wdol,
      &ol_data,
      sizeof(ol_data)));

    // output variables
    output_bg_[ol - 1] = as_int_aligned(i_data->data_) != kOriginalInt &&
      as_int_aligned(s_data->data_) != kOriginalInt ? 'B' : 'G';
    output_prices_[ol - 1] = i_data->price_;
    std::memcpy(output_item_names_[ol - 1], i_data->name_, sizeof(i_data->name_));
    output_quantities_[ol - 1] = quantity;
    output_amounts_[ol - 1] = ol_data.amount_;
    output_total_ += ol_data.amount_;
  }
  return kErrorCodeOk;
}


}  // namespace tpcc
}  // namespace foedus
