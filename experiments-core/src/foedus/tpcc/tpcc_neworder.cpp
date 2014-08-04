/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_client.hpp"

#include <string.h>
#include <glog/logging.h>

#include <string>

#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"

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
  const uint8_t ol_cnt = rnd_.uniform_within(kMinOlCount, kMaxOlCount);

  // New-order transaction has the "1% random rollback" rule.
  const bool will_rollback = (rnd_.uniform_within(1, 100) == 1);

  auto* stocks = storages_.stocks_;
  auto* neworders = storages_.neworders_;
  auto* orders = storages_.orders_;
  auto* orders_secondary = storages_.orders_secondary_;
  auto* orderlines = storages_.orderlines_;

  // these are for showing results on stdout (part of the spec, kind of)
  char        output_bg[kMaxOlCount];
  uint32_t    output_prices[kMaxOlCount];
  char        output_item_names[kMaxOlCount][25];
  uint32_t    output_quantities[kMaxOlCount];
  double      output_amounts[kMaxOlCount];
  double      output_total = 0.0;

  // SELECT TAX from WAREHOUSE
  double w_tax;
  const uint16_t w_offset = offsetof(WarehouseStaticData, tax_);
  CHECK_ERROR_CODE(storages_.warehouses_static_->get_record_primitive(
    context_,
    wid,
    &w_tax,
    w_offset));

  // SELECT TAX FROM DISTRICT
  double d_tax;
  const uint16_t d_tax_offset = offsetof(DistrictStaticData, tax_);
  CHECK_ERROR_CODE(storages_.districts_static_->get_record_primitive(
    context_,
    wdid,
    &d_tax,
    d_tax_offset));

  // UPDATE DISTRICT SET next_o_id=next_o_id+1
  Oid oid = 1;
  CHECK_ERROR_CODE(storages_.districts_next_oid_->increment_record<Oid>(context_, wdid, &oid, 0));
  ASSERT_ND(oid >= kOrders);
  Wdoid wdoid = combine_wdoid(wdid, oid);

  // SELECT DISCOUNT from CUSTOMER
  double c_discount;
  const uint16_t c_offset = offsetof(CustomerStaticData, discount_);
  CHECK_ERROR_CODE(storages_.customers_static_->get_record_primitive(
    context_,
    wdcid,
    &c_discount,
    c_offset));

  // INSERT INTO ORDERLINE with random item.
  bool all_local_warehouse = true;
  for (Ol ol = 1; ol <= ol_cnt; ++ol) {
    const uint32_t quantity = rnd_.uniform_within(1, 10);
    Iid iid = rnd_.non_uniform_within(8191, 0, kItems - 1);
    if (will_rollback) {
        DVLOG(2) << "NewOrder: 1% random rollback happened. Dummy IID=" << iid;
        return kErrorCodeXctUserAbort;
    }

    // only 1% has different wid for supplier.
    bool remote_warehouse = (rnd_.uniform_within(1, 100) <= kNewOrderRemotePercent);
    uint32_t supply_wid;
    if (remote_warehouse && kWarehouses > 1) {
        supply_wid = rnd_.uniform_within_except(0, kWarehouses - 1, wid);
        all_local_warehouse = false;
    } else {
        supply_wid = wid;
    }

    // SELECT ... FROM ITEM WHERE IID=iid
    ItemData i_data;
    CHECK_ERROR_CODE(storages_.items_->get_record(context_, iid, &i_data, 0, sizeof(i_data)));

    // SELECT ... FROM STOCK WHERE WID=supply_wid AND IID=iid
    // then UPDATE quantity and remote count
    Sid sid = combine_sid(wid, iid);
    StockData s_data;
    const uint16_t s_quantity_offset = offsetof(StockData, quantity_);
    const uint16_t s_remote_offset = offsetof(StockData, remote_cnt_);
    CHECK_ERROR_CODE(stocks->get_record(context_, sid, &s_data, 0, sizeof(s_data)));
    if (s_data.quantity_ > quantity) {
        s_data.quantity_ -= quantity;
    } else {
        s_data.quantity_ += (91U - quantity);
    }
    if (remote_warehouse) {
        // in this case we are also incrementing remote cnt
      CHECK_ERROR_CODE(stocks->overwrite_record_primitive<uint32_t>(
        context_,
        sid,
        s_data.remote_cnt_ + 1,
        s_remote_offset));
    }
    // overwrite quantity
    CHECK_ERROR_CODE(stocks->overwrite_record_primitive<uint32_t>(
      context_,
      sid,
      s_data.quantity_,
      s_quantity_offset));

    OrderlineData ol_data;
    ol_data.amount_ = quantity * i_data.price_ * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
    ::memcpy(ol_data.dist_info_, s_data.dist_data_[did], sizeof(ol_data.dist_info_));
    ol_data.iid_ = iid;
    ol_data.quantity_ = quantity;
    ol_data.supply_wid_ = supply_wid;

    Wdol wdol = combine_wdol(wdoid, ol);
    CHECK_ALREADY_EXISTS(orderlines->insert_record_normalized(
      context_,
      wdol,
      &ol_data,
      sizeof(ol_data)));

    // output variables
    output_bg[ol - 1] = ::strstr(i_data.data_, "original") != NULL
        && ::strstr(s_data.data_, "original") != NULL ? 'B' : 'G';
    output_prices[ol - 1] = i_data.price_;
    ::memcpy(output_item_names[ol - 1], i_data.name_, sizeof(i_data.name_));
    output_quantities[ol - 1] = quantity;
    output_amounts[ol - 1] = ol_data.amount_;
    output_total += ol_data.amount_;
  }

  // INSERT INTO ORDERS and NEW_ORDERS
  const std::string& time_str = timestring_;
  OrderData o_data;
  o_data.all_local_ = all_local_warehouse ? 1 : 0;
  o_data.cid_ = cid;
  o_data.carrier_id_ = 0;
  ::memcpy(o_data.entry_d_, time_str.data(), time_str.size());
  o_data.entry_d_[time_str.size()] = '\0';
  o_data.ol_cnt_ = ol_cnt;

  CHECK_ALREADY_EXISTS(orders->insert_record_normalized(context_, wdoid, &o_data, sizeof(o_data)));
  CHECK_ALREADY_EXISTS(neworders->insert_record_normalized(context_, wdoid));
  Wdcoid wdcoid = combine_wdcoid(wdcid, oid);
  CHECK_ALREADY_EXISTS(orders_secondary->insert_record_normalized(context_, wdcoid));

  // show output on console
  DVLOG(3) << "Neworder: : wid=" << wid << ", did=" << did << ", oid=" << oid
    << ", cid=" << cid << ", ol_cnt=" << ol_cnt << ", total=" << output_total
    << ". ol[0]=" << output_bg[0]
    << "." << output_item_names[0] << ".$" << output_prices[0]
    << "*" << output_quantities[0] << "." << output_amounts[0];
  return kErrorCodeOk;
}


}  // namespace tpcc
}  // namespace foedus
