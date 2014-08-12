/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_client.hpp"

#include <glog/logging.h>

#include <cstddef>
#include <cstdio>
#include <string>

#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"

namespace foedus {
namespace tpcc {
ErrorCode TpccClientTask::do_payment(Wid c_wid) {
  // these are the customer's home wid/did
  const Did c_did = get_random_district_id();
  const double amount = static_cast<double>(rnd_.uniform_within(100, 500000)) / 100.0f;

  // 85% accesses the home wid/did. 15% other wid/did (wid must be !=c_wid).
  Wid wid;
  Did did;
  const bool remote_warehouse = rnd_.uniform_within(1, 100) <= payment_remote_percent_;
  if (remote_warehouse && total_warehouses_ > 1U) {
    wid = rnd_.uniform_within_except(0, total_warehouses_ - 1, c_wid);
    did = rnd_.uniform_within(0, kDistricts - 1);  // re-draw did.
  } else {
    wid = c_wid;
    did = c_did;
  }

  // SELECT NAME FROM WAREHOUSE
  const void *w_address;
  CHECK_ERROR_CODE(storage::array::ArrayStorage::get_record_payload(
    context_,
    storages_.warehouses_static_cache_,
    wid,
    &w_address));
  const WarehouseStaticData* w_record = reinterpret_cast<const WarehouseStaticData*>(w_address);

  // UPDATE WAREHOUSE SET YTD=YTD+amount
  CHECK_ERROR_CODE(storages_.warehouses_ytd_->increment_record_oneshot<double>(
    context_,
    wid,
    amount,
    0));

  // SELECT NAME FROM DISTRICT
  const void *d_address;
  CHECK_ERROR_CODE(storage::array::ArrayStorage::get_record_payload(
    context_,
    storages_.districts_static_cache_,
    did,
    &d_address));
  const DistrictStaticData* d_record = reinterpret_cast<const DistrictStaticData*>(d_address);

  // UPDATE DISTRICT SET YTD=YTD+amount
  CHECK_ERROR_CODE(storages_.districts_ytd_->increment_record_oneshot<double>(
    context_,
    did,
    amount,
    0));

  // get customer record.
  Cid cid;
  ErrorCode ret_customer = lookup_customer_by_id_or_name(c_wid, c_did, &cid);
  if (ret_customer == kErrorCodeStrKeyNotFound) {
    DVLOG(1) << "OrderStatus: customer of random last name not found";
    return kErrorCodeOk;  // this is a correct result
  } else if (ret_customer != kErrorCodeOk) {
    return ret_customer;
  }
  Wdcid wdcid = combine_wdcid(combine_wdid(c_wid, c_did), cid);

  const std::string& time_str = timestring_;

  // UPDATE CUSTOMER SET BALANCE-=amount,YTD_PAYMENT+=amount,PAYMENT_CNT++
  // (if C_CREDID="BC") C_DATA=...
  // unless c_credit="BC", we don't retrieve history data. see section 2.5.2.2.

  CHECK_ERROR_CODE(storages_.customers_dynamic_->increment_record_oneshot<double>(
    context_,
    wdcid,
    -amount,
    offsetof(CustomerDynamicData, balance_)));
  CHECK_ERROR_CODE(storages_.customers_dynamic_->increment_record_oneshot<uint64_t>(
    context_,
    wdcid,
    static_cast<uint64_t>(amount),
    offsetof(CustomerDynamicData, ytd_payment_)));

  const void *c_address;
  CHECK_ERROR_CODE(storage::array::ArrayStorage::get_record_payload(
    context_,
    storages_.customers_static_cache_,
    wdcid,
    &c_address));
  const CustomerStaticData* c_record = reinterpret_cast<const CustomerStaticData*>(c_address);

  if (c_record->credit_[0] == 'B' && c_record->credit_[1] == 'C') {
    // in this case we are also retrieving and rewriting data_.
    // what/how much is faster?
    // http://zverovich.net/2013/09/07/integer-to-string-conversion-in-cplusplus.html
    // let's consider cppformat if this turns out to be bottleneck
    char c_old_data[CustomerStaticData::kHistoryDataLength];
    CHECK_ERROR_CODE(storage::array::ArrayStorage::get_record(
      context_,
      storages_.customers_history_cache_,
      wdcid,
      c_old_data,
      0,
      sizeof(c_old_data)));
    char c_new_data[CustomerStaticData::kHistoryDataLength];
    int written = std::snprintf(
      c_new_data,
      sizeof(c_new_data),
      "| %4d %2d %4d %2d %4d $%7.2f",
      cid,
      did,
      wid,
      c_did,
      c_wid,
      amount);
    std::memcpy(c_new_data + written, time_str.data(), time_str.size());
    written += time_str.size();
    std::memcpy(c_new_data + written, c_old_data, sizeof(c_new_data) - written - 1);
    c_new_data[sizeof(c_new_data) - 1] = '\0';
    CHECK_ERROR_CODE(storages_.customers_history_->overwrite_record(
      context_,
      wdcid,
      c_new_data,
      0,
      sizeof(c_new_data)));
  }

  // INSERT INTO HISTORY
  HistoryData h_data;
  h_data.amount_ = amount;
  h_data.c_did_ = c_did;
  h_data.cid_ = cid;
  h_data.c_wid_ = c_wid;
  h_data.did_ = did;
  h_data.wid_ = wid;

  std::memcpy(h_data.data_, w_record->name_, 10);
  h_data.data_[10] = ' ';
  h_data.data_[11] = ' ';
  h_data.data_[12] = ' ';
  h_data.data_[13] = ' ';
  std::memcpy(h_data.data_ + 14, d_record->name_, 11);

  std::memcpy(h_data.date_, time_str.data(), time_str.size());
  h_data.date_[time_str.size()] = '\0';

  CHECK_ERROR_CODE(storages_.histories_->append_record(context_, &h_data, sizeof(h_data)));

  DVLOG(2) << "Payment: wid=" << wid << ", did=" << did
    << ", cid=" << cid << ", c_wid=" << c_wid << ", c_did=" << c_did
    << ", time=" << time_str;
  return kErrorCodeOk;
}


}  // namespace tpcc
}  // namespace foedus
