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
  const bool remote_warehouse = rnd_.uniform_within(1, 100) <= kPaymentRemotePercent;
  if (remote_warehouse) {
    wid = rnd_.uniform_within_except(0, kWarehouses - 1, c_wid);
    did = rnd_.uniform_within(0, kDistricts - 1);  // re-draw did.
  } else {
    wid = c_wid;
    did = c_did;
  }

  // UPDATE WAREHOUSE SET YTD=YTD+amount
  char w_name[11];
  CHECK_ERROR_CODE(storages_.warehouses_->get_record(context_, wid, w_name, 0, sizeof(w_name)));
  double amount_after = amount;
  const uint16_t w_offset = offsetof(WarehouseData, ytd_);
  CHECK_ERROR_CODE(storages_.warehouses_->increment_record<double>(
    context_,
    wid,
    &amount_after,
    w_offset));

  // UPDATE DISTRICT SET YTD=YTD+amount
  char d_name[11];
  CHECK_ERROR_CODE(storages_.districts_->get_record(context_, did, d_name, 0, sizeof(d_name)));
  amount_after = amount;
  const uint16_t d_offset = offsetof(DistrictData, ytd_);
  CHECK_ERROR_CODE(storages_.districts_->increment_record<double>(
    context_,
    did,
    &amount_after,
    d_offset));

  // get customer record.
  Cid cid;
  CHECK_ERROR_CODE(lookup_customer_by_id_or_name(c_wid, c_did, &cid));
  Wdcid wdcid = combine_wdcid(combine_wdid(c_wid, c_did), cid);

  std::string time_str(get_current_time_string());

  // UPDATE CUSTOMER SET BALANCE-=amount,YTD_PAYMENT+=amount
  // (if C_CREDID="BC") C_DATA=...
  CustomerData c_data;
  const uint16_t c_data_offset = offsetof(CustomerData, data_);
  const uint16_t c_ytd_offset = offsetof(CustomerData, ytd_payment_);
  auto* customers = storages_.customers_;
  // unless c_credit="BC", we don't retrieve c_data. see section 2.5.2.2.
  CHECK_ERROR_CODE(customers->get_record(context_, wdcid, &c_data, 0, c_data_offset));

  c_data.balance_ -= amount;
  c_data.ytd_payment_ += amount;
  // ytd_payment_/balance_ are contiguous
  CHECK_ERROR_CODE(customers->overwrite_record(
    context_,
    wdcid,
    reinterpret_cast<char*>(&c_data) + c_ytd_offset,
    c_ytd_offset,
    sizeof(c_data.ytd_payment_) + sizeof(c_data.balance_)));
  if (c_data.credit_[0] == 'B' && c_data.credit_[1] == 'C') {
    // in this case we are also retrieving and rewriting data_.
    // what/how much is faster?
    // http://zverovich.net/2013/09/07/integer-to-string-conversion-in-cplusplus.html
    // let's consider cppformat if this turns out to be bottleneck
    CHECK_ERROR_CODE(customers->get_record(
      context_,
      wdcid,
      reinterpret_cast<char*>(&c_data) + c_data_offset,
      c_data_offset,
      sizeof(c_data.data_)));
    char c_new_data[sizeof(c_data.data_)];
    int written = std::snprintf(
      c_new_data,
      sizeof(c_new_data),
      "| %4d %2d %4d %2d %4d $%7.2f",
      cid,
      did,
      wid,
      c_did,
      c_wid,
      amount_after);
    std::memcpy(c_new_data + written, time_str.data(), time_str.size());
    written += time_str.size();
    std::memcpy(c_new_data + written, c_data.data_, sizeof(c_new_data) - written - 1);
    c_new_data[sizeof(c_new_data) - 1] = '\0';
    CHECK_ERROR_CODE(customers->overwrite_record(
      context_,
      wdcid,
      c_new_data,
      c_data_offset,
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

  std::memcpy(h_data.data_, w_name, 10);
  h_data.data_[10] = ' ';
  h_data.data_[11] = ' ';
  h_data.data_[12] = ' ';
  h_data.data_[13] = ' ';
  std::memcpy(h_data.data_ + 14, d_name, 11);

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
