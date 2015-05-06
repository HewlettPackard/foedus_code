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
#include "foedus/tpcc/tpcc_client.hpp"

#include <glog/logging.h>

#include <cstddef>
#include <cstdio>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace tpcc {
ErrorCode TpccClientTask::do_payment(Wid c_wid) {
  // these are the customer's home wid/did
  const Did c_did = get_random_district_id();
  const double amount = static_cast<double>(rnd_.uniform_within(100, 500000)) / 100.0f;

  // 85% accesses the home wid/did. 15% other wid/did (wid must be !=c_wid).
  Wid wid;
  Did did;
  const bool remote_warehouse = (rnd_.uniform_within(1, 100) <= payment_remote_percent_);
  if (remote_warehouse && total_warehouses_ > 1U) {
    // alex_memstat only: pair with "the only remote node"
    // wid = rnd_.uniform_within_except(0, total_warehouses_ - 1, c_wid);
    wid = (c_wid + (total_warehouses_ / 2)) % total_warehouses_;
    did = rnd_.uniform_within(0, kDistricts - 1);  // re-draw did.
//    ++stat_wids_[wid];
//    ++stat_dids_[did];
  } else {
    wid = c_wid;
    did = c_did;
  }
  const Wdid wdid = combine_wdid(wid, did);
  // ++debug_wdid_access_[wdid];
  // SELECT NAME FROM WAREHOUSE
  const void *w_address;
  CHECK_ERROR_CODE(storages_.warehouses_static_.get_record_payload(
    context_,
    wid,
    &w_address));
  const WarehouseStaticData* w_record = reinterpret_cast<const WarehouseStaticData*>(w_address);

  // UPDATE WAREHOUSE SET YTD=YTD+amount
  CHECK_ERROR_CODE(storages_.warehouses_ytd_.increment_record_oneshot<double>(
    context_,
    wid,
    amount,
    0));

  // SELECT NAME FROM DISTRICT
  const void *d_address;
  CHECK_ERROR_CODE(storages_.districts_static_.get_record_payload(
    context_,
    wdid,
    &d_address));
  const DistrictStaticData* d_record = reinterpret_cast<const DistrictStaticData*>(d_address);

  // UPDATE DISTRICT SET YTD=YTD+amount
  CHECK_ERROR_CODE(storages_.districts_ytd_.increment_record_oneshot<double>(
    context_,
    wdid,
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
  // ++debug_wdcid_access_[wdcid];

  // UPDATE CUSTOMER SET BALANCE-=amount,YTD_PAYMENT+=amount,PAYMENT_CNT++
  // (if C_CREDID="BC") C_DATA=...
  // unless c_credit="BC", we don't retrieve history data. see section 2.5.2.2.

  CHECK_ERROR_CODE(storages_.customers_dynamic_.increment_record_oneshot<double>(
    context_,
    wdcid,
    -amount,
    offsetof(CustomerDynamicData, balance_)));
  CHECK_ERROR_CODE(storages_.customers_dynamic_.increment_record_oneshot<uint64_t>(
    context_,
    wdcid,
    static_cast<uint64_t>(amount),
    offsetof(CustomerDynamicData, ytd_payment_)));

  const void *c_address;
  CHECK_ERROR_CODE(storages_.customers_static_.get_record_payload(
    context_,
    wdcid,
    &c_address));
  const CustomerStaticData* c_record = reinterpret_cast<const CustomerStaticData*>(c_address);

  if (c_record->credit_[0] == 'B' && c_record->credit_[1] == 'C') {
    // in this case we are also retrieving and rewriting data_.
    // what/how much is faster?
    // http://zverovich.net/2013/09/07/integer-to-string-conversion-in-cplusplus.html
    // let's consider cppformat if this turns out to be bottleneck
    char c_old_data[CustomerStaticData::kHistoryDataLength];
    CHECK_ERROR_CODE(storages_.customers_history_.get_record(
      context_,
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
    std::memcpy(c_new_data + written, timestring_.data(), timestring_.size());
    written += timestring_.size();
    std::memcpy(c_new_data + written, c_old_data, sizeof(c_new_data) - written);
    CHECK_ERROR_CODE(storages_.customers_history_.overwrite_record(
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
  std::memcpy(h_data.data_ + 14, d_record->name_, 10);

  std::memcpy(h_data.date_, timestring_.data(), timestring_.size());
  if (timestring_.size() < sizeof(h_data.date_)) {
    std::memset(h_data.date_ + timestring_.size(), 0, sizeof(h_data.date_) - timestring_.size());
  }
  CHECK_ERROR_CODE(storages_.histories_.append_record(context_, &h_data, sizeof(h_data)));

  DVLOG(2) << "Payment: wid=" << wid << ", did=" << static_cast<int>(did)
    << ", cid=" << cid << ", c_wid=" << c_wid << ", c_did=" << static_cast<int>(c_did)
    << ", time=" << timestring_.str();
  Epoch ep;
  return engine_->get_xct_manager()->precommit_xct(context_, &ep);
}


}  // namespace tpcc
}  // namespace foedus
