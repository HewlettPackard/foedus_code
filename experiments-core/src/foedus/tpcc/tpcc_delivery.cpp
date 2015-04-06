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
#include <set>
#include <string>

#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace tpcc {

ErrorCode TpccClientTask::do_delivery(Wid wid) {
  const uint32_t carrier_id = rnd_.uniform_within(1, 10);
  for (Did did = 0; did < kDistricts; ++did) {
    Oid oid;
    ErrorCode ret = pop_neworder(wid, did, &oid);
    if (ret == kErrorCodeStrKeyNotFound) {
      DVLOG(1) << "Delivery: no neworder";
      continue;
    } else if (ret != kErrorCodeOk) {
      return ret;
    }

    // SELECT CID FROM ORDER WHERE wid/did/oid=..
    storage::masstree::KeySlice wdoid = to_wdoid_slice(wid, did, oid);
    Cid cid;
    CHECK_ERROR_CODE(storages_.orders_.get_record_primitive_normalized<Cid>(
      context_,
      wdoid,
      &cid,
      offsetof(OrderData, cid_)));

    // UPDATE ORDER SET O_CARRIER_ID=carrier_id WHERE wid/did/oid=..
    // Note that we don't have to update the secondary index
    // as O_CARRIER_ID is not included in it.
    CHECK_ERROR_CODE(storages_.orders_.overwrite_record_primitive_normalized<uint32_t>(
      context_,
      wdoid,
      carrier_id,
      offsetof(OrderData, carrier_id_)));
    // TODO(Hideaki) it's a waste to do this in two steps. but exposing more complicated APIs
    // to do read+write in one shot is arguable... is it cheating or not?

    // SELECT SUM(ol_amount) FROM ORDERLINE WHERE wid/did/oid=..
    // UPDATE ORDERLINE SET DELIVERY_D=delivery_time WHERE wid/did/oid=..
    uint64_t amount_total = 0;
    uint32_t ol_count;
    CHECK_ERROR_CODE(update_orderline_delivery_dates(
      wid,
      did,
      oid,
      timestring_.data(),
      timestring_.size(),
      &amount_total,
      &ol_count));

    // UPDATE CUSTOMER SET balance+=amount_total,delivery_cnt++ WHERE WID/DID/CID=..
    // No need to update secondary index as balance is not a key.
    Wdcid wdcid = combine_wdcid(combine_wdid(wid, did), cid);
    CHECK_ERROR_CODE(storages_.customers_dynamic_.increment_record_oneshot<uint32_t>(
      context_,
      wdcid,
      1U,
      offsetof(CustomerDynamicData, delivery_cnt_)));
    CHECK_ERROR_CODE(storages_.customers_dynamic_.increment_record_oneshot<double>(
      context_,
      wdcid,
      static_cast<double>(amount_total),
      offsetof(CustomerDynamicData, balance_)));

    DVLOG(2) << "Delivery: updated: oid=" << oid << ", #ol=" << ol_count;
  }
  Epoch ep;
  return engine_->get_xct_manager()->precommit_xct(context_, &ep);
}

ErrorCode TpccClientTask::pop_neworder(Wid wid, Did did, Oid* oid) {
  storage::masstree::KeySlice low = to_wdoid_slice(wid, did, 0);
  storage::masstree::KeySlice high = to_wdoid_slice(wid, did + 1, 0);
  storage::masstree::MasstreeCursor cursor(storages_.neworders_, context_);
  CHECK_ERROR_CODE(cursor.open_normalized(high, low, false, true, false, true));
  if (cursor.is_valid_record()) {
    ASSERT_ND(cursor.get_key_length() == sizeof(Wdoid));
    Wdoid id = assorted::read_bigendian<Wdoid>(cursor.get_key());
    *oid = extract_oid_from_wdoid(id);
    // delete the fetched record
    return cursor.delete_record();
  } else {
    return kErrorCodeStrKeyNotFound;
  }
}

ErrorCode TpccClientTask::update_orderline_delivery_dates(
  Wid wid,
  Did did,
  Oid oid,
  const char* delivery_date,
  uint32_t delivery_date_len,
  uint64_t* ol_amount_total,
  uint32_t* ol_count) {
  Wdid wdid = combine_wdid(wid, did);
  Wdol low = combine_wdol(combine_wdoid(wdid, oid), 0U);
  Wdol high = combine_wdol(combine_wdoid(wdid, oid + 1U), 0U);
  *ol_amount_total = 0;
  *ol_count = 0;

  // SELECT SUM(ol_amount) FROM ORDERLINE WHERE wid/did/oid=..
  // UPDATE ORDERLINE SET DELIVERY_D=delivery_time WHERE wid/did/oid=..
  const uint16_t offset = offsetof(OrderlineData, delivery_d_);
  storage::masstree::MasstreeCursor cursor(storages_.orderlines_, context_);
  CHECK_ERROR_CODE(cursor.open_normalized(low, high, true, true));
  while (cursor.is_valid_record()) {
    const char* key_be = cursor.get_key();
    ASSERT_ND(assorted::read_bigendian<Wdol>(key_be) >= low);
    ASSERT_ND(assorted::read_bigendian<Wdol>(key_be) < high);
    ASSERT_ND(cursor.get_key_length() == sizeof(Wdol));
    ASSERT_ND(cursor.get_payload_length() == sizeof(OrderlineData));
    const OrderlineData* payload = reinterpret_cast<const OrderlineData*>(cursor.get_payload());
    *ol_amount_total += payload->amount_;
    ++(*ol_count);
    CHECK_ERROR_CODE(cursor.overwrite_record(delivery_date, offset, delivery_date_len));
    CHECK_ERROR_CODE(cursor.next());
  }

  return kErrorCodeOk;
}

}  // namespace tpcc
}  // namespace foedus
