/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_client.hpp"

#include <glog/logging.h>

#include <string>

#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace tpcc {

ErrorCode TpccClientTask::do_order_status(Wid wid) {
  const Did did = get_random_district_id();

  Cid cid;
  Epoch ep;
  ErrorCode ret_customer = lookup_customer_by_id_or_name(wid, did, &cid);
  if (ret_customer == kErrorCodeStrKeyNotFound) {
    DVLOG(1) << "OrderStatus: customer of random last name not found";
    return engine_->get_xct_manager().precommit_xct(context_, &ep);  // this is a correct result
  } else if (ret_customer != kErrorCodeOk) {
    return ret_customer;
  }

  // identify the last order by this customer
  Oid oid;
  ErrorCode ret = get_last_orderid_by_customer(wid, did, cid, &oid);
  if (ret == kErrorCodeStrKeyNotFound) {
    DVLOG(1) << "OrderStatus: no order";
    return engine_->get_xct_manager().precommit_xct(context_, &ep);  // this is a correct result
  } else if (ret != kErrorCodeOk) {
    return ret;
  }

  // SELECT IID,SUPPLY_WID,QUANTITY,AMOUNT,DELIVERY_D FROM ORDERLINE
  // WHERE WID/DID/OID=..
  Wdid wdid = combine_wdid(wid, did);
  Wdol low = combine_wdol(combine_wdoid(wdid, oid), 0U);
  Wdol high = combine_wdol(combine_wdoid(wdid, oid + 1U), 0U);
  storage::masstree::MasstreeCursor cursor(storages_.orderlines_, context_);
  CHECK_ERROR_CODE(cursor.open_normalized(low, high));
  uint32_t cnt = 0;
  while (cursor.is_valid_record()) {
    ASSERT_ND(assorted::read_bigendian<Wdol>(cursor.get_key()) >= low);
    ASSERT_ND(assorted::read_bigendian<Wdol>(cursor.get_key()) < high);
    ASSERT_ND(cursor.get_key_length() == sizeof(Wdol));
    ASSERT_ND(cursor.get_payload_length() == sizeof(OrderlineData));

    ++cnt;
    const OrderlineData *ol_data = reinterpret_cast<const OrderlineData*>(cursor.get_payload());
    DVLOG(3) << "Order-status[" << cnt << "]:"
        << "IID=" << ol_data->iid_
        << ", SUPPLY_WID=" << ol_data->supply_wid_
        << ", QUANTITY=" << ol_data->quantity_
        << ", AMOUNT=" << ol_data->amount_
        << ", DELIVERY_D=" << std::string(ol_data->delivery_d_, sizeof(ol_data->delivery_d_));

    CHECK_ERROR_CODE(cursor.next());
  }

  DVLOG(2) << "Order-status:" << cnt << " records. wid=" << wid
    << ", did=" << did << ", cid=" << cid << ", oid=" << oid << std::endl;
  return engine_->get_xct_manager().precommit_xct(context_, &ep);
}

ErrorCode TpccClientTask::get_last_orderid_by_customer(Wid wid, Did did, Cid cid, Oid* oid) {
  // SELECT TOP 1 ... FROM ORDERS WHERE WID/DID/CID=.. ORDER BY OID DESC
  // Use the secondary index for this query.
  Wdid wdid = combine_wdid(wid, did);
  Wdcoid low = combine_wdcoid(combine_wdcid(wdid, cid), 0U);
  Wdcoid high = combine_wdcoid(combine_wdcid(wdid, cid + 1U), 0U);
  storage::masstree::MasstreeCursor cursor(storages_.orders_secondary_, context_);
  CHECK_ERROR_CODE(cursor.open_normalized(high, low, false, false, false, true));
  if (cursor.is_valid_record()) {
    Wdcoid key = assorted::read_bigendian<Wdcoid>(cursor.get_key());
    ASSERT_ND(key >= low);
    ASSERT_ND(key < high);
    ASSERT_ND(cursor.get_key_length() == sizeof(Wdcoid));
    ASSERT_ND(cursor.get_payload_length() == 0U);
    *oid = extract_oid_from_wdcoid(key);
    return kErrorCodeOk;
  } else {
    return kErrorCodeStrKeyNotFound;
  }
}

}  // namespace tpcc
}  // namespace foedus
