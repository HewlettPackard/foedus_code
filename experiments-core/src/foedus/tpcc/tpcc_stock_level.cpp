/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_client.hpp"

#include <glog/logging.h>

#include <cstddef>

#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"

namespace foedus {
namespace tpcc {
ErrorCode TpccClientTask::do_stock_level(Wid wid) {
  const Did did = get_random_district_id();
  Wdid wdid = combine_wdid(wid, did);
  const uint32_t threshold = rnd_.uniform_within(10, 20);

  // SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID=wid AND D_ID=did
  uint16_t d_offset = offsetof(DistrictData, next_o_id_);
  Oid next_oid;
  CHECK_ERROR_CODE(storages_.districts_->get_record_primitive<Oid>(
    context_,
    wdid,
    &next_oid,
    d_offset));

  // SELECT COUNT(DISTINCT(s_i_id))
  // FROM ORDERLINE INNER JOIN STOCK ON (WID,IID)
  // WHERE WID=wid AND DID=did AND OID BETWEEN next_oid-20 AND next_oid
  // AND QUANTITY<threshold
  uint32_t result = 0;

  // SELECT OL_I_ID FROM ORDERLINE
  // WHERE WID=wid AND DID=did AND OID BETWEEN oid_from AND oid_to.
  Wdol low = combine_wdol(combine_wdoid(wdid, next_oid - 20U), 0U);
  Wdol high = combine_wdol(combine_wdoid(wdid, next_oid + 1U), 0U);
  storage::masstree::MasstreeCursor cursor(engine_, storages_.orderlines_, context_);
  CHECK_ERROR_CODE(cursor.open_normalized(low, high));
  uint16_t s_offset = offsetof(StockData, quantity_);
  while (cursor.is_valid_record()) {
    ASSERT_ND(assorted::read_bigendian<Wdol>(cursor.get_key()) >= low);
    ASSERT_ND(assorted::read_bigendian<Wdol>(cursor.get_key()) < high);
    ASSERT_ND(cursor.get_key_length() == sizeof(Wdol));
    ASSERT_ND(cursor.get_payload_length() == sizeof(OrderlineData));

    const OrderlineData *ol_data = reinterpret_cast<const OrderlineData*>(cursor.get_payload());
    Iid iid = ol_data->iid_;

    Sid sid = combine_sid(wid, iid);
    uint32_t quantity;
    CHECK_ERROR_CODE(storages_.stocks_->get_record_primitive<uint32_t>(
      context_,
      sid,
      &quantity,
      s_offset));
    if (quantity < threshold) {
      ++result;
    }

    CHECK_ERROR_CODE(cursor.next());
  }

  DVLOG(2) << "Stock-Level: result=" << result;
  return kErrorCodeOk;
}
}  // namespace tpcc
}  // namespace foedus
