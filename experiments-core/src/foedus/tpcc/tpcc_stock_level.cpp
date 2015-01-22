/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_client.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>

#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace tpcc {
ErrorCode TpccClientTask::do_stock_level(Wid wid) {
  const Did did = get_random_district_id();
  Wdid wdid = combine_wdid(wid, did);
  const uint32_t threshold = rnd_.uniform_within(10, 20);

  // SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID=wid AND D_ID=did
  Oid next_oid;
  CHECK_ERROR_CODE(storages_.districts_next_oid_.get_record_primitive<Oid>(
    context_,
    wdid,
    &next_oid,
    0));

  // SELECT COUNT(DISTINCT(s_i_id))
  // FROM ORDERLINE INNER JOIN STOCK ON (WID,IID)
  // WHERE WID=wid AND DID=did AND OID BETWEEN next_oid-20 AND next_oid
  // AND QUANTITY<threshold
  uint32_t result = 0;

  // SELECT OL_I_ID FROM ORDERLINE
  // WHERE WID=wid AND DID=did AND OID BETWEEN oid_from AND oid_to.
  Wdol low = combine_wdol(combine_wdoid(wdid, next_oid - 20U), 0U);
  Wdol high = combine_wdol(combine_wdoid(wdid, next_oid + 1U), 0U);
  storage::masstree::MasstreeCursor cursor(storages_.orderlines_, context_);
  CHECK_ERROR_CODE(cursor.open_normalized(low, high));
  uint16_t s_offset = offsetof(StockData, quantity_);

  storage::array::ArrayOffset* sids = tmp_sids_;
  uint16_t read = 0;
  while (cursor.is_valid_record()) {
    ASSERT_ND(assorted::read_bigendian<Wdol>(cursor.get_key()) >= low);
    ASSERT_ND(assorted::read_bigendian<Wdol>(cursor.get_key()) < high);
    ASSERT_ND(cursor.get_key_length() == sizeof(Wdol));
    ASSERT_ND(cursor.get_payload_length() == sizeof(OrderlineData));

    const OrderlineData *ol_data = reinterpret_cast<const OrderlineData*>(cursor.get_payload());
    Iid iid = ol_data->iid_;

    sids[read] = combine_sid(wid, iid);
    ++read;
    CHECK_ERROR_CODE(cursor.next());
  }

  // hmm, this makes it slower. std::__introsort_loop() is really significant in cpu profile.
  // However, CacheHashTable::find_batch() etc does get faster instead. It's just not enough
  // to justify the sorting cost.
  //// sort sids before the search. this is called sorted-index-scan and used in many DBMS.
  //// it makes sense only when we have many sids.
  // if (read > 50U) {
  //   std::sort(sids, sids + read);
  // }

  uint32_t* quantities = tmp_quantities_;
  CHECK_ERROR_CODE(storages_.stocks_.get_record_primitive_batch<uint32_t>(
    context_,
    s_offset,
    read,
    sids,
    quantities));
  for (uint16_t i = 0; i < read; ++i) {
    if (quantities[i] < threshold) {
      ++result;
    }
  }

  DVLOG(2) << "Stock-Level: result=" << result;
  Epoch ep;
  return engine_->get_xct_manager()->precommit_xct(context_, &ep);
}
}  // namespace tpcc
}  // namespace foedus
