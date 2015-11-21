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
#include "foedus/tpce/tpce_client.hpp"

#include <cstring>

#include "foedus/engine.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace tpce {

struct TradeUpdateOutput {
  TradeT    id_;
  IdentT    acct_id_;
  char      exec_name_[49];
  bool      is_cash_;
  SQtyT     qty_;
  SPriceT   price_;
  SymbT     symb_id_;
  Datetime  trade_dts_;
  char      trade_type_[3];
  char      type_name_[12];
};

ErrorCode TpceClientTask::do_trade_update() {
  auto trades = storages_.trades_;
  auto trades_index = storages_.trades_secondary_symb_dts_;
  auto trade_types = storages_.trade_types_;

  // Frame-3
  // We fix most input values as below.
  const uint32_t in_max_trades = 20;
  // const uint32_t in_max_updates = 20;  // not used in our impl
  const Datetime in_start_trade_dts = artificial_cur_dts_ - 10U;
  const Datetime in_end_trade_dts = artificial_cur_dts_;

  // Input "symbol" is an ID in our implementation
  const SymbT symbol = zipfian_symbol_.next();
  ASSERT_ND(symbol < scale_.get_security_cardinality());
  // Input "max_attr_id" is not used in the spec

  // Join TRADE with TRADE_TYPE.
  // As TRADE_TYPE has only 5 rows, we anyway scan all
  // TRADE_TYPE records and join (in other words, NLJ without index).
  // Let's scan TRADE_TYPE first.
  TradeTypeData tt_records[TradeTypeData::kCount];
  for (uint32_t i = 0; i < TradeTypeData::kCount; ++i) {
    CHECK_ERROR_CODE(trade_types.get_record(context_, i, tt_records + i));
    // We could batch this part, but there are only 5 records.
    // probably doesn't matter.
  }

  // Second, query the secondary index on TRADE to get list of TIDs
  const SymbDtsKey from_key = to_symb_dts_key(symbol, in_start_trade_dts, 0);
  const SymbDtsKey to_key = to_symb_dts_key(symbol, in_end_trade_dts + 1U, 0);
  // +1 because the end is also inclusive.

  storage::masstree::MasstreeCursor cursor(trades_index, context_);
  uint32_t fetched_rows = 0;
  TradeUpdateOutput outputs[20];
  CHECK_ERROR_CODE(cursor.open_normalized(from_key, to_key));
  while (cursor.is_valid_record()) {
    ASSERT_ND(cursor.get_key_length() == sizeof(SymbDtsKey));

#ifndef NDEBUG
    SymbDtsKey key = cursor.get_normalized_key();
    SymbT extracted_symbol = to_symb_from_symb_dts_key(key);
    Datetime extracted_dts = to_dts_from_symb_dts_key(key);
    ASSERT_ND(extracted_symbol == symbol);
    ASSERT_ND(extracted_dts >= in_start_trade_dts);
    ASSERT_ND(extracted_dts <= in_end_trade_dts);
#endif  // NDEBUG

    ASSERT_ND(cursor.get_payload_length() == sizeof(TradeT));
    outputs[fetched_rows].id_ = *reinterpret_cast<const TradeT*>(cursor.get_payload());
    ++fetched_rows;
    if (fetched_rows >= in_max_trades) {
      break;
    }
  }

  // Finally, query them in TRADE's primary storage
  for (uint32_t i = 0; i < fetched_rows; ++i) {
    TradeT key = outputs[i].id_;
    TradeData payload;
    uint16_t payload_capacity = sizeof(payload);
    CHECK_ERROR_CODE(trades.get_record<TradeT>(context_, key, &payload, &payload_capacity));
    outputs[fetched_rows].acct_id_ = payload.ca_id_;
    std::memcpy(outputs[fetched_rows].exec_name_, payload.exec_name_, sizeof(payload.exec_name_));
    outputs[fetched_rows].is_cash_ = payload.is_cash_;
    outputs[fetched_rows].qty_ = payload.qty_;
    outputs[fetched_rows].price_ = payload.trade_price_;
    outputs[fetched_rows].symb_id_ = payload.symb_id_;
    ASSERT_ND(payload.symb_id_ == symbol);
    outputs[fetched_rows].trade_dts_ = payload.dts_;
    ASSERT_ND(payload.dts_ >= in_start_trade_dts);
    ASSERT_ND(payload.dts_ <= in_end_trade_dts);
    outputs[fetched_rows].trade_dts_ = payload.dts_;
    std::memcpy(outputs[fetched_rows].trade_type_, payload.tt_id_, sizeof(payload.tt_id_));

    // NLJ without index. Just iterate through
    bool found = false;
    for (uint32_t j = 0; j < TradeTypeData::kCount; ++j) {
      if (std::memcmp(tt_records[j].id_, payload.tt_id_, sizeof(payload.tt_id_)) == 0) {
        std::memcpy(
          outputs[fetched_rows].type_name_,
          tt_records[j].name_,
          sizeof(tt_records[j].name_));
        found = true;
        break;
      }
    }
    ASSERT_ND(found);
  }

  Epoch ep;
  return engine_->get_xct_manager()->precommit_xct(context_, &ep);
}

}  // namespace tpce
}  // namespace foedus
