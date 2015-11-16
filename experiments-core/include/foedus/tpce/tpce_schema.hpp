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
#ifndef FOEDUS_TPCE_TPCE_LOAD_SCHEMA_HPP_
#define FOEDUS_TPCE_TPCE_LOAD_SCHEMA_HPP_

#include <stdint.h>

#include <cstring>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/tpce/tpce_scale.hpp"

/**
 * @file tpce_schema.hpp
 * @brief Definition of TPC-E schema.
 * @details
 * See Section 2 of the spec.
 */
namespace foedus {
namespace tpce {

/** Packages all storages in TPC-E */
struct TpceStorages {
  TpceStorages();

  void assert_initialized();
  bool has_snapshot_versions();
  void initialize_tables(Engine* engine);

  /**
   * TradeT as PK.
   * So far Hash because I don't see any case where we need
   * a range access on TradeT. If there is, we should use masstree.
   */
  storage::hash::HashStorage              trades_;
  /** (CA_ID,DTS,ID) on TRADE. Used in a cursor for OrderUpdate etc */
  storage::masstree::MasstreeStorage      trades_secondary_ca_dts_;
  /** Index in TRADE_TYPE has no meaning. Always TradeTypeData::kCount entries. */
  storage::array::ArrayStorage            trade_types_;
};

/// See Section 2.2.2 of the TPC-E spec.

/**
 * DATETIME represents the data type of date value
 * that includes a time component.
 * Our implementation uses Unix systime.
 */
typedef uint64_t Datetime;

/**
 * IDENT_T is defined as NUM(11) and is used to hold non-trade identifiers.
 */
typedef uint64_t IdentT;

/**
 * TRADE_T is defined as NUM(15) and is used to hold trade identifiers.
 */
typedef uint64_t TradeT;

/**
 * S_PRICE_T is defined as ENUM(8,2) and is used to hold the value of a share price.
 * Scaled up by 100 in our implementation.
 */
typedef uint32_t SPriceT;

/**
 * S_COUNT_T is defined as NUM(12) and is used to hold the aggregate
 * the count of shares.
 */
typedef uint64_t SCountT;

/**
 * S_QTY_T is defined as SNUM(6) and is used to hold the quantity
 * of shares per individual trade.
 */
typedef int32_t SQtyT;

/**
 * VALUE_T is defined as SENUM(12,2) and is used to hold non-aggragated
 * transaction and security related values such as cost, dividend, etc.
 * Scaled up by 100 in our implementation.
 */
typedef int64_t ValueT;

/** TRADE table, Section 2.2.5.6 */
struct TradeData {
  TradeT    id_;
  Datetime  dts_;
  char      st_id_[4];
  char      tt_id_[3];
  bool      is_cash_;
  char      s_symb_[15];
  SQtyT     qty_;
  SPriceT   bid_price_;
  IdentT    ca_id_;
  char      exec_name_[49];
  /**
   * This is the only nillable column in TPC-E's TRADE table.
   * Our implementation uses 0 as null for this column.
   */
  SPriceT   trade_price_;
  ValueT    chrg_;
  ValueT    comm_;
  ValueT    tax_;
  bool      lifo_;
};

/**
 * Key for the secondary index TRADE(CA_ID,DTS).
 * The last TradeT is the uniqufier and the data.
 * So, this secondary index has only keys because
 * we only need to know TradeT.
 */
struct TradeSecondaryKeyCaDts {
  IdentT    ca_id_;
  Datetime  dts_;
  TradeT    id_;
};

/** TRADE_TYPE table, Section 2.2.5.9 */
struct TradeTypeData {
  /** Indexes in trade_types_ */
  enum Indexes {
    kTlb = 0,
    kTls,
    kTmb,
    kTms,
    kTsl,
    kCount,
  };
  char   id_[3];
  char   name_[12];
  bool   is_sell_;
  bool   is_mrkt_;
  // ah, these two make it 17 bytes. if we compact the two bools to
  // 1 byte (some DBMS does it), it will be 16 byte and save many things.
  // but for now I don't care...
};


}  // namespace tpce
}  // namespace foedus

#endif  // FOEDUS_TPCE_TPCE_LOAD_SCHEMA_HPP_
