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

#include <ctime>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/fwd.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"

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
  void initialize_tables(Engine* engine);

  /**
   * TradeT as PK.
   * So far Hash because I don't see any case where we need
   * a range access on TradeT. If there is, we should use masstree.
   */
  storage::hash::HashStorage              trades_;
  /**
   * Index(SYMB_ID,DTS) on TRADE. Key is SymbDtsKey, Value is TradeT.
   * Used in a cursor for OrderUpdate etc.
   */
  storage::masstree::MasstreeStorage      trades_secondary_symb_dts_;
  /** Index in TRADE_TYPE has no meaning. Always TradeTypeData::kCount entries. */
  storage::array::ArrayStorage            trade_types_;
};

/// See Section 2.2.2 of the TPC-E spec.

/**
 * DATETIME represents the data type of date value
 * that includes a time component with granulality in seconds.
 * Our implementation so far just uses std::time_t.
 */
typedef uint32_t Datetime;

inline Datetime get_current_datetime() {
  return static_cast<Datetime>(std::time(nullptr));
}

inline std::string to_datetime_string(Datetime value) {
  std::time_t converted = static_cast<std::time_t>(value);
  // Yes, this function must not be called in a racy place due to this.
  std::tm * ptm = std::gmtime(&converted);  // NOLINT(runtime/threadsafe_fn)
  // No clean solution in std, though ("_r" thingy are not standard yet).
  // Did you drink enough coffee, C++ committees?
  // http://stackoverflow.com/questions/25618702/why-is-there-no-c11-threadsafe-alternative-to-stdlocaltime-and-stdgmtime
  char buffer[64];
  int written = std::strftime(buffer, 64, "%Y/%m/%d %H:%M:%S GMT", ptm);
  return std::string(buffer, written);
}

/**
 * IDENT_T is defined as NUM(11) and is used to hold non-trade identifiers.
 */
typedef uint32_t IdentT;

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

/**
 * This type is specific to our implementation.
 * This denotes the \e partition either of the work or of the data.
 * Each worker/loader is assigned to one partition to exploit locality.
 * However, in TPC-E it is not clear what the partition should be based on.
 * We so far ignore it and every worker touches all data randomly.
 * With a work-scheduler, we might exploit this later.
 */
typedef uint32_t PartitionT;

/**
 * This is a drastic simplification from full TPC-E.
 * We don't have SECURITY table yet. so we can't query on symbols.
 * Instead, we just use an integer to represent s_symb_.
 * The value is 0 to TpceScale::get_security_cardinality() - 1.
 * @see TpceScale::get_security_cardinality()
 */
typedef uint32_t SymbT;
/**
 * we also assume SymbT consumes up to 20 bits.
 * This means we so far support tpsE up to 2^20 / (685/2)=3061.
 * It should be plenty.
 */
const SymbT kMaxSymbT = 1U << 20;

/**
 * The spec says CUSTOMER_ACCOUNT's cardinality is always
 * 5 * customers. Our implementation thus constructs
 * Customer Account (CA_ID) as CID * 5 + [0,5).
 */
const IdentT kAccountsPerCustomer = 5;

/**
 * Parameters to determine the size of TPC-E tables.
 * See Section 2.6.
 */
struct TpceScale {
  /**
   * @see PartitionT
   */
  PartitionT total_partitions_;
  /**
   * The number of customers, or Scale Factor * tpsE.
   * The Scale Factor (SF) is the number of required customer rows per
   * single tpsE. SF for Nominal Throughput is 500.
   * For example, for a database size of 5000 customers,
   * the nominal performance is 10.00 tpsE.
   * The TPC-E spec also defines that the minimal # of customers is 5000,
   * so tpcE must be 10 or larger. The spec also specifies that this
   * number must be a multiply of 1000 (Load Unit).
   */
  uint64_t customers_;
  /**
   * The Initial Trade Days (ITD) is the number of Business Days used to
   * populate the database. This population is made of trade data
   * that would be generated by the SUT when running at the
   * Nominal Throughput for the specified number of Business Days.
   * ITD for Nominal Throughput is 300.
   */
  uint64_t initial_trade_days_;

  /**
   * Skewness to pick a security symbol
   * for both trade-order (insert) and other references.
   * 0 means uniform. Higher value causes higher skew, skewing to lower symbol IDs.
   */
  double symbol_skew_;

  uint64_t get_tpse() const {
    return customers_ / 500U;
  }

  uint64_t calculate_initial_trade_cardinality() const {
    return initial_trade_days_ * 8ULL * 36ULL * customers_ / 5U;
  }

  uint64_t get_security_cardinality() const {
    return 685U * customers_ / 1000U;
  }
};

/** TRADE table, Section 2.2.5.6 */
struct TradeData {
  TradeT    id_;
  Datetime  dts_;
  char      st_id_[4];
  char      tt_id_[3];
  bool      is_cash_;
  // char      s_symb_[15];
  /**
   * Instead of s_symb_.
   * @see SymbT
   */
  SymbT     symb_id_;
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

inline IdentT to_cid_from_ca(IdentT ca) {
  return ca / kAccountsPerCustomer;
}
inline IdentT to_ordinal_from_ca(IdentT ca) {
  return ca % kAccountsPerCustomer;
}
inline IdentT to_ca(IdentT cid, IdentT ordinal) {
  ASSERT_ND(ordinal < kAccountsPerCustomer);
  return cid * kAccountsPerCustomer + ordinal;
}

/**
 * Composite Key for the secondary index TRADE(SYMB_ID,DTS).
 * High 20-bits are SYMB_ID, next 32-bits are DTS, then
 * the last 12 bits are partition_id just as a uniquefier.
 */
typedef uint64_t SymbDtsKey;

inline SymbDtsKey to_symb_dts_key(SymbT symb_id, Datetime dts, PartitionT partition_id) {
  SymbDtsKey ret = static_cast<SymbDtsKey>(symb_id);
  ret = (ret << 32) | dts;
  ret = (ret << 12) | partition_id;
  return ret;
}
inline SymbT to_symb_from_symb_dts_key(SymbDtsKey key) {
  return static_cast<SymbT>(key >> 44);
}
inline Datetime to_dts_from_symb_dts_key(SymbDtsKey key) {
  return static_cast<Datetime>(key >> 12);
}
inline PartitionT to_uniquefier_from_symb_dts_key(SymbDtsKey key) {
  return static_cast<PartitionT>(key & ((1U << 12) - 1U));
}


/**
 * @brief generates a new and unique TradeT
 * @details
 * TPC-E spec explicitly prohibits correlating trade ID with customer ID
 * or any other partitionable data.
 * To satisfy the requirement without worrying about duplicate keys,
 * high bits use a unique counter in each partition and low bits use partition ID.
 * This way, we guarantee there are no correlations between trade IDs and
 * partitions.
 *
 * We don't want to make this function a bottleneck, eg atomic increment
 * among \e all threads. TradeT anyway has no meaning in its value,
 * so this simple value generator suffices.
 */
inline TradeT get_new_trade_id(
  const TpceScale& scale,
  PartitionT partition_id,
  uint64_t in_partition_count) {
  TradeT tid = in_partition_count * scale.total_partitions_ + partition_id;
  return tid;
}

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

  static const char* generate_type_id(uint16_t index) {
    switch (index) {
      case TradeTypeData::kTlb:
        return "TLB";
      case TradeTypeData::kTls:
        return "TLS";
      case TradeTypeData::kTmb:
        return "TMB";
      case TradeTypeData::kTms:
        return "TMS";
      default:
        return "TSL";
    }
  }
};

}  // namespace tpce
}  // namespace foedus

#endif  // FOEDUS_TPCE_TPCE_LOAD_SCHEMA_HPP_
