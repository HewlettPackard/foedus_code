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
#ifndef FOEDUS_TPCC_TPCC_LOAD_SCHEMA_HPP_
#define FOEDUS_TPCC_TPCC_LOAD_SCHEMA_HPP_

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
#include "foedus/tpcc/tpcc_scale.hpp"

/**
 * @file tpcc_schema.hpp
 * @brief Definition of TPC-C schema.
 * @details
 * Acknowledgement:
 * Soem of the following source came from TpccOverBkDB by A. Fedorova:
 *   http://www.cs.sfu.ca/~fedorova/Teaching/CMPT886/Spring2007/benchtools.html
 *
 * A few things have been changed to be more platform independent.
 */
namespace foedus {
namespace tpcc {

/** Packages all storages in TPC-C */
struct TpccStorages {
  TpccStorages();


  void assert_initialized();
  bool has_snapshot_versions();
  void initialize_tables(Engine* engine);

  /** (Wid, Did, Cid) == Wdcid */
  storage::array::ArrayStorage            customers_static_;
  storage::array::ArrayStorage            customers_dynamic_;
  storage::array::ArrayStorage            customers_history_;
  /** (Wid, Did, last, first, Cid) */
  storage::masstree::MasstreeStorage      customers_secondary_;
  /**
   * (Wid, Did) == Wdid. ytd/next_oid are vertically partitioned. others are static.
   * Vertical partitioning is allowed (Section 1.4.5).
   */
  storage::array::ArrayStorage            districts_static_;
  storage::array::ArrayStorage            districts_ytd_;
  storage::array::ArrayStorage            districts_next_oid_;
  /** () */
  storage::sequential::SequentialStorage  histories_;
  /** (Wid, Did, Oid) == Wdoid */
  storage::masstree::MasstreeStorage      neworders_;
  /** (Wid, Did, Oid) == Wdoid */
  storage::masstree::MasstreeStorage      orders_;
  /** (Wid, Did, Cid, Oid) == Wdcoid */
  storage::masstree::MasstreeStorage      orders_secondary_;
  /** (Wid, Did, Oid, Ol) == Wdol */
  storage::masstree::MasstreeStorage      orderlines_;
  /** (Iid) */
  storage::array::ArrayStorage            items_;
  /** (Wid, Iid) == Sid */
  storage::array::ArrayStorage            stocks_;
  /** (Wid). ytd is vertically partitioned. others are static */
  storage::array::ArrayStorage            warehouses_static_;
  storage::array::ArrayStorage            warehouses_ytd_;
};


/** Warehouse ID */
typedef uint16_t Wid;

/** District ID (not unique across warehouses. Wid+Did is district's unique ID) */
typedef uint8_t Did;

/** Wid and Did combined (Wid occupies more significant bits)*/
typedef uint32_t Wdid;

inline Wdid combine_wdid(Wid wid, Did did) { return static_cast<Wdid>(wid) * kDistricts + did; }
inline Wid  extract_wid_from_wdid(Wdid id) { return static_cast<Wid>(id / kDistricts); }
inline Did  extract_did_from_wdid(Wdid id) { return static_cast<Did>(id % kDistricts); }

/** Customer ID (not unique across districts. Wdid+Cid is customer's unique ID) */
typedef uint32_t Cid;

/** Wdid and Cid combined (Widd occupies more significant bits)*/
typedef uint64_t Wdcid;

inline Wdcid  combine_wdcid(Wdid wdid, Cid cid) {
  return static_cast<Wdcid>(wdid) * kCustomers + cid;
}
inline Wdid   extract_wdid_from_wdcid(Wdcid id) { return static_cast<Wdid>(id / kCustomers); }
inline Cid    extract_cid_from_wdcid(Wdcid id) { return static_cast<Cid>(id % kCustomers); }

/**
 * Order ID.
 *  (not unique across districts. Wdid+Oid is order's unique ID)
 * @todo this might have to be 64 bit later
 */
typedef uint32_t Oid;

/** Wdid and Oid combined (Wdid occupies more significant bits)*/
typedef uint64_t Wdoid;

inline Wdoid  combine_wdoid(Wdid wdid, Oid oid) {
  return static_cast<Wdoid>(wdid) * kMaxOrders + oid;
}
inline Wdid   extract_wdid_from_wdoid(Wdoid id) { return static_cast<Wdid>(id / kMaxOrders); }
inline Oid    extract_oid_from_wdoid(Wdoid id) { return static_cast<Oid>(id % kMaxOrders); }

/** Wdcid + oid (be aware of order) */
typedef uint64_t Wdcoid;

inline Wdcoid combine_wdcoid(Wdcid wdcid, Oid oid) { return wdcid * kMaxOrders + oid; }
inline Wdcid  extract_wdcid_from_wdcoid(Wdcoid id) { return static_cast<Wdcid>(id / kMaxOrders); }
inline Oid    extract_oid_from_wdcoid(Wdcoid id) { return static_cast<Oid>(id % kMaxOrders); }

/** Orderline ordinal (1-25) */
typedef uint16_t Ol;

/** Wdoid and Ol combined */
typedef uint64_t Wdol;

inline Wdol  combine_wdol(Wdoid wdoid, Ol ol) { return wdoid * kOlMax + ol; }
inline Wdid  extract_wdid_from_wdol(Wdol id) { return static_cast<Wdid>(id / kOlMax); }
inline Ol    extract_ol_from_wdol(Wdol id) { return static_cast<Oid>(id % kOlMax); }

/**
 * Item ID.
 */
typedef uint32_t Iid;

/**
 * Stock ID, which is Wid + Iid.
 */
typedef uint64_t Sid;

inline Sid  combine_sid(Wid wid, Iid iid) { return static_cast<Sid>(wid) * kItems + iid; }
inline Wid  extract_wid_from_sid(Sid id) { return static_cast<Wid>(id / kItems); }
inline Iid  extract_iid_from_sid(Sid id) { return static_cast<Iid>(id % kItems); }

struct WarehouseStaticData {
  char   name_[10];
  char   street1_[20];
  char   street2_[20];
  char   city_[20];
  char   state_[2];
  char   zip_[9];
  double tax_;
// Following is vertically partitioned
//   double ytd_;
};

struct WarehouseYtdData {
  double ytd_;          // +8 -> 8
  char   dummy_[40];    // +40 -> 48
  // with kRecordOverhead (16 bytes), this is 64 bytes. good for avoiding false sharing
  char   dummy2_[64];   // another padding in case of adjacent cacheline prefetch enabled
  char   dummy3_[1920];  // even further. let's make this one record per page. it's a small table.
};

struct DistrictStaticData {
  char   name_[10];
  char   street1_[20];
  char   street2_[20];
  char   city_[20];
  char   state_[2];
  char   zip_[9];
  double tax_;
// Followings are vertically partitioned
//  uint64_t ytd_;
//  Oid      next_o_id_;
};

struct DistrictYtdData {
  double    ytd_;          // +8 -> 8
  char      dummy_[40];    // +40 -> 48
  // with kRecordOverhead (16 bytes), this is 64 bytes. good for avoiding false sharing
  char      dummy2_[64];   // another padding in case of adjacent cacheline prefetch enabled
  char      dummy3_[128];  // evne more in case it prefetches 4 cacheline
};
STATIC_SIZE_CHECK(sizeof(DistrictYtdData), 256 - storage::kRecordOverhead)

struct DistrictNextOidData {
  Oid       next_o_id_;    // +4 -> 4
  uint32_t  dummy1_;       // +4 -> 8
  char      dummy2_[40];   // +40 -> 48
  // with kRecordOverhead (16 bytes), this is 64 bytes. good for avoiding false sharing
  char      dummy3_[64];   // another padding in case of adjacent cacheline prefetch enabled
  char      dummy4_[128];  // evne more in case it prefetches 4 cacheline
};
STATIC_SIZE_CHECK(sizeof(DistrictNextOidData), 256 - storage::kRecordOverhead)

struct CustomerStaticData {
  char   first_[16];
  char   middle_[2];
  char   last_[16];
  char   street1_[20];
  char   street2_[20];
  char   city_[20];
  char   state_[2];
  char   zip_[9];
  char   phone_[15];
  char   since_[25];
  char   credit_[2];
  double credit_lim_;
  double discount_;
  enum Constants {
    kHistoryDataLength = 500,
  };
  // char   data_[500];  vertically partitioned as customer_history
};

struct CustomerDynamicData {
  uint32_t  payment_cnt_;   // +4->4
  uint32_t  delivery_cnt_;  // +4->8
  uint64_t  ytd_payment_;   // +8->16
  double    balance_;       // +8->24
  char      dummy_[24];     // +24->48
  // with kRecordOverhead (16 bytes), this is 64 bytes. good for avoiding false sharing
  // customer is many-valued, so probably one cacheline is enough..
};
STATIC_SIZE_CHECK(sizeof(DistrictNextOidData), 256 - storage::kRecordOverhead)

/**
 * (wid, did, last, first, cid).
 * Key of secondary index for customer to allow lookup by last name.
 */
struct CustomerSecondaryKey {
  enum Constants {
    /** Length of the key. note that this doesn't contain padding as a struct. */
    kKeyLength = sizeof(Wid) + sizeof(Did) + 16 + 16 + sizeof(Cid),
  };
};

/**
 * NOTE unlike the original implementation, these are data, not key.
 */
struct HistoryData {
  Cid       cid_;
  Wid       c_wid_;
  Did       c_did_;
  Wid       wid_;
  Did       did_;
  char      date_[25];
  double    amount_;
  char      data_[24];
};

struct OrderData {
  Cid       cid_;         // +4 -> 4
  uint32_t  carrier_id_;  // +4 -> 8
  char      entry_d_[25];  // +25 -> 33
  char      ol_cnt_;      // +1 -> 34
  char      all_local_;   // +1 -> 35
};

struct OrderlineData {
  Iid     iid_;           // +4 -> 4
  Wid     supply_wid_;    // +2 -> 6
  char    delivery_d_[25];  // +25 -> 31
  char    quantity_;        // +1 -> 32
  char    dist_info_[24];   // +24 -> 56
  double  amount_;          // +8 -> 64
};
STATIC_SIZE_CHECK(sizeof(OrderlineData), 64)

struct ItemData {
  uint32_t  im_id_;     // +4 -> 4
  uint32_t  price_;     // +4 -> 8
  char      name_[24];  // +24 -> 32
  char      data_[50];  // +50 -> 82
};

struct StockData {
  uint64_t ytd_;        // +8 -> 8
  uint32_t order_cnt_;  // +4 -> 12
  uint32_t quantity_;   // +4 -> 16
  uint32_t remote_cnt_;   // +4 -> 20
  char dist_data_[10][24];  // +240 -> 260
  char data_[50];  // +50 -> 310
};
}  // namespace tpcc
}  // namespace foedus

#endif  // FOEDUS_TPCC_TPCC_LOAD_SCHEMA_HPP_
