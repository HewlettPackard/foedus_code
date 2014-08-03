/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_TPCC_TPCC_LOAD_SCHEMA_HPP_
#define FOEDUS_TPCC_TPCC_LOAD_SCHEMA_HPP_

#include <stdint.h>

#include <cstring>

#include "foedus/compiler.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/storage/array/fwd.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/tpcc/tpcc_scale.hpp"

/**
 * @file tpcc_schema.hpp
 * @brief Definition of TPC-C schema.
 * @details
 * Acknowledgement:
 * Soem of the following source came from TpccOverBkDB by A. Fedorova:
 *   http://www.cs.sfu.ca/~fedorova/Teaching/CMPT886/Spring2007/benchtools.html
 *
 * All character arrays are 1 char longer than required to
 * account for the null character at the end of the string.
 * A few things have been changed to be more platform independent.
 */
namespace foedus {
namespace tpcc {

/** Packages all storages in TPC-C */
struct TpccStorages {
  /** (Wid, Did, Cid) == Wdcid */
  storage::array::ArrayStorage*           customers_;
  /** (Wid, Did, last, first, Cid) */
  storage::masstree::MasstreeStorage*     customers_secondary_;
  /** (Wid, Did) == Wdid */
  storage::array::ArrayStorage*           districts_;
  /** () */
  storage::sequential::SequentialStorage* histories_;
  /** (Wid, Did, Oid) == Wdoid */
  storage::masstree::MasstreeStorage*     neworders_;
  /** (Wid, Did, Oid) == Wdoid */
  storage::masstree::MasstreeStorage*     orders_;
  /** (Wid, Did, Cid, Oid) == Wdcoid */
  storage::masstree::MasstreeStorage*     orders_secondary_;
  /** (Wid, Did, Oid, Ol) == Wdol */
  storage::masstree::MasstreeStorage*     orderlines_;
  /** (Iid) */
  storage::array::ArrayStorage*           items_;
  /** (Wid, Iid) == Sid */
  storage::array::ArrayStorage*           stocks_;
  /** (Wid) */
  storage::array::ArrayStorage*           warehouses_;
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

inline Wdoid  combine_wdoid(Wdid wdid, Oid oid) { return static_cast<Wdoid>(wdid) * kOrders + oid; }
inline Wdid   extract_wdid_from_wdoid(Wdoid id) { return static_cast<Wdid>(id / kOrders); }
inline Oid    extract_oid_from_wdoid(Wdoid id) { return static_cast<Oid>(id % kOrders); }

/** Wdcid + oid (be aware of order) */
typedef uint64_t Wdcoid;

inline Wdcoid combine_wdcoid(Wdcid wdcid, Oid oid) { return wdcid * kOrders + oid; }
inline Wdcid  extract_wdcid_from_wdcoid(Wdcoid id) { return static_cast<Wdcid>(id / kOrders); }
inline Oid    extract_oid_from_wdcoid(Wdcoid id) { return static_cast<Oid>(id % kOrders); }

/** Orderline ordinal (1-25) */
typedef uint8_t Ol;

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

struct WarehouseData {
  char   name_[11];
  char   street1_[21];
  char   street2_[21];
  char   city_[21];
  char   state_[3];
  char   zip_[10];
  double tax_;
  double ytd_;
};

struct DistrictData {
  char   name_[11];
  char   street1_[21];
  char   street2_[21];
  char   city_[21];
  char   state_[3];
  char   zip_[10];
  double tax_;
  uint64_t ytd_;
  Oid    next_o_id_;
};

struct CustomerData {
  char   first_[17];
  char   middle_[3];
  char   last_[17];
  char   street1_[21];
  char   street2_[21];
  char   city_[21];
  char   state_[3];
  char   zip_[10];
  char   phone_[16];
  char   since_[26];
  char   credit_[3];
  double credit_lim_;
  double discount_;
  uint32_t payment_cnt_;
  uint32_t delivery_cnt_;
  uint64_t ytd_payment_;
  double balance_;
  char   data_[501];
};

/**
 * (wid, did, last, first, cid).
 * Key of secondary index for customer to allow lookup by last name.
 */
struct CustomerSecondaryKey {
  enum Constants {
    /** Length of the key. note that this doesn't contain padding as a struct. */
    kKeyLength = sizeof(Wid) + sizeof(Did) + 17 + 17 + sizeof(Cid),
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
  char      date_[26];
  double    amount_;
  char      data_[25];
};

struct OrderData {
  Cid       cid_;
  char      entry_d_[26];
  uint32_t  carrier_id_;
  char      ol_cnt_;
  char      all_local_;
};

struct OrderlineData {
  Iid     iid_;
  Wid     supply_wid_;
  char    delivery_d_[26];
  char    quantity_;
  double  amount_;
  char    dist_info_[25];
};

struct ItemData {
  uint32_t  im_id_;
  char      name_[25];
  uint32_t  price_;
  char      data_[51];
};

struct StockData {
  char dist_data_[10][25];
  uint64_t ytd_;
  uint32_t order_cnt_;
  uint32_t quantity_;
  uint32_t remote_cnt_;
  char data_[51];
};
}  // namespace tpcc
}  // namespace foedus

#endif  // FOEDUS_TPCC_TPCC_LOAD_SCHEMA_HPP_
