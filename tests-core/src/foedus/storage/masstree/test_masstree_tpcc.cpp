/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <stdint.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_access.hpp"
#include "foedus/xct/xct_manager.hpp"

/**
 * @file test_masstree_tpcc.cpp
 * TPCC tables as Masstree.
 */
namespace foedus {
namespace storage {
namespace masstree {
DEFINE_TEST_CASE_PACKAGE(MasstreeTpccTest, foedus.storage.masstree);

// Below are copied from experiment's tpcc_schema/scale.h

/** Number of warehouses. Does not grow dynamically */
const uint16_t kWarehouses = 2U;

/** Number of districts per warehouse. Does not grow dynamically  */
const uint8_t kDistricts = 2U;

/** Number of items per warehouse. Does not grow dynamically  */
const uint32_t kItems = 100000U;

/** Number of customers per district. Does not grow dynamically  */
const uint32_t kCustomers = 3000U;

/** Number of orders per district. Does grow dynamically. */
const uint32_t kOrders = 3000U;
/** Max number of orders per district */
const uint32_t kMaxOrders = 1U << 31;

/** Number of variations of last names. Does not grow dynamically. */
const uint32_t kLnames = 1000U;

const uint8_t kMinOlCount = 5U;
const uint8_t kMaxOlCount = 15U;
const uint8_t kOlMax = kMaxOlCount + 1U;

/** Packages all storages in TPC-C */
struct TpccStorages {
  /** (Wid, Did, last, first, Cid) */
  MasstreeStorage      customers_secondary_;
  uint32_t             customers_secondary_count_;
  /** (Wid, Did, Oid) == Wdoid */
  MasstreeStorage      neworders_;
  uint32_t             neworders_count_;
  /** (Wid, Did, Oid) == Wdoid */
  MasstreeStorage      orders_;
  uint32_t             orders_count_;
  /** (Wid, Did, Cid, Oid) == Wdcoid */
  MasstreeStorage      orders_secondary_;
  uint32_t             orders_secondary_count_;
  /** (Wid, Did, Oid, Ol) == Wdol */
  MasstreeStorage      orderlines_;
  uint32_t             orderlines_count_;
};
/** Set in TpccLoadTask::run() */
TpccStorages storages;

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

inline void generate_lastname(uint32_t num, char *name) {
  const char *n[] = {
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
  };
  const uint8_t l[] = {3, 5, 4, 3, 4, 3, 4, 5, 5, 4};

  uint8_t len = 0;
  for (int i = 0; i < 3; ++i) {
    uint16_t choice = i == 0 ? num % 10 : (i == 1 ? (num / 10) % 10 : (num / 100) % 10);
    ASSERT_ND(choice < 10U);
    ASSERT_ND(len + l[choice] <= 17U);
    std::memcpy(name + len, n[choice], l[choice]);
    len += l[choice];
  }

  // to make sure, fill out _all_ remaining part with NULL character.
  std::memset(name + len, 0, 17 - len);
}

template <typename T>
inline void zero_clear(T* data) {
  std::memset(data, 0, sizeof(T));
}

bool load_customers_secondary;
bool load_neworders;
bool load_orders;
bool load_orders_secondary;
bool load_orderlines;

class TpccLoadTask {
 public:
  TpccLoadTask();
  ErrorStack          run(thread::Thread* context);

  ErrorStack          load_tables();

  enum Constants {
    kCommitBatch = 500,
  };

  Engine* engine_;
  thread::Thread* context_;
  xct::XctManager* xct_manager_;
  /** timestamp for date fields. */
  char* timestamp_;

  assorted::UniformRandom rnd_;

  void      random_orig(bool *orig);

  Cid       get_permutation(bool* cid_array);

  ErrorStack create_tables();
  ErrorStack create_masstree(const storage::StorageName& name, MasstreeStorage* storage);

  ErrorCode  commit_if_full();

  /** Loads the Customer Table */
  ErrorStack load_customers();

  /**
  * Loads Customer Table.
  * Also inserts corresponding history record.
  * @param[in] wid warehouse id
  * @param[in] did district id
  */
  ErrorStack load_customers_in_district(Wid wid, Did did);

  /** Loads the Orders and Order_Line Tables */
  ErrorStack load_orders_data();

  /**
  *  Loads the Orders table.
  *  Also loads the orderLine table on the fly.
  *  @param[in] w_id warehouse id
  *  @param[in] d_id district id
  */
  ErrorStack load_orders_in_district(Wid wid, Did did);

  /** Make a string of letter */
  int32_t    make_alpha_string(int32_t min, int32_t max, char *str);
};
TpccLoadTask::TpccLoadTask() {
  // Initialize timestamp (for date columns)
  time_t t_clock;
  ::time(&t_clock);
  timestamp_ = ::ctime(&t_clock);  // NOLINT(runtime/threadsafe_fn) no race here
  ASSERT_ND(timestamp_);
}

ErrorStack TpccLoadTask::run(thread::Thread* context) {
  context_ = context;
  engine_ = context->get_engine();
  engine_->get_debug()->set_debug_log_min_threshold(debugging::DebuggingOptions::kDebugLogWarning);
  xct_manager_ = engine_->get_xct_manager();
  debugging::StopWatch watch;
  CHECK_ERROR(load_tables());
  watch.stop();
  LOG(INFO) << "Loaded TPC-C tables in " << watch.elapsed_sec() << "sec";
  engine_->get_debug()->set_debug_log_min_threshold(debugging::DebuggingOptions::kDebugLogInfo);
  return kRetOk;
}

ErrorStack TpccLoadTask::load_tables() {
  CHECK_ERROR(create_tables());

  CHECK_ERROR(load_customers());
  CHECK_ERROR(load_orders_data());

  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  CHECK_ERROR(storages.customers_secondary_.verify_single_thread(context_));
  CHECK_ERROR(storages.neworders_.verify_single_thread(context_));
  CHECK_ERROR(storages.orderlines_.verify_single_thread(context_));
  CHECK_ERROR(storages.orders_.verify_single_thread(context_));
  CHECK_ERROR(storages.orders_secondary_.verify_single_thread(context_));
  WRAP_ERROR_CODE(xct_manager_->abort_xct(context_));
  LOG(INFO) << "Loaded all:" << engine_->get_memory_manager()->dump_free_memory_stat();
  return kRetOk;
}

ErrorStack TpccLoadTask::create_tables() {
  std::memset(&storages, 0, sizeof(storages));

  LOG(INFO) << "Initial:" << engine_->get_memory_manager()->dump_free_memory_stat();
  CHECK_ERROR(create_masstree("customers_secondary", &storages.customers_secondary_));
  CHECK_ERROR(create_masstree("neworders", &storages.neworders_));
  CHECK_ERROR(create_masstree("orders", &storages.orders_));
  CHECK_ERROR(create_masstree("orders_secondary", &storages.orders_secondary_));
  CHECK_ERROR(create_masstree("orderlines", &storages.orderlines_));

  return kRetOk;
}

ErrorStack TpccLoadTask::create_masstree(
  const storage::StorageName& name,
  MasstreeStorage* storage) {
  Epoch ep;
  storage::masstree::MasstreeMetadata meta(name);
  EXPECT_FALSE(storage->exists());
  CHECK_ERROR(engine_->get_storage_manager()->create_masstree(&meta, storage, &ep));
  EXPECT_TRUE(storage->exists());
  return kRetOk;
}

ErrorCode TpccLoadTask::commit_if_full() {
  if (context_->get_current_xct().get_write_set_size() >= kCommitBatch) {
    Epoch commit_epoch;
    CHECK_ERROR_CODE(xct_manager_->precommit_xct(context_, &commit_epoch));
    CHECK_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kDirtyReadPreferVolatile));
  }
  return kErrorCodeOk;
}

ErrorStack TpccLoadTask::load_customers() {
  if (!load_customers_secondary) {
    return kRetOk;
  }
  for (Wid wid = 0; wid < kWarehouses; ++wid) {
    for (Did did = 0; did < kDistricts; ++did) {
      CHECK_ERROR(load_customers_in_district(wid, did));
    }
  }
  return kRetOk;
}

ErrorStack TpccLoadTask::load_customers_in_district(Wid wid, Did did) {
  LOG(INFO) << "Loading Customer for DID=" << static_cast<int>(did) << ", WID=" << wid
    << ": " << engine_->get_memory_manager()->dump_free_memory_stat();

  // insert to customers_secondary at the end after sorting
  memory::AlignedMemory secondary_keys_buffer;
  struct Secondary {
    char  last_[17];      // +17 -> 17
    char  first_[17];     // +17 -> 34
    char  padding_[2];    // +2 -> 36
    Cid   cid_;           // +4 -> 40
    static bool compare(const Secondary &left, const Secondary& right) ALWAYS_INLINE {
      int cmp = std::memcmp(left.last_, right.last_, sizeof(left.last_));
      if (cmp < 0) {
        return true;
      } else if (cmp > 0) {
        return false;
      }
      cmp = std::memcmp(left.first_, right.first_, sizeof(left.first_));
      if (cmp < 0) {
        return true;
      } else if (cmp > 0) {
        return false;
      }
        ASSERT_ND(left.cid_ != right.cid_);
      if (left.cid_ < right.cid_) {
        return true;
      } else {
        return false;
      }
    }
  };
  secondary_keys_buffer.alloc(
    kCustomers * sizeof(Secondary),
    1U << 21,
    memory::AlignedMemory::kNumaAllocOnnode,
    context_->get_numa_node());
  Secondary* secondary_keys = reinterpret_cast<Secondary*>(secondary_keys_buffer.get_block());
  Epoch ep;
  for (Cid cid = 0; cid < kCustomers; ++cid) {
    make_alpha_string(8, 16, secondary_keys[cid].first_);
    if (cid < kLnames) {
      generate_lastname(cid, secondary_keys[cid].last_);
    } else {
      generate_lastname(rnd_.non_uniform_within(255, 0, kLnames - 1), secondary_keys[cid].last_);
    }
    secondary_keys[cid].cid_ = cid;
  }

  // now insert all secondary keys.
  // by sorting them here, we get better insert performance and fill factor.
  debugging::StopWatch sort_watch;
  std::sort(secondary_keys, secondary_keys + kCustomers, Secondary::compare);
  sort_watch.stop();
  LOG(INFO) << "Sorted secondary entries in " << sort_watch.elapsed_us() << "us";
  MasstreeStorage customers_secondary = storages.customers_secondary_;
  for (Cid from = 0; from < kCustomers;) {
    uint32_t cur_batch_size = std::min<uint32_t>(kCommitBatch, kCustomers - from);
    char key_be[CustomerSecondaryKey::kKeyLength];
    assorted::write_bigendian<Wid>(wid, key_be);
    assorted::write_bigendian<Did>(did, key_be + sizeof(Wid));
    // An easy optimization for batched inserts. Trigger reserve_record for all of them,
    // then abort and do it as a fresh transaction so that no moved-bit tracking is required.
    for (int rep = 0; rep < 2; ++rep) {
      WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
      for (Cid i = from; i < from + cur_batch_size; ++i) {
        std::memcpy(key_be + sizeof(Wid) + sizeof(Did), secondary_keys[i].last_, 34);
        // note: this one might not be aligned
        Cid* address = reinterpret_cast<Cid*>(key_be + sizeof(Wid) + sizeof(Did) + 34);
        *address = assorted::htobe<Cid>(secondary_keys[i].cid_);
        WRAP_ERROR_CODE(customers_secondary.insert_record(context_, key_be, sizeof(key_be)));
      }
      if (rep == 0) {
        WRAP_ERROR_CODE(xct_manager_->abort_xct(context_));
      } else {
        WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &ep));
      }
    }
    from += cur_batch_size;
  }
  storages.customers_secondary_count_ += kCustomers;
  return kRetOk;
}

ErrorStack TpccLoadTask::load_orders_data() {
  if (!load_neworders && !load_orders && !load_orders_secondary && !load_orderlines) {
    return kRetOk;
  }
  for (Wid wid = 0; wid < kWarehouses; ++wid) {
    for (Did did = 0; did < kDistricts; ++did) {
      CHECK_ERROR(load_orders_in_district(wid, did));
    }
  }
  return kRetOk;
}

ErrorStack TpccLoadTask::load_orders_in_district(Wid wid, Did did) {
  LOG(INFO) << "Loading Orders for D=" << static_cast<int>(did) << ", W= " << wid
    << ": " << engine_->get_memory_manager()->dump_free_memory_stat();
  // Whether the customer id for the current order is already taken.
  bool cid_array[kCustomers];
  std::memset(cid_array, 0, sizeof(cid_array));

  Epoch ep;
  auto neworders = storages.neworders_;
  auto orders = storages.orders_;
  auto orders_secondary = storages.orders_secondary_;
  auto orderlines = storages.orderlines_;
  OrderData o_data;
  zero_clear(&o_data);
  OrderlineData ol_data;
  zero_clear(&ol_data);
  Wdid wdid = combine_wdid(wid, did);
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  for (Oid oid = 0; oid < kOrders; ++oid) {
    Wdoid wdoid = combine_wdoid(wdid, oid);
    WRAP_ERROR_CODE(commit_if_full());

    // Generate Order Data
    Cid o_cid = get_permutation(cid_array);
    Wdcid wdcid = combine_wdcid(wdid, o_cid);
    uint32_t o_carrier_id = rnd_.uniform_within(1, 10);
    uint32_t o_ol_cnt = rnd_.uniform_within(5, 15);

    o_data.cid_ = o_cid;
    o_data.all_local_ = 1;
    o_data.ol_cnt_ = o_ol_cnt;
    std::memcpy(o_data.entry_d_, timestamp_, 26);

    if (oid >= 2100U) {   /* the last 900 orders have not been delivered) */
      o_data.carrier_id_ = 0;
      if (load_neworders) {
        WRAP_ERROR_CODE(neworders.insert_record_normalized(context_, wdoid));
          ++storages.neworders_count_;
      }
    } else {
      o_data.carrier_id_ = o_carrier_id;
    }

    if (load_orders) {
      WRAP_ERROR_CODE(orders.insert_record_normalized(context_, wdoid, &o_data, sizeof(o_data)));
        ++storages.orders_count_;
    }
    Wdcoid wdcoid = combine_wdcoid(wdcid, oid);
    if (load_orders_secondary) {
      WRAP_ERROR_CODE(orders_secondary.insert_record_normalized(context_, wdcoid));
        ++storages.orders_secondary_count_;
    }
    DVLOG(2) << "OID = " << oid << ", CID = " << o_cid << ", DID = "
      << static_cast<int>(did) << ", WID = " << wid;
    if (load_orderlines) {
      for (Ol ol = 1; ol <= o_ol_cnt; ol++) {
        // Generate Order Line Data
        make_alpha_string(24, 24, ol_data.dist_info_);
        ol_data.iid_ = rnd_.uniform_within(0, kItems - 1);
        ol_data.supply_wid_ = wid;
        ol_data.quantity_ = 5;
        if (oid >= 2100U) {
          ol_data.amount_ = 0;
        } else {
          std::string time_str(timestamp_);
          ol_data.amount_ = static_cast<float>(rnd_.uniform_within(10L, 10000L)) / 100.0;
          std::memcpy(ol_data.delivery_d_, time_str.data(), time_str.size());
        }

        Wdol wdol = combine_wdol(wdoid, ol);
        WRAP_ERROR_CODE(orderlines.insert_record_normalized(
          context_,
          wdol,
          &ol_data,
          sizeof(ol_data)));
        ++storages.orderlines_count_;

        DVLOG(2) << "OL = " << ol << ", IID = " << ol_data.iid_ << ", QUAN = " << ol_data.quantity_
          << ", AMT = " << ol_data.amount_;
      }
    }
  }
  WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &ep));
  return kRetOk;
}

void TpccLoadTask::random_orig(bool *orig) {
  std::memset(orig, 0, kItems * sizeof (bool));
  for (uint32_t i = 0; i< kItems / 10; ++i) {
    int32_t pos;
    do {
      pos = rnd_.uniform_within(0, kItems - 1);
    } while (orig[pos]);
    orig[pos] = true;
  }
}

int32_t TpccLoadTask::make_alpha_string(int32_t min, int32_t max, char *str) {
  const char *character =
    /***  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; */
    "abcedfghijklmnopqrstuvwxyz";
  int32_t length = rnd_.uniform_within(min, max);

  for (int32_t i = 0; i < length;  ++i) {
    str[i] = character[rnd_.uniform_within(0, 25)];
  }
  // to make sure, fill out _all_ remaining part with NULL character.
  std::memset(str + length, 0, max - length + 1);
  return length;
}

Cid TpccLoadTask::get_permutation(bool* cid_array) {
  while (true) {
    Cid r = rnd_.uniform_within(0, kCustomers - 1);
    if (cid_array[r]) {       /* This number already taken */
      continue;
    }
    cid_array[r] = true;         /* mark taken */
    return r;
  }
}

ErrorStack tpcc_load_task(
  thread::Thread* context,
  const void* /*input_buffer*/,
  uint32_t /*input_len*/,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  return TpccLoadTask().run(context);
}

void run_test(
  proc::Proc proc,
  bool load_customers_secondary_arg,
  bool load_neworders_arg,
  bool load_orders_arg,
  bool load_orders_secondary_arg,
  bool load_orderlines_arg) {
  load_customers_secondary = load_customers_secondary_arg;
  load_neworders = load_neworders_arg;
  load_orders = load_orders_arg;
  load_orders_secondary = load_orders_secondary_arg;
  load_orderlines = load_orderlines_arg;

  EngineOptions options = get_tiny_options();
  options.thread_.group_count_ = 1;
  options.thread_.thread_count_per_group_ = 1;
  options.log_.log_buffer_kb_ = 1 << 14;
  options.log_.log_file_size_mb_ = 1 << 10;
  options.memory_.page_pool_size_mb_per_node_ = 1 << 7;
  options.cache_.snapshot_cache_size_mb_per_node_ = 1 << 4;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("the_task", proc);
  engine.get_proc_manager()->pre_register("tpcc_load_task", tpcc_load_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("tpcc_load_task"));
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("the_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

MasstreeStorage get_scan_target(uint32_t* expected_records_out, std::string* name) {
  MasstreeStorage target;
  uint32_t expected_records;
  if (storages.customers_secondary_count_) {
    target = storages.customers_secondary_;
    expected_records = storages.customers_secondary_count_;
    EXPECT_EQ(kCustomers * kDistricts * kWarehouses, expected_records);
    *name = "customers_secondary";
  } else if (storages.neworders_count_) {
    target = storages.neworders_;
    expected_records = storages.neworders_count_;
    // about 30% neworder
    EXPECT_GT(expected_records, kOrders * kDistricts * kWarehouses * 29ULL / 100ULL);
    EXPECT_LT(expected_records, kOrders * kDistricts * kWarehouses * 31ULL / 100ULL);
    *name = "neworders";
  } else if (storages.orders_count_) {
    target = storages.orders_;
    expected_records = storages.orders_count_;
    EXPECT_EQ(kOrders * kDistricts * kWarehouses, expected_records);
    *name = "orders";
  } else if (storages.orders_secondary_count_) {
    target = storages.orders_secondary_;
    expected_records = storages.orders_secondary_count_;
    EXPECT_EQ(kOrders * kDistricts * kWarehouses, expected_records);
    *name = "orders_secondary";
  } else {
    target = storages.orderlines_;
    expected_records = storages.orderlines_count_;
    // about 10 OL per order
    EXPECT_GT(expected_records, kOrders * kDistricts * kWarehouses * 9ULL);
    EXPECT_LT(expected_records, kOrders * kDistricts * kWarehouses * 11ULL);
    *name = "orderlines";
  }
  *expected_records_out = expected_records;
  return target;
}

ErrorStack full_scan_task(
  thread::Thread* context,
  const void* /*input_buffer*/,
  uint32_t /*input_len*/,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  uint32_t expected_records;
  std::string name;
  MasstreeStorage target = get_scan_target(&expected_records, &name);

  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  const uint32_t kBatch = 200;
  SCOPED_TRACE(testing::Message() << "Full scan, index=" << name);
  {
    // full forward scan
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    MasstreeCursor cursor(target, context);
    EXPECT_EQ(kErrorCodeOk, cursor.open());
    Epoch commit_epoch;
    uint32_t count = 0;
    char prev_key[100];
    uint32_t prev_key_length = 0;
    while (cursor.is_valid_record()) {
      if (count > 0) {
        EXPECT_LT(
          std::string(prev_key, prev_key_length),
          std::string(cursor.get_key(), cursor.get_key_length()));
      }
      prev_key_length = cursor.get_key_length();
      ASSERT_ND(prev_key_length <= 100U);
      std::memcpy(prev_key, cursor.get_key(), cursor.get_key_length());
      ++count;
      if ((count % kBatch) == 0U) {
        EXPECT_EQ(kErrorCodeOk, xct_manager->precommit_xct(context, &commit_epoch)) << count;
        WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
      }
      EXPECT_EQ(kErrorCodeOk, cursor.next()) << count;
    }
    EXPECT_EQ(kErrorCodeOk, xct_manager->precommit_xct(context, &commit_epoch));
    EXPECT_EQ(expected_records, count);
  }
  {
    // full backward scan
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    MasstreeCursor cursor(target, context);
    EXPECT_EQ(kErrorCodeOk, cursor.open(
      nullptr,
      MasstreeCursor::kKeyLengthExtremum,
      nullptr,
      MasstreeCursor::kKeyLengthExtremum,
      false));
    Epoch commit_epoch;
    uint32_t count = 0;
    char prev_key[100];
    uint32_t prev_key_length = 0;
    while (cursor.is_valid_record()) {
      if (count > 0) {
        EXPECT_GT(
          std::string(prev_key, prev_key_length),
          std::string(cursor.get_key(), cursor.get_key_length()));
      }
      prev_key_length = cursor.get_key_length();
      ASSERT_ND(prev_key_length <= 100U);
      std::memcpy(prev_key, cursor.get_key(), cursor.get_key_length());
      ++count;
      if ((count % kBatch) == 0U) {
        EXPECT_EQ(kErrorCodeOk, xct_manager->precommit_xct(context, &commit_epoch)) << count;
        WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
      }
      EXPECT_EQ(kErrorCodeOk, cursor.next()) << count;
    }
    EXPECT_EQ(kErrorCodeOk, xct_manager->precommit_xct(context, &commit_epoch));
    EXPECT_EQ(expected_records, count);
  }
  return kRetOk;
}

TEST(MasstreeTpccTest, FullscanCustomersSecondary) {
  run_test(full_scan_task, true, false, false, false, false);
}
TEST(MasstreeTpccTest, FullscanNeworders) {
  run_test(full_scan_task, false, true, false, false, false);
}
TEST(MasstreeTpccTest, FullscanOrders) {
  run_test(full_scan_task, false, false, true, false, false);
}
TEST(MasstreeTpccTest, FullscanOrdersSecondary) {
  run_test(full_scan_task, false, false, false, true, false);
}
TEST(MasstreeTpccTest, FullscanOrderlines) {
  run_test(full_scan_task, false, false, false, false, true);
}

ErrorStack district_scan_task(
  thread::Thread* context,
  const void* /*input_buffer*/,
  uint32_t /*input_len*/,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  uint32_t expected_records;
  std::string name;
  MasstreeStorage target = get_scan_target(&expected_records, &name);

  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  const uint32_t kBatch = 200;
  for (Wid wid = 0; wid < kWarehouses; ++wid) {
    for (Did did = 0; did < kDistricts; ++did) {
      char low[8];
      char high[8];
      std::memset(low, 0, 8);
      std::memset(high, 0, 8);
      Wdid wdid = combine_wdid(wid, did);
      Wdid wdid_h = combine_wdid(wid, did + 1U);
      if (storages.customers_secondary_count_) {
        assorted::write_bigendian<Wid>(wid, low);
        assorted::write_bigendian<Did>(did, low + sizeof(Wid));
        assorted::write_bigendian<Wid>(wid, high);
        assorted::write_bigendian<Did>(did + 1U, high + sizeof(Wid));
      } else if (storages.neworders_count_ || storages.orders_count_) {
        assorted::write_bigendian<Wdoid>(combine_wdoid(wdid, 0), low);
        assorted::write_bigendian<Wdoid>(combine_wdoid(wdid_h, 0), high);
      } else if (storages.orders_secondary_count_) {
        assorted::write_bigendian<Wdcoid>(combine_wdcoid(combine_wdcid(wdid, 0), 0), low);
        assorted::write_bigendian<Wdcoid>(combine_wdcoid(combine_wdcid(wdid_h, 0), 0), high);
      } else {
        assorted::write_bigendian<Wdol>(combine_wdol(combine_wdoid(wdid, 0), 0), low);
        assorted::write_bigendian<Wdol>(combine_wdol(combine_wdoid(wdid_h, 0), 0), high);
      }
      std::string low_str(low, 8);
      std::string high_str(high, 8);
      SCOPED_TRACE(testing::Message() << "Wid=" << wid << ", Did=" << static_cast<int>(did)
        << ", index=" << name);
      {
        // in-district forward scan
        WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
        MasstreeCursor cursor(target, context);
        EXPECT_EQ(kErrorCodeOk, cursor.open(low, 8, high, 8));
        Epoch commit_epoch;
        uint32_t count = 0;
        char prev_key[100];
        uint32_t prev_key_length = 0;
        while (cursor.is_valid_record()) {
          std::string cur_key(cursor.get_key(), cursor.get_key_length());
          if (count > 0) {
            EXPECT_LT(std::string(prev_key, prev_key_length), cur_key);
          }
          EXPECT_GE(cur_key, low_str);
          EXPECT_LT(cur_key, high_str);
          prev_key_length = cursor.get_key_length();
          ASSERT_ND(prev_key_length <= 100U);
          std::memcpy(prev_key, cursor.get_key(), cursor.get_key_length());
          ++count;
          if ((count % kBatch) == 0U) {
            EXPECT_EQ(kErrorCodeOk, xct_manager->precommit_xct(context, &commit_epoch)) << count;
            WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
          }
          EXPECT_EQ(kErrorCodeOk, cursor.next()) << count;
        }
        EXPECT_EQ(kErrorCodeOk, xct_manager->precommit_xct(context, &commit_epoch));
        EXPECT_LT(count, expected_records / kDistricts / kWarehouses * 11U / 10U);
        EXPECT_GT(count, expected_records / kDistricts / kWarehouses * 9U / 10U);
      }
      {
        // in-district backward scan
        WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
        MasstreeCursor cursor(target, context);
        EXPECT_EQ(kErrorCodeOk, cursor.open(
          high,
          8,
          low,
          8,
          false,
          false,
          false,
          true));
        Epoch commit_epoch;
        uint32_t count = 0;
        char prev_key[100];
        uint32_t prev_key_length = 0;
        while (cursor.is_valid_record()) {
          std::string cur_key(cursor.get_key(), cursor.get_key_length());
          if (count > 0) {
            EXPECT_GT(std::string(prev_key, prev_key_length), cur_key);
          }
          EXPECT_GE(cur_key, low_str);
          EXPECT_LT(cur_key, high_str);
          prev_key_length = cursor.get_key_length();
          ASSERT_ND(prev_key_length <= 100U);
          std::memcpy(prev_key, cursor.get_key(), cursor.get_key_length());
          ++count;
          if ((count % kBatch) == 0U) {
            EXPECT_EQ(kErrorCodeOk, xct_manager->precommit_xct(context, &commit_epoch)) << count;
            WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
          }
          EXPECT_EQ(kErrorCodeOk, cursor.next()) << count;
        }
        EXPECT_EQ(kErrorCodeOk, xct_manager->precommit_xct(context, &commit_epoch));
        EXPECT_LT(count, expected_records / kDistricts / kWarehouses * 11U / 10U);
        EXPECT_GT(count, expected_records / kDistricts / kWarehouses * 9U / 10U);
      }
    }
  }
  return kRetOk;
}


TEST(MasstreeTpccTest, DistrictScanCustomersSecondary) {
  run_test(district_scan_task, true, false, false, false, false);
}
TEST(MasstreeTpccTest, DistrictScanNeworders) {
  run_test(district_scan_task, false, true, false, false, false);
}
TEST(MasstreeTpccTest, DistrictScanOrders) {
  run_test(district_scan_task, false, false, true, false, false);
}
TEST(MasstreeTpccTest, DistrictScanOrdersSecondary) {
  run_test(district_scan_task, false, false, false, true, false);
}
TEST(MasstreeTpccTest, DistrictScanOrderlines) {
  run_test(district_scan_task, false, false, false, false, true);
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
