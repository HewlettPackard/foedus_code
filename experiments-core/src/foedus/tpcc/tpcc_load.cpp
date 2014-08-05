/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_load.hpp"

#include <fcntl.h>
#include <time.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/epoch.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/tpcc/tpcc.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"


namespace foedus {
namespace tpcc {
ErrorStack TpccLoadTask::run(thread::Thread* context) {
  context_ = context;
  engine_ = context->get_engine();
  xct_manager_ = &engine_->get_xct_manager();
  debugging::StopWatch watch;
  CHECK_ERROR(load_tables());
  watch.stop();
  LOG(INFO) << "Loaded TPC-C tables in " << watch.elapsed_sec() << "sec";
  return kRetOk;
}

ErrorStack TpccLoadTask::load_tables() {
  // Initialize timestamp (for date columns)
  time_t t_clock;
  ::time(&t_clock);
  timestamp_ = ::ctime(&t_clock);  // NOLINT(runtime/threadsafe_fn) no race here
  ASSERT_ND(timestamp_);

  CHECK_ERROR(create_tables());

  CHECK_ERROR(load_warehouses());
  LOG(INFO) << "Loaded Warehouses:" << engine_->get_memory_manager().dump_free_memory_stat();
  CHECK_ERROR(load_districts());
  LOG(INFO) << "Loaded Districts:" << engine_->get_memory_manager().dump_free_memory_stat();
  CHECK_ERROR(load_customers());
  LOG(INFO) << "Loaded Customers:" << engine_->get_memory_manager().dump_free_memory_stat();
  CHECK_ERROR(load_items());
  LOG(INFO) << "Loaded Items:" << engine_->get_memory_manager().dump_free_memory_stat();
  CHECK_ERROR(load_stocks());
  LOG(INFO) << "Loaded Strocks:" << engine_->get_memory_manager().dump_free_memory_stat();
  CHECK_ERROR(load_orders());
  LOG(INFO) << "Loaded Orders:" << engine_->get_memory_manager().dump_free_memory_stat();

  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  CHECK_ERROR(storages_.customers_secondary_->verify_single_thread(context_));
  CHECK_ERROR(storages_.neworders_->verify_single_thread(context_));
  CHECK_ERROR(storages_.orderlines_->verify_single_thread(context_));
  CHECK_ERROR(storages_.orders_->verify_single_thread(context_));
  CHECK_ERROR(storages_.orders_secondary_->verify_single_thread(context_));
  WRAP_ERROR_CODE(xct_manager_->abort_xct(context_));

  LOG(INFO) << "Loaded all tables. Waiting for flushing all logs...";
  Epoch ep = engine_->get_xct_manager().get_current_global_epoch();
  engine_->get_xct_manager().advance_current_global_epoch();
  WRAP_ERROR_CODE(engine_->get_log_manager().wait_until_durable(ep));
  LOG(INFO) << "Okay, flushed all logs.";

  return kRetOk;
}

ErrorStack TpccLoadTask::create_tables() {
  std::memset(&storages_, 0, sizeof(storages_));

  LOG(INFO) << "Initial:" << engine_->get_memory_manager().dump_free_memory_stat();
  CHECK_ERROR(create_array(
    "customers_static",
    sizeof(CustomerStaticData),
    kWarehouses * kDistricts * kCustomers,
    &storages_.customers_static_));
  CHECK_ERROR(create_array(
    "customers_dynamic",
    sizeof(CustomerDynamicData),
    kWarehouses * kDistricts * kCustomers,
    &storages_.customers_dynamic_));
  CHECK_ERROR(create_array(
    "customers_history",
    CustomerStaticData::kHistoryDataLength,
    kWarehouses * kDistricts * kCustomers,
    &storages_.customers_history_));
  LOG(INFO) << "Created Customers:" << engine_->get_memory_manager().dump_free_memory_stat();

  CHECK_ERROR(create_masstree("customers_secondary", &storages_.customers_secondary_));

  CHECK_ERROR(create_array(
    "districts_static",
    sizeof(DistrictStaticData),
    kWarehouses * kDistricts,
    &storages_.districts_static_));
  CHECK_ERROR(create_array(
    "districts_ytd",
    sizeof(uint64_t),
    kWarehouses * kDistricts,
    &storages_.districts_ytd_));
  CHECK_ERROR(create_array(
    "districts_next_oid",
    sizeof(Oid),
    kWarehouses * kDistricts,
    &storages_.districts_next_oid_));
  LOG(INFO) << "Created Districts:" << engine_->get_memory_manager().dump_free_memory_stat();

  CHECK_ERROR(create_sequential("histories", &storages_.histories_));
  CHECK_ERROR(create_masstree("neworders", &storages_.neworders_));
  CHECK_ERROR(create_masstree("orders", &storages_.orders_));
  CHECK_ERROR(create_masstree("orders_secondary", &storages_.orders_secondary_));
  CHECK_ERROR(create_masstree("orderlines", &storages_.orderlines_));

  CHECK_ERROR(create_array(
    "items",
    sizeof(ItemData),
    kItems,
    &storages_.items_));
  LOG(INFO) << "Created Items:" << engine_->get_memory_manager().dump_free_memory_stat();

  CHECK_ERROR(create_array(
    "stocks",
    sizeof(StockData),
    kWarehouses * kItems,
    &storages_.stocks_));
  LOG(INFO) << "Created Stocks:" << engine_->get_memory_manager().dump_free_memory_stat();

  CHECK_ERROR(create_array(
    "warehouses_static",
    sizeof(WarehouseStaticData),
    kWarehouses,
    &storages_.warehouses_static_));
  CHECK_ERROR(create_array(
    "warehouses_ytd",
    sizeof(double),
    kWarehouses,
    &storages_.warehouses_ytd_));
  LOG(INFO) << "Created Warehouses:" << engine_->get_memory_manager().dump_free_memory_stat();

  return kRetOk;
}

ErrorStack TpccLoadTask::create_array(
  const std::string& name,
  uint32_t payload_size,
  uint64_t array_size,
  storage::array::ArrayStorage** storage) {
  Epoch ep;
  storage::array::ArrayMetadata meta(name, payload_size, array_size);
  CHECK_ERROR(engine_->get_storage_manager().create_array(context_, &meta, storage, &ep));
  ASSERT_ND(*storage);
  return kRetOk;
}

ErrorStack TpccLoadTask::create_masstree(
  const std::string& name,
  storage::masstree::MasstreeStorage** storage) {
  Epoch ep;
  storage::masstree::MasstreeMetadata meta(name);
  CHECK_ERROR(engine_->get_storage_manager().create_masstree(context_, &meta, storage, &ep));
  ASSERT_ND(*storage);
  return kRetOk;
}

ErrorStack TpccLoadTask::create_sequential(
  const std::string& name,
  storage::sequential::SequentialStorage** storage) {
  Epoch ep;
  storage::sequential::SequentialMetadata meta(name);
  CHECK_ERROR(engine_->get_storage_manager().create_sequential(context_, &meta, storage, &ep));
  ASSERT_ND(*storage);
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

ErrorStack TpccLoadTask::load_warehouses() {
  LOG(INFO) << "Loading Warehouse";
  WarehouseStaticData data;
  Epoch ep;
  auto* static_storage = storages_.warehouses_static_;
  auto* ytd_storage = storages_.warehouses_ytd_;
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  for (Wid wid = 0; wid < kWarehouses; ++wid) {
    zero_clear(&data);

    // Generate Warehouse Data
    make_alpha_string(6, 10, data.name_);
    make_address(data.street1_, data.street2_, data.city_, data.state_, data.zip_);
    data.tax_ = (static_cast<float>(rnd_.uniform_within(10L, 20L))) / 100.0;
    double ytd = 3000000.00;
    WRAP_ERROR_CODE(static_storage->overwrite_record(context_, wid, &data, 0, sizeof(data)));
    WRAP_ERROR_CODE(ytd_storage->overwrite_record_primitive<double>(context_, wid, ytd, 0));
    WRAP_ERROR_CODE(commit_if_full());
    VLOG(0) << "WID = " << wid << ", Name= " << data.name_ << ", Tax = " << data.tax_;
  }
  WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &ep));
  LOG(INFO) << "Loaded Warehouse";
  return kRetOk;
}

ErrorStack TpccLoadTask::load_districts() {
  DistrictStaticData data;
  Epoch ep;
  auto* static_storage = storages_.districts_static_;
  auto* ytd_storage = storages_.districts_ytd_;
  auto* oid_storage = storages_.districts_next_oid_;
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  for (Wid wid = 0; wid < kWarehouses; ++wid) {
    LOG(INFO) << "Loading District Wid=" << wid;
    for (Did did = 0; did < kDistricts; ++did) {
      zero_clear(&data);
      uint64_t ytd = 30000;
      Oid next_o_id = kOrders;
      make_alpha_string(6, 10, data.name_);
      make_address(data.street1_, data.street2_, data.city_, data.state_, data.zip_);
      data.tax_ = (static_cast<float>(rnd_.uniform_within(10, 20))) / 100.0;
      Wdid wdid = combine_wdid(wid, did);
      WRAP_ERROR_CODE(static_storage->overwrite_record(context_, wdid, &data, 0, sizeof(data)));
      WRAP_ERROR_CODE(ytd_storage->overwrite_record_primitive<uint64_t>(context_, wdid, ytd, 0));
      WRAP_ERROR_CODE(oid_storage->overwrite_record_primitive<Oid>(context_, wdid, next_o_id, 0));
      WRAP_ERROR_CODE(commit_if_full());
      VLOG(0) << "DID = " << static_cast<int>(did) << ", WID = " << wid
        << ", Name = " << data.name_ << ", Tax = " << data.tax_;
    }
  }
  WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &ep));
  LOG(INFO) << "District Done";
  return kRetOk;
}

ErrorStack TpccLoadTask::load_items() {
  LOG(INFO) << "Loading Item";

  bool orig[kItems];
  random_orig(orig);

  Epoch ep;
  auto* storage = storages_.items_;
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  ItemData data;
  for (Iid iid = 0; iid < kItems; ++iid) {
    zero_clear(&data);

    /* Generate Item Data */
    make_alpha_string(14, 24, data.name_);
    data.price_ = (static_cast<float>(rnd_.uniform_within(100L, 10000L))) / 100.0;
    int32_t idatasiz = make_alpha_string(26, 50, data.data_);

    if (orig[iid]) {
      int32_t pos = rnd_.uniform_within(0, idatasiz-8);
      std::memcpy(data.data_ + pos, "original", 8);
    }

    DVLOG(2) << "IID = " << iid << ", Name= " << data.name_ << ", Price = " << data.price_;

    data.im_id_ = 0;
    WRAP_ERROR_CODE(storage->overwrite_record(context_, iid, &data, 0, sizeof(data)));
    WRAP_ERROR_CODE(commit_if_full());
    if ((iid % 20000) == 0) {
      LOG(INFO) << "IID=" << iid << "/" << kItems;
    }
  }
  WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &ep));

  LOG(INFO) << "Item Done.";
  return kRetOk;
}

ErrorStack TpccLoadTask::load_stocks() {
  Epoch ep;
  auto* storage = storages_.stocks_;
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  StockData data;
  for (Wid wid = 0; wid < kWarehouses; ++wid) {
      LOG(INFO) << "Loading Stock Wid=" << wid;
    bool orig[kItems];
    random_orig(orig);

    for (Iid iid = 0; iid < kItems; ++iid) {
      zero_clear(&data);

      // Generate Stock Data
      for (Did did = 0; did < kDistricts; ++did) {
        make_alpha_string(24, 24, data.dist_data_[did]);
      }
      int32_t sdatasiz = make_alpha_string(26, 50, data.data_);
      if (orig[iid]) {
        int32_t pos = rnd_.uniform_within(0, sdatasiz - 8);
        std::memcpy(data.data_ + pos, "original", 8);
      }

      data.quantity_ = rnd_.uniform_within(10, 100);
      data.ytd_ = 0;
      data.order_cnt_ = 0;
      data.remote_cnt_ = 0;
      Sid sid = combine_sid(wid, iid);
      WRAP_ERROR_CODE(storage->overwrite_record(context_, sid, &data, 0, sizeof(data)));
      WRAP_ERROR_CODE(commit_if_full());
      DVLOG(2) << "SID = " << iid << ", WID = " << wid << ", Quan = " << data.quantity_;
      if ((iid % 20000) == 0) {
        LOG(INFO) << "IID=" << iid << "/" << kItems;
      }
    }
  }
  WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &ep));
  LOG(INFO) << " Stock Done.";
  return kRetOk;
}

ErrorStack TpccLoadTask::load_customers() {
  for (Wid wid = 0; wid < kWarehouses; ++wid) {
    for (Did did = 0; did < kDistricts; ++did) {
      CHECK_ERROR(load_customers_in_district(wid, did));
    }
  }
  return kRetOk;
}

ErrorStack TpccLoadTask::load_customers_in_district(Wid wid, Did did) {
  LOG(INFO) << "Loading Customer for DID=" << static_cast<int>(did) << ", WID=" << wid
    << ": " << engine_->get_memory_manager().dump_free_memory_stat();

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
  auto* histories = storages_.histories_;
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  CustomerStaticData c_data;
  CustomerDynamicData c_dynamic;
  char c_history[CustomerStaticData::kHistoryDataLength];
  HistoryData h_data;
  const Wdid wdid = combine_wdid(wid, did);
  for (Cid cid = 0; cid < kCustomers; ++cid) {
    zero_clear(&c_data);
    zero_clear(&c_dynamic);
    zero_clear(&h_data);

    // Generate Customer Data
    make_alpha_string(8, 16, c_data.first_);
    c_data.middle_[0] = 'O';
    c_data.middle_[1] = 'E';
    c_data.middle_[2] = '\0';

    if (cid < kLnames) {
      generate_lastname(cid, c_data.last_);
    } else {
      generate_lastname(rnd_.non_uniform_within(255, 0, kLnames - 1), c_data.last_);
    }

    make_address(c_data.street1_, c_data.street2_, c_data.city_, c_data.state_, c_data.zip_);
    make_number_string(16, 16, c_data.phone_);
    c_data.credit_[0] = (rnd_.uniform_within(0, 1) == 0 ? 'G' : 'B');
    c_data.credit_[1] = 'C';
    c_data.credit_[2] = '\0';
    make_alpha_string(300, 500, c_history);

    // Prepare for putting into the database
    c_data.discount_ = (static_cast<float>(rnd_.uniform_within(0, 50))) / 100.0;
    c_dynamic.balance_ = -10.0;
    c_data.credit_lim_ = 50000;

    Wdcid wdcid = combine_wdcid(wdid, cid);
    WRAP_ERROR_CODE(storages_.customers_static_->overwrite_record(
      context_,
      wdcid,
      &c_data,
      0,
      sizeof(c_data)));
    WRAP_ERROR_CODE(storages_.customers_dynamic_->overwrite_record(
      context_,
      wdcid,
      &c_dynamic,
      0,
      sizeof(c_dynamic)));
    WRAP_ERROR_CODE(storages_.customers_history_->overwrite_record(
      context_,
      wdcid,
      &c_history,
      0,
      sizeof(c_history)));
    WRAP_ERROR_CODE(commit_if_full());
    std::memcpy(secondary_keys[cid].last_, c_data.last_, sizeof(c_data.last_));
    std::memcpy(secondary_keys[cid].first_, c_data.first_, sizeof(c_data.first_));
    secondary_keys[cid].cid_ = cid;
    DVLOG(2) << "CID = " << cid << ", LST = " << c_data.last_ << ", P# = " << c_data.phone_;

    make_alpha_string(12, 24, h_data.data_);
    h_data.cid_ = cid;
    h_data.c_did_ = did;
    h_data.c_wid_ = wid;
    h_data.wid_ = wid;
    h_data.did_ = did;
    h_data.amount_ = 10.0;
    std::memcpy(h_data.date_, timestamp_, 26);
    WRAP_ERROR_CODE(histories->append_record(context_, &h_data, sizeof(h_data)));
    WRAP_ERROR_CODE(commit_if_full());
  }
  WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &ep));

  // now insert all secondary keys.
  // by sorting them here, we get better insert performance and fill factor.
  debugging::StopWatch sort_watch;
  std::sort(secondary_keys, secondary_keys + kCustomers, Secondary::compare);
  sort_watch.stop();
  LOG(INFO) << "Sorted secondary entries in " << sort_watch.elapsed_us() << "us";
  auto* customers_secondary = storages_.customers_secondary_;
  for (Cid from = 0; from < kCustomers;) {
    uint32_t cur_batch_size = std::min<uint32_t>(kCommitBatch, kCustomers - from);
    char key_be[CustomerSecondaryKey::kKeyLength];
    assorted::write_bigendian<Wid>(wid, key_be);
    key_be[sizeof(Wid)] = did;
    // An easy optimization for batched inserts. Trigger reserve_record for all of them,
    // then abort and do it as a fresh transaction so that no moved-bit tracking is required.
    for (int rep = 0; rep < 2; ++rep) {
      WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
      for (Cid i = from; i < from + cur_batch_size; ++i) {
        std::memcpy(key_be + sizeof(Wid) + sizeof(Did), secondary_keys[i].last_, 34);
        Cid* address = reinterpret_cast<Cid*>(key_be + sizeof(Wid) + sizeof(Did) + 34);
        *address = assorted::htobe<Cid>(secondary_keys[i].cid_);
        WRAP_ERROR_CODE(customers_secondary->insert_record(context_, key_be, sizeof(key_be)));
      }
      if (rep == 0) {
        WRAP_ERROR_CODE(xct_manager_->abort_xct(context_));
      } else {
        WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &ep));
      }
    }
    from += cur_batch_size;
  }
  return kRetOk;
}

ErrorStack TpccLoadTask::load_orders() {
  for (Wid wid = 0; wid < kWarehouses; ++wid) {
    for (Did did = 0; did < kDistricts; ++did) {
      CHECK_ERROR(load_orders_in_district(wid, did));
    }
  }
  return kRetOk;
}

ErrorStack TpccLoadTask::load_orders_in_district(Wid wid, Did did) {
  LOG(INFO) << "Loading Orders for D=" << static_cast<int>(did) << ", W= " << wid
    << ": " << engine_->get_memory_manager().dump_free_memory_stat();
  // Whether the customer id for the current order is already taken.
  bool cid_array[kCustomers];
  std::memset(cid_array, 0, sizeof(cid_array));

  Epoch ep;
  auto* neworders = storages_.neworders_;
  auto* orders = storages_.orders_;
  auto* orders_secondary = storages_.orders_secondary_;
  auto* orderlines = storages_.orderlines_;
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
      WRAP_ERROR_CODE(neworders->insert_record_normalized(context_, wdoid));
    } else {
      o_data.carrier_id_ = o_carrier_id;
    }

    WRAP_ERROR_CODE(orders->insert_record_normalized(context_, wdoid, &o_data, sizeof(o_data)));
    Wdcoid wdcoid = combine_wdcoid(wdcid, oid);
    WRAP_ERROR_CODE(orders_secondary->insert_record_normalized(context_, wdcoid));
    DVLOG(2) << "OID = " << oid << ", CID = " << o_cid << ", DID = "
      << static_cast<int>(did) << ", WID = " << wid;
    for (Ol ol = 1; ol <= o_ol_cnt; ol++) {
      // Generate Order Line Data
      make_alpha_string(24, 24, ol_data.dist_info_);
      ol_data.iid_ = rnd_.uniform_within(0, kItems - 1);
      ol_data.supply_wid_ = wid;
      ol_data.quantity_ = 5;
      if (oid >= 2100U) {
        ol_data.amount_ = 0;
      } else {
        ol_data.amount_ = static_cast<float>(rnd_.uniform_within(10L, 10000L)) / 100.0;
        std::memcpy(ol_data.delivery_d_, timestamp_, 26);
      }

      Wdol wdol = combine_wdol(wdoid, ol);
      WRAP_ERROR_CODE(orderlines->insert_record_normalized(
        context_,
        wdol,
        &ol_data,
        sizeof(ol_data)));

      DVLOG(2) << "OL = " << ol << ", IID = " << ol_data.iid_ << ", QUAN = " << ol_data.quantity_
        << ", AMT = " << ol_data.amount_;
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

int32_t TpccLoadTask::make_number_string(int32_t min, int32_t max, char *str) {
  const char *character =
    /***  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; */
    "1234567890";
  int32_t length = rnd_.uniform_within(min, max);
  for (int32_t i = 0; i < length; ++i) {
    str[i] = character[rnd_.uniform_within(0, 9)];
  }
  // to make sure, fill out _all_ remaining part with NULL character.
  std::memset(str + length, 0, max - length + 1);

  return length;
}

void TpccLoadTask::make_address(char *str1, char *str2, char *city, char *state, char *zip) {
  make_alpha_string(10, 20, str1); /* Street 1*/
  make_alpha_string(10, 20, str2); /* Street 2*/
  make_alpha_string(10, 20, city); /* City */
  make_alpha_string(2, 2, state); /* State */
  make_number_string(9, 9, zip); /* Zip */
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

}  // namespace tpcc
}  // namespace foedus
