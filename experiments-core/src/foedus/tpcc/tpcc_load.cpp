/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_load.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <mutex>
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
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/tpcc/tpcc.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"


namespace foedus {
namespace tpcc {
ErrorStack tpcc_finishup_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  if (args.input_len_ != sizeof(Wid)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  Wid total_warehouses = *reinterpret_cast<const Wid*>(args.input_buffer_);
  TpccFinishupTask task(total_warehouses);
  return task.run(context);
}

ErrorStack tpcc_load_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  if (args.input_len_ != sizeof(TpccLoadTask::Inputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  const TpccLoadTask::Inputs* inputs = reinterpret_cast<const TpccLoadTask::Inputs*>(
    args.input_buffer_);
  TpccLoadTask task(
    inputs->total_warehouses_,
    inputs->timestamp_,
    inputs->from_wid_,
    inputs->to_wid_,
    inputs->from_iid_,
    inputs->to_iid_);
  return task.run(context);
}

ErrorStack create_all(Engine* engine, Wid total_warehouses) {
  debugging::StopWatch watch;

  LOG(INFO) << "Initial:" << engine->get_memory_manager()->dump_free_memory_stat();
  CHECK_ERROR(create_array(
    engine,
    "customers_static",
    sizeof(CustomerStaticData),
    total_warehouses * kDistricts * kCustomers));
  CHECK_ERROR(create_array(
    engine,
    "customers_dynamic",
    sizeof(CustomerDynamicData),
    total_warehouses * kDistricts * kCustomers));
  CHECK_ERROR(create_array(
    engine,
    "customers_history",
    CustomerStaticData::kHistoryDataLength,
    total_warehouses * kDistricts * kCustomers));
  LOG(INFO) << "Created Customers:" << engine->get_memory_manager()->dump_free_memory_stat();

  CHECK_ERROR(create_masstree(
    engine,
    "customers_secondary",
    0));  // customer is a static table, so why not 100% fill-factor

  CHECK_ERROR(create_array(
    engine,
    "districts_static",
    sizeof(DistrictStaticData),
    total_warehouses * kDistricts));
  CHECK_ERROR(create_array(
    engine,
    "districts_ytd",
    sizeof(DistrictYtdData),
    total_warehouses * kDistricts));
  CHECK_ERROR(create_array(
    engine,
    "districts_next_oid",
    sizeof(DistrictNextOidData),
    total_warehouses * kDistricts));
  LOG(INFO) << "Created Districts:" << engine->get_memory_manager()->dump_free_memory_stat();

  CHECK_ERROR(create_sequential(engine, "histories"));

  // orders use around 75% fill factor
  CHECK_ERROR(create_masstree(
    engine,
    "neworders",
    64 * 0.75));
  CHECK_ERROR(create_masstree(
    engine,
    "orders",
    (storage::masstree::MasstreeBorderPage::kDataSize / sizeof(OrderData)) * 0.75));
  CHECK_ERROR(create_masstree(
    engine,
    "orders_secondary",
    64 * 0.75));
  CHECK_ERROR(create_masstree(
    engine,
    "orderlines",
    (storage::masstree::MasstreeBorderPage::kDataSize / sizeof(OrderlineData)) * 0.75));

  CHECK_ERROR(create_array(engine, "items", sizeof(ItemData), kItems));
  LOG(INFO) << "Created Items:" << engine->get_memory_manager()->dump_free_memory_stat();

  CHECK_ERROR(create_array(
    engine,
    "stocks",
    sizeof(StockData),
    total_warehouses * kItems));
  LOG(INFO) << "Created Stocks:" << engine->get_memory_manager()->dump_free_memory_stat();

  CHECK_ERROR(create_array(
    engine,
    "warehouses_static",
    sizeof(WarehouseStaticData),
    total_warehouses));
  CHECK_ERROR(create_array(
    engine,
    "warehouses_ytd",
    sizeof(WarehouseYtdData),
    total_warehouses));
  LOG(INFO) << "Created Warehouses:" << engine->get_memory_manager()->dump_free_memory_stat();

  watch.stop();
  LOG(INFO) << "Created TPC-C tables in " << watch.elapsed_sec() << "sec";
  return kRetOk;
}


ErrorStack create_array(
  Engine* engine,
  const storage::StorageName& name,
  uint32_t payload_size,
  uint64_t array_size) {
  Epoch ep;
  storage::array::ArrayMetadata meta(name, payload_size, array_size);
  return engine->get_storage_manager()->create_storage(&meta, &ep);
}

ErrorStack create_masstree(
  Engine* engine,
  const storage::StorageName& name,
  float border_fill_factor) {
  Epoch ep;
  storage::masstree::MasstreeMetadata meta(name, border_fill_factor);
  return engine->get_storage_manager()->create_storage(&meta, &ep);
}

ErrorStack create_sequential(Engine* engine, const storage::StorageName& name) {
  Epoch ep;
  storage::sequential::SequentialMetadata meta(name);
  return engine->get_storage_manager()->create_storage(&meta, &ep);
}

ErrorStack TpccFinishupTask::run(thread::Thread* context) {
  Engine* engine = context->get_engine();
  storages_.initialize_tables(engine);
// let's do this even in release. good to check abnormal state
// #ifndef NDEBUG
  WRAP_ERROR_CODE(engine->get_xct_manager()->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(storages_.customers_secondary_.verify_single_thread(context));
  CHECK_ERROR(storages_.neworders_.verify_single_thread(context));
  CHECK_ERROR(storages_.orderlines_.verify_single_thread(context));
  CHECK_ERROR(storages_.orders_.verify_single_thread(context));
  CHECK_ERROR(storages_.orders_secondary_.verify_single_thread(context));
  WRAP_ERROR_CODE(engine->get_xct_manager()->abort_xct(context));

  LOG(INFO) << "Verifying customers_secondary_ in detail..";
  WRAP_ERROR_CODE(engine->get_xct_manager()->begin_xct(context, xct::kDirtyReadPreferVolatile));
  storage::masstree::MasstreeCursor cursor(storages_.customers_secondary_, context);
  WRAP_ERROR_CODE(cursor.open());
  for (Wid wid = 0; wid < total_warehouses_; ++wid) {
    for (Did did = 0; did < kDistricts; ++did) {
      bool cid_array[kCustomers];
      std::memset(cid_array, 0, sizeof(cid_array));
      for (uint32_t c = 0; c < kCustomers; ++c) {  // NOT cid
        if (!cursor.is_valid_record()) {
          LOG(FATAL) << "Record not exist: customers_secondary_: wid=" << wid << ", did="
            << static_cast<int>(did) << ", c=" << c;
        }
        if (cursor.get_key_length() != CustomerSecondaryKey::kKeyLength) {
          LOG(FATAL) << "Key Length wrong: customers_secondary_: wid=" << wid << ", did="
            << static_cast<int>(did) << ", c=" << c;
        }
        if (cursor.get_payload_length() != 0) {
          LOG(FATAL) << "Payload Length wrong: customers_secondary_: wid=" << wid << ", did="
            << static_cast<int>(did) << ", c=" << c;
        }
        const char* key = cursor.get_key();
        Wid wid2 = assorted::read_bigendian<Wid>(key);
        if (wid != wid2) {
          LOG(FATAL) << "Wid mismatch: customers_secondary_: wid=" << wid << ", did="
            << static_cast<int>(did) << ", c=" << c << ". value=" << wid2;
        }
        Did did2 = assorted::read_bigendian<Did>(key + sizeof(Wid));
        if (did != did2) {
          LOG(FATAL) << "Did mismatch: customers_secondary_: wid=" << wid << ", did="
            << static_cast<int>(did) << ", c=" << c << ". value=" << static_cast<int>(did2);
        }
        Cid cid = assorted::betoh<Cid>(
          *reinterpret_cast<const Cid*>(key + sizeof(Wid) + sizeof(Did) + 32));
        if (cid >= kCustomers) {
          LOG(FATAL) << "Cid out of range: customers_secondary_: wid=" << wid << ", did="
            << static_cast<int>(did) << ", c=" << c << ". value=" << cid;
        }
        if (cid_array[cid]) {
          LOG(FATAL) << "Cid duplicate: customers_secondary_: wid=" << wid << ", did="
            << static_cast<int>(did) << ", c=" << c << ". value=" << cid;
        }
        cid_array[cid] = true;
        WRAP_ERROR_CODE(cursor.next());
      }
    }
  }

  WRAP_ERROR_CODE(engine->get_xct_manager()->abort_xct(context));
  LOG(INFO) << "Verified customers_secondary_ in detail.";
// #endif  // NDEBUG

  LOG(INFO) << "Loaded all tables. Waiting for flushing all logs...";
  Epoch ep = engine->get_xct_manager()->get_current_global_epoch();
  engine->get_xct_manager()->advance_current_global_epoch();
  WRAP_ERROR_CODE(engine->get_log_manager()->wait_until_durable(ep));
  LOG(INFO) << "Okay, flushed all logs.";
  return kRetOk;
}

ErrorStack TpccLoadTask::run(thread::Thread* context) {
  context_ = context;
  engine_ = context->get_engine();
  storages_.initialize_tables(engine_);
  xct_manager_ = engine_->get_xct_manager();
  debugging::StopWatch watch;
  CHECK_ERROR(load_tables());
  watch.stop();
  LOG(INFO) << "Loaded TPC-C tables in " << watch.elapsed_sec() << "sec";
  return kRetOk;
}

ErrorStack TpccLoadTask::load_tables() {
  CHECK_ERROR(load_warehouses());
  VLOG(0) << "Loaded Warehouses:" << engine_->get_memory_manager()->dump_free_memory_stat();
  CHECK_ERROR(load_districts());
  VLOG(0) << "Loaded Districts:" << engine_->get_memory_manager()->dump_free_memory_stat();
  CHECK_ERROR(load_customers());
  VLOG(0) << "Loaded Customers:" << engine_->get_memory_manager()->dump_free_memory_stat();
  CHECK_ERROR(load_items());
  VLOG(0) << "Loaded Items:" << engine_->get_memory_manager()->dump_free_memory_stat();
  CHECK_ERROR(load_stocks());
  VLOG(0) << "Loaded Strocks:" << engine_->get_memory_manager()->dump_free_memory_stat();
  CHECK_ERROR(load_orders());
  VLOG(0) << "Loaded Orders:" << engine_->get_memory_manager()->dump_free_memory_stat();
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
  auto static_storage = storages_.warehouses_static_;
  auto ytd_storage = storages_.warehouses_ytd_;
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  for (Wid wid = from_wid_; wid < to_wid_; ++wid) {
    zero_clear(&data);

    // Generate Warehouse Data
    make_alpha_string(6, 10, data.name_);
    make_address(data.street1_, data.street2_, data.city_, data.state_, data.zip_);
    data.tax_ = (static_cast<float>(rnd_.uniform_within(10L, 20L))) / 100.0;
    double ytd = 3000000.00;
    WRAP_ERROR_CODE(static_storage.overwrite_record(context_, wid, &data, 0, sizeof(data)));
    WRAP_ERROR_CODE(ytd_storage.overwrite_record_primitive<double>(context_, wid, ytd, 0));
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
  auto static_storage = storages_.districts_static_;
  auto ytd_storage = storages_.districts_ytd_;
  auto oid_storage = storages_.districts_next_oid_;
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  for (Wid wid = from_wid_; wid < to_wid_; ++wid) {
    LOG(INFO) << "Loading District Wid=" << wid;
    for (Did did = 0; did < kDistricts; ++did) {
      zero_clear(&data);
      double ytd = 30000;
      Oid next_o_id = kOrders;
      make_alpha_string(6, 10, data.name_);
      make_address(data.street1_, data.street2_, data.city_, data.state_, data.zip_);
      data.tax_ = (static_cast<float>(rnd_.uniform_within(10, 20))) / 100.0;
      Wdid wdid = combine_wdid(wid, did);
      WRAP_ERROR_CODE(static_storage.overwrite_record(context_, wdid, &data, 0, sizeof(data)));
      WRAP_ERROR_CODE(ytd_storage.overwrite_record_primitive<double>(context_, wdid, ytd, 0));
      WRAP_ERROR_CODE(oid_storage.overwrite_record_primitive<Oid>(context_, wdid, next_o_id, 0));
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
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  ItemData data;
  for (Iid iid = from_iid_; iid < to_iid_; ++iid) {
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
    WRAP_ERROR_CODE(storages_.items_.overwrite_record(context_, iid, &data, 0, sizeof(data)));
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
  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
  StockData data;
  for (Wid wid = from_wid_; wid < to_wid_; ++wid) {
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
      WRAP_ERROR_CODE(storages_.stocks_.overwrite_record(context_, sid, &data, 0, sizeof(data)));
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
  for (Wid wid = from_wid_; wid < to_wid_; ++wid) {
    for (Did did = 0; did < kDistricts; ++did) {
      CHECK_ERROR(load_customers_in_district(wid, did));
    }
  }
  return kRetOk;
}

// synchronize data load to customer_secondary.
// this is ideal for almost sequential inserts.
// std::mutex customer_secondary_mutex;

ErrorStack TpccLoadTask::load_customers_in_district(Wid wid, Did did) {
  LOG(INFO) << "Loading Customer for DID=" << static_cast<int>(did) << ", WID=" << wid;
  //  << ": " << engine_->get_memory_manager()->dump_free_memory_stat();

  // insert to customers_secondary at the end after sorting
  struct Secondary {
    char  last_[16];      // +16 -> 16
    char  first_[16];     // +16 -> 32
    Cid   cid_;           // +4 -> 36
    char  padding_[4];    // +4 -> 40
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
  if (customer_secondary_keys_buffer_.is_null()) {
    customer_secondary_keys_buffer_.alloc(
      kCustomers * sizeof(Secondary),
      1U << 21,
      memory::AlignedMemory::kNumaAllocOnnode,
      context_->get_numa_node());
  }
  Secondary* secondary_keys = reinterpret_cast<Secondary*>(
    customer_secondary_keys_buffer_.get_block());
  Epoch ep;
  auto histories = storages_.histories_;
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

    if (cid < kLnames) {
      generate_lastname(cid, c_data.last_);
    } else {
      generate_lastname(rnd_.non_uniform_within(255, 0, kLnames - 1), c_data.last_);
    }

    make_address(c_data.street1_, c_data.street2_, c_data.city_, c_data.state_, c_data.zip_);
    make_number_string(15, 15, c_data.phone_);
    c_data.credit_[0] = (rnd_.uniform_within(0, 1) == 0 ? 'G' : 'B');
    c_data.credit_[1] = 'C';
    make_alpha_string(300, 500, c_history);

    // Prepare for putting into the database
    c_data.discount_ = (static_cast<float>(rnd_.uniform_within(0, 50))) / 100.0;
    c_dynamic.balance_ = -10.0;
    c_data.credit_lim_ = 50000;

    Wdcid wdcid = combine_wdcid(wdid, cid);
    WRAP_ERROR_CODE(storages_.customers_static_.overwrite_record(
      context_,
      wdcid,
      &c_data,
      0,
      sizeof(c_data)));
    WRAP_ERROR_CODE(storages_.customers_dynamic_.overwrite_record(
      context_,
      wdcid,
      &c_dynamic,
      0,
      sizeof(c_dynamic)));
    WRAP_ERROR_CODE(storages_.customers_history_.overwrite_record(
      context_,
      wdcid,
      &c_history,
      0,
      sizeof(c_history)));
    WRAP_ERROR_CODE(commit_if_full());
    std::memcpy(secondary_keys[cid].last_, c_data.last_, sizeof(c_data.last_));
    std::memcpy(secondary_keys[cid].first_, c_data.first_, sizeof(c_data.first_));
    secondary_keys[cid].cid_ = cid;
    DVLOG(2) << "CID = " << cid << ", LST = " << std::string(c_data.last_, sizeof(c_data.last_))
      << ", P# = " << std::string(c_data.phone_, sizeof(c_data.phone_));

    make_alpha_string(12, 24, h_data.data_);
    h_data.cid_ = cid;
    h_data.c_did_ = did;
    h_data.c_wid_ = wid;
    h_data.wid_ = wid;
    h_data.did_ = did;
    h_data.amount_ = 10.0;
    std::memcpy(h_data.date_, timestamp_.data(), sizeof(h_data.date_));
    WRAP_ERROR_CODE(histories.append_record(context_, &h_data, sizeof(h_data)));
    WRAP_ERROR_CODE(commit_if_full());
  }
  WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &ep));

  // now insert all secondary keys.
  // by sorting them here, we get better insert performance and fill factor.
  debugging::StopWatch sort_watch;
  std::sort(secondary_keys, secondary_keys + kCustomers, Secondary::compare);
  sort_watch.stop();
  LOG(INFO) << "Sorted secondary entries in " << sort_watch.elapsed_us() << "us";
  auto customers_secondary = storages_.customers_secondary_;

  // synchronize insert to customer_secondary
  // std::lock_guard<std::mutex> guard(customer_secondary_mutex);
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
        std::memcpy(key_be + sizeof(Wid) + sizeof(Did), secondary_keys[i].last_, 32);
        Cid* address = reinterpret_cast<Cid*>(key_be + sizeof(Wid) + sizeof(Did) + 32);
        *address = assorted::htobe<Cid>(secondary_keys[i].cid_);
        WRAP_ERROR_CODE(customers_secondary.insert_record(context_, key_be, sizeof(key_be)));
      }
      if (rep == 0) {
        WRAP_ERROR_CODE(xct_manager_->abort_xct(context_));
      } else {
        ErrorCode ret = xct_manager_->precommit_xct(context_, &ep);
        if (ret == kErrorCodeOk) {
          break;
        } else if (ret == kErrorCodeXctRaceAbort) {
          VLOG(0) << "Abort in concurrent customer load. retry";
          --rep;
        } else {
          return ERROR_STACK(ret);
        }
      }
    }
    from += cur_batch_size;
  }
  return kRetOk;
}

ErrorStack TpccLoadTask::load_orders() {
  for (Wid wid = from_wid_; wid < to_wid_; ++wid) {
    for (Did did = 0; did < kDistricts; ++did) {
      CHECK_ERROR(load_orders_in_district(wid, did));
    }
  }
  return kRetOk;
}

ErrorStack TpccLoadTask::load_orders_in_district(Wid wid, Did did) {
  LOG(INFO) << "Loading Orders for D=" << static_cast<int>(did) << ", W= " << wid;
  //  << ": " << engine_->get_memory_manager()->dump_free_memory_stat();
  // Whether the customer id for the current order is already taken.
  bool cid_array[kCustomers];
  std::memset(cid_array, 0, sizeof(cid_array));

  Epoch ep;
  auto neworders = storages_.neworders_;
  auto orders = storages_.orders_;
  auto orders_secondary = storages_.orders_secondary_;
  auto orderlines = storages_.orderlines_;
  OrderData o_data;
  zero_clear(&o_data);
  OrderlineData ol_data[kOlMax];
  std::memset(ol_data, 0, sizeof(ol_data));
  Wdid wdid = combine_wdid(wid, did);
  for (Oid oid = 0; oid < kOrders; ++oid) {
    Wdoid wdoid = combine_wdoid(wdid, oid);
    // unfortunately, this one is vulnerable to aborts due to concurrent loaders.
    // especially when the tree is small, this can happen.

    // Generate Order Data
    Cid o_cid = get_permutation(cid_array);
    Wdcid wdcid = combine_wdcid(wdid, o_cid);
    uint32_t o_carrier_id = rnd_.uniform_within(1, 10);
    uint32_t o_ol_cnt = rnd_.uniform_within(5, 15);

    o_data.cid_ = o_cid;
    o_data.all_local_ = 1;
    o_data.ol_cnt_ = o_ol_cnt;
    std::memcpy(o_data.entry_d_, timestamp_.data(), sizeof(o_data.entry_d_));

    if (oid >= 2100U) {   /* the last 900 orders have not been delivered) */
      o_data.carrier_id_ = 0;
    } else {
      o_data.carrier_id_ = o_carrier_id;
    }

    Wdcoid wdcoid = combine_wdcoid(wdcid, oid);
    DVLOG(2) << "OID = " << oid << ", CID = " << o_cid << ", DID = "
      << static_cast<int>(did) << ", WID = " << wid;
    for (Ol ol = 1; ol <= o_ol_cnt; ol++) {
      // Generate Order Line Data
      make_alpha_string(24, 24, ol_data[ol].dist_info_);
      ol_data[ol].iid_ = rnd_.uniform_within(0, kItems - 1);
      ol_data[ol].supply_wid_ = wid;
      ol_data[ol].quantity_ = 5;
      if (oid >= 2100U) {
        ol_data[ol].amount_ = 0;
      } else {
        ol_data[ol].amount_ = static_cast<float>(rnd_.uniform_within(10L, 10000L)) / 100.0;
        std::memcpy(ol_data[ol].delivery_d_, timestamp_.data(), sizeof(ol_data[ol].delivery_d_));
      }

      DVLOG(2) << "OL = " << ol << ", IID = " << ol_data[ol].iid_ << ", QUAN = "
        << ol_data[ol].quantity_ << ", AMT = " << ol_data[ol].amount_;
    }

    // retry until succeed
    uint32_t successive_aborts = 0;
    while (true) {
      WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
      if (o_data.carrier_id_ == 0) {
        WRAP_ERROR_CODE(neworders.insert_record_normalized(context_, wdoid));
      }
      WRAP_ERROR_CODE(orders.insert_record_normalized(context_, wdoid, &o_data, sizeof(o_data)));
      WRAP_ERROR_CODE(orders_secondary.insert_record_normalized(context_, wdcoid));
      for (Ol ol = 1; ol <= o_ol_cnt; ol++) {
        Wdol wdol = combine_wdol(wdoid, ol);
        WRAP_ERROR_CODE(orderlines.insert_record_normalized(
          context_,
          wdol,
          &(ol_data[ol]),
          sizeof(OrderlineData)));
      }
      ErrorCode ret = xct_manager_->precommit_xct(context_, &ep);
      if (ret == kErrorCodeOk) {
        break;
      } else if (ret == kErrorCodeXctRaceAbort) {
        VLOG(0) << "Abort in concurrent data load. successive_aborts=" << successive_aborts;
        ++successive_aborts;
        if (successive_aborts % 100 == 0) {
          LOG(WARNING) << "Lots of successive aborts: " << successive_aborts << ", thread="
            << context_->get_thread_id();
        }
      } else {
        return ERROR_STACK(ret);
      }
    }
  }
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
  if (max > length) {
    std::memset(str + length, 0, max - length);
  }
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
  if (max > length) {
    std::memset(str + length, 0, max - length);
  }
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
