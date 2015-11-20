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
#include "foedus/tpce/tpce_load.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/epoch.hpp"
#include "foedus/assorted/zipfian_random.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/tpce/tpce.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"


namespace foedus {
namespace tpce {
ErrorStack tpce_finishup_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  if (args.input_len_ != sizeof(TpceFinishupTask::Inputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  const TpceFinishupTask::Inputs* input
    = reinterpret_cast<const TpceFinishupTask::Inputs*>(args.input_buffer_);
  TpceFinishupTask task(*input);
  return task.run(context);
}

ErrorStack tpce_load_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  if (args.input_len_ != sizeof(TpceLoadTask::Inputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  const TpceLoadTask::Inputs* inputs = reinterpret_cast<const TpceLoadTask::Inputs*>(
    args.input_buffer_);
  TpceLoadTask task(inputs->scale_, inputs->partition_id_);
  return task.run(context);
}

storage::masstree::SlotIndex estimate_masstree_records(
  uint8_t layer,
  storage::masstree::KeyLength key_length,
  storage::masstree::PayloadLength payload_length) {
  return storage::masstree::MasstreeStorage::estimate_records_per_page(
    layer,
    key_length,
    payload_length);
}

ErrorStack create_all(Engine* engine, const TpceScale& scale) {
  debugging::StopWatch watch;

  LOG(INFO) << "Initial:" << engine->get_memory_manager()->dump_free_memory_stat();

  uint64_t trade_cardinality
    = scale.calculate_initial_trade_cardinality();
  uint16_t trade_max_record_per_page
    = storage::hash::kHashDataPageDataSize / sizeof(TradeData);
  double trade_fill_factor = 0.2;  // 20% fill factor to leave room for growth
  CHECK_ERROR(create_hash(
    engine,
    "trades",
    true,
    trade_cardinality,
    trade_max_record_per_page * trade_fill_factor));

  CHECK_ERROR(create_masstree(
    engine,
    "trades_secondary_symb_dts",
    true,
    estimate_masstree_records(0, sizeof(SymbDtsKey), sizeof(TradeT)) * 0.75,
    0));

  CHECK_ERROR(create_array(
    engine,
    "trade_types",
    true,
    sizeof(TradeTypeData),
    TradeTypeData::kCount));

  watch.stop();
  LOG(INFO) << "Created TPC-E tables in " << watch.elapsed_sec() << "sec";
  return kRetOk;
}


ErrorStack create_array(
  Engine* engine,
  const storage::StorageName& name,
  bool keep_all_volatile_pages,
  uint32_t payload_size,
  uint64_t array_size) {
  Epoch ep;
  storage::array::ArrayMetadata meta(name, payload_size, array_size);
  if (keep_all_volatile_pages) {
    meta.snapshot_thresholds_.snapshot_keep_threshold_ = 0xFFFFFFFFU;
    meta.snapshot_drop_volatile_pages_threshold_ = 8;
  } else {
    meta.snapshot_thresholds_.snapshot_keep_threshold_ = 0;
    meta.snapshot_drop_volatile_pages_threshold_
      = storage::array::ArrayMetadata::kDefaultSnapshotDropVolatilePagesThreshold;
  }

  return engine->get_storage_manager()->create_storage(&meta, &ep);
}

ErrorStack create_hash(
  Engine* engine,
  const storage::StorageName& name,
  bool keep_all_volatile_pages,
  uint64_t expected_records,
  double preferred_records_per_bin) {
  Epoch ep;
  storage::hash::HashMetadata meta(name);
  meta.set_capacity(expected_records, preferred_records_per_bin);
  if (keep_all_volatile_pages) {
    meta.snapshot_thresholds_.snapshot_keep_threshold_ = 0xFFFFFFFFU;
  } else {
    meta.snapshot_thresholds_.snapshot_keep_threshold_ = 0;
  }

  return engine->get_storage_manager()->create_storage(&meta, &ep);
}

ErrorStack create_masstree(
  Engine* engine,
  const storage::StorageName& name,
  bool keep_all_volatile_pages,
  float border_fill_factor,
  storage::masstree::Layer min_layer_hint) {
  Epoch ep;
  storage::masstree::MasstreeMetadata meta(name, border_fill_factor);
  meta.min_layer_hint_ = min_layer_hint;
  if (keep_all_volatile_pages) {
    meta.snapshot_thresholds_.snapshot_keep_threshold_ = 0xFFFFFFFFU;
    meta.snapshot_drop_volatile_pages_btree_levels_ = 0;
    meta.snapshot_drop_volatile_pages_layer_threshold_ = 8;
  } else {
    meta.snapshot_thresholds_.snapshot_keep_threshold_ = 0;
    meta.snapshot_drop_volatile_pages_btree_levels_
      = storage::masstree::MasstreeMetadata::kDefaultDropVolatilePagesBtreeLevels;
    meta.snapshot_drop_volatile_pages_layer_threshold_ = 0;
  }

  return engine->get_storage_manager()->create_storage(&meta, &ep);
}

ErrorStack create_sequential(Engine* engine, const storage::StorageName& name) {
  Epoch ep;
  storage::sequential::SequentialMetadata meta(name);
  return engine->get_storage_manager()->create_storage(&meta, &ep);
}

ErrorStack TpceFinishupTask::run(thread::Thread* context) {
  Engine* engine = context->get_engine();
  storages_.initialize_tables(engine);
// let's do this even in release. good to check abnormal state
// #ifndef NDEBUG
  if (inputs_.fatify_masstree_) {
    // assure some number of direct children in root page to make partition more efficient.
    // a better solution for partitioning is to consider children, not just root. later, later, ...
    LOG(INFO) << "FAT. FAAAT. FAAAAAAAAAAAAAAAAAT";
    uint32_t desired = 32;  // context->get_engine()->get_soc_count();  // maybe 2x?
    CHECK_ERROR(storages_.trades_secondary_symb_dts_.fatify_first_root(context, desired));
  }

  if (inputs_.skip_verify_) {
    LOG(INFO) << "oh boy. are you going to skip verification?";
  } else {
    WRAP_ERROR_CODE(engine->get_xct_manager()->begin_xct(context, xct::kSerializable));
    // to speedup experiments, skip a few storages' verify() if they are static storages.
    // TASK(Hideaki) make verify() checks snapshot pages too.
    CHECK_ERROR(storages_.trades_.verify_single_thread(context));
    CHECK_ERROR(storages_.trades_secondary_symb_dts_.verify_single_thread(context));
    CHECK_ERROR(storages_.trade_types_.verify_single_thread(context));
    WRAP_ERROR_CODE(engine->get_xct_manager()->abort_xct(context));
  }
// #endif  // NDEBUG

  LOG(INFO) << "Loaded all tables. Waiting for flushing all logs...";
  Epoch ep = engine->get_xct_manager()->get_current_global_epoch();
  engine->get_xct_manager()->advance_current_global_epoch();
  WRAP_ERROR_CODE(engine->get_log_manager()->wait_until_durable(ep));
  LOG(INFO) << "Okay, flushed all logs.";
  return kRetOk;
}

ErrorStack TpceLoadTask::run(thread::Thread* context) {
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

ErrorStack TpceLoadTask::load_tables() {
  CHECK_ERROR(load_trade_types());
  CHECK_ERROR(load_trades());
  VLOG(0) << "Loaded tables:" << engine_->get_memory_manager()->dump_free_memory_stat();
  return kRetOk;
}

ErrorCode TpceLoadTask::commit_if_full() {
  if (context_->get_current_xct().get_write_set_size() >= kCommitBatch) {
    Epoch commit_epoch;
    CHECK_ERROR_CODE(xct_manager_->precommit_xct(context_, &commit_epoch));
    CHECK_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kDirtyRead));
  }
  return kErrorCodeOk;
}

ErrorStack TpceLoadTask::load_trade_types() {
  // TRADE_TYPE is a tiny table. just the first worker loads it.
  if (partition_id_ > 0) {
    return kRetOk;
  }

  WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));

  CHECK_ERROR(load_trade_types_one_row(TradeTypeData::kTlb, "TLB", "Limit-Buy", false, false));
  CHECK_ERROR(load_trade_types_one_row(TradeTypeData::kTls, "TLS", "Limit-Sell", true, false));
  CHECK_ERROR(load_trade_types_one_row(TradeTypeData::kTmb, "TMB", "Market-Buy", false, true));
  CHECK_ERROR(load_trade_types_one_row(TradeTypeData::kTms, "TMS", "Market-Sell", true, true));
  CHECK_ERROR(load_trade_types_one_row(TradeTypeData::kTsl, "TSL", "Stop-Loss", true, false));

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager_->precommit_xct(context_, &commit_epoch));
  return kRetOk;
}

ErrorStack TpceLoadTask::load_trade_types_one_row(
  uint16_t index,
  const char* id,
  const char* name,
  bool is_sell,
  bool is_mrkt) {
  TradeTypeData data;
  std::memcpy(data.id_, id, sizeof(data.id_));
  std::memcpy(data.name_, name, sizeof(data.name_));
  data.is_sell_ = is_sell;
  data.is_mrkt_ = is_mrkt;
  WRAP_ERROR_CODE(storages_.trade_types_.overwrite_record(context_, index, &data));
  return kRetOk;
}

ErrorStack TpceLoadTask::load_trades() {
  LOG(INFO) << "Loading TRADE for partition=" << partition_id_;
  Epoch ep;
  auto trades = storages_.trades_;
  auto symb_dts_index = storages_.trades_secondary_symb_dts_;

  // This partition loads initial trade records for the following customers.
  // This doesn't mean transactions on workers are naturally
  // partitioned by customer. They are random and touch all customers.
  const IdentT customers_per_pertition = scale_.customers_ / scale_.total_partitions_;
  const IdentT customer_from = customers_per_pertition * partition_id_;
  const IdentT customer_to =
    (partition_id_ + 1U == scale_.total_partitions_
      ? scale_.customers_
      : customer_from + customers_per_pertition);
  const IdentT customer_count = customer_to - customer_from;

  // const SymbT max_symb_id = scale_.get_security_cardinality();
  const Datetime dts_to = get_current_datetime();
  const uint64_t in_partition_count
    = scale_.calculate_initial_trade_cardinality() / scale_.total_partitions_;
  ASSERT_ND(in_partition_count > 0);

  // For faster data loading, we sort secondary key in a thread-private array
  // then insert. We might want to consider batching, but not mandatory.
  struct SecondaryKeyValue {
    SymbDtsKey key_;
    TradeT value_;
    bool operator<(const SecondaryKeyValue& rhs) const { return key_ < rhs.key_; }
  };
  memory::AlignedMemory secondary_buffer;
  const uint64_t secondary_size = sizeof(SecondaryKeyValue) * in_partition_count;
  LOG(INFO) << "Data Loader-" << partition_id_ << " allocating "
    << (secondary_size / 1000000.0) << " MBs for private sort buffer...";
  secondary_buffer.alloc_onnode(secondary_size, 1U << 21, context_->get_numa_node());
  if (secondary_buffer.is_null()) {
    return ERROR_STACK_MSG(kErrorCodeOutofmemory, "We need more hugepages for secondary buffer.");
  }

  SecondaryKeyValue* secondary_array
    = reinterpret_cast<SecondaryKeyValue*>(secondary_buffer.get_block());
  assorted::ZipfianRandom symbol_rnd(
    scale_.get_security_cardinality(),
    scale_.symbol_skew_,
    partition_id_);
  LOG(INFO) << "Data Loader-" << partition_id_ << " started loading TRADE.";

  // insert to the main TRADE storage first. The key (TradeT) is nicely sorted here,
  // no need for additional sorting.
  debugging::StopWatch main_watch;
  const uint64_t kPrimaryBatchSize = 64;
  TradeData primary_array[kPrimaryBatchSize];
  for (uint64_t batch = 0; batch * kPrimaryBatchSize < in_partition_count; ++batch) {
    const uint64_t batch_from = batch * kPrimaryBatchSize;
    const uint64_t batch_to
      = std::min<uint64_t>((batch + 1ULL) * kPrimaryBatchSize, in_partition_count);
    if (batch % (1U << 10) == 0) {
      LOG(INFO) << "Data Loader-" << partition_id_ << " loading TRADE... "
        << batch_from << "/" << in_partition_count;
    }

    for (uint64_t i = batch_from; i < batch_to; ++i) {
      const Datetime dts = dts_to - in_partition_count + i;
      const IdentT cid = (rnd_.next_uint32() % customer_count) + customer_from;
      const IdentT ordinal = 0;  // always use the first account of the customer
      const IdentT ca = to_ca(cid, ordinal);
      const SymbT symb_id = symbol_rnd.next();
      const SymbDtsKey secondary_key = to_symb_dts_key(symb_id, dts, partition_id_);
      const TradeT tid = get_new_trade_id(scale_, partition_id_, i);
      DVLOG(3) << "tid=" << tid << ", secondary_key=" << secondary_key;
      secondary_array[i].key_ = secondary_key;
      secondary_array[i].value_ = tid;

      // Load the trades and symb_dts_index.
      TradeData& record = primary_array[i - batch_from];
      record.dts_ = dts;
      record.id_ = tid;
      uint32_t type_index = rnd_.next_uint32() % TradeTypeData::kCount;
      const char* type_id = TradeTypeData::generate_type_id(type_index);
      std::memcpy(record.tt_id_, type_id, sizeof(record.tt_id_));
      record.symb_id_ = symb_id;
      record.ca_id_ = ca;
      record.tax_ = 0;
      record.lifo_ = false;
      record.trade_price_ = 0;

      // Followings should be also random, but we hard code them for now.
      std::memcpy(record.st_id_, "ACTV", sizeof(record.st_id_));
      record.is_cash_ = true;
      record.qty_ = 10;
      record.bid_price_ = 10000;
      std::memcpy(
        record.exec_name_,
        "01234567890123456789012345678901234567890123456789",
        sizeof(record.exec_name_));
      record.comm_ = 100;
      record.chrg_ = 100;
    }

    // Retry in case of race abort.
    while (true) {
      WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));

      bool has_error = false;
      for (uint64_t i = batch_from; i < batch_to; ++i) {
        const TradeData& record = primary_array[i - batch_from];
        ErrorCode ret = trades.insert_record<TradeT>(context_, record.id_, &record, sizeof(record));
        if (ret == kErrorCodeXctRaceAbort) {
          LOG(WARNING) << "oops, race abort during load 1. retry..";
          has_error = true;
          if (context_->is_running_xct()) {
            WRAP_ERROR_CODE(xct_manager_->abort_xct(context_));
          }
          break;
        }
        WRAP_ERROR_CODE(ret);
      }

      if (has_error) {
        continue;
      }
      Epoch commit_epoch;
      ErrorCode ret = xct_manager_->precommit_xct(context_, &commit_epoch);
      if (ret == kErrorCodeXctRaceAbort) {
        LOG(WARNING) << "oops, race abort during load 3. retry..";
        continue;
      }
      WRAP_ERROR_CODE(ret);
      break;
    }
  }

  main_watch.stop();
  LOG(INFO) << "Data Loader-" << partition_id_ << " inserted to main TRADE storage in "
    << main_watch.elapsed_sec() << " sec."
    << " now pre-sorting secondary keys before insertion...";
  debugging::StopWatch sort_watch;
  std::sort(secondary_array, secondary_array + in_partition_count);
  sort_watch.stop();
  LOG(INFO) << "Data Loader-" << partition_id_ << " pre-sorted TRADE secondary key in "
    << main_watch.elapsed_sec() << " sec."
    << " now inserting the secondary index...";

  debugging::StopWatch index_watch;
  // Batch insert them. These key/values are small.
  const uint64_t kSecondaryBatchSize = 128;
  for (uint64_t batch = 0; batch * kSecondaryBatchSize < in_partition_count; ++batch) {
    const uint64_t batch_from = batch * kSecondaryBatchSize;
    const uint64_t batch_to
      = std::min<uint64_t>((batch + 1ULL) * kSecondaryBatchSize, in_partition_count);

    // Retry in case of race abort.
    while (true) {
      WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));

      bool has_error = false;
      for (uint64_t i = batch_from; i < batch_to; ++i) {
        const SecondaryKeyValue& kv = secondary_array[i];
        ErrorCode ret = symb_dts_index.insert_record_normalized(
          context_,
          kv.key_,
          &kv.value_,
          sizeof(kv.value_));
        if (ret == kErrorCodeXctRaceAbort) {
          LOG(WARNING) << "oops, race abort during index-load 1. retry..";
          has_error = true;
          if (context_->is_running_xct()) {
            WRAP_ERROR_CODE(xct_manager_->abort_xct(context_));
          }
          break;
        }
        WRAP_ERROR_CODE(ret);
      }
      if (has_error) {
        continue;
      }

      Epoch commit_epoch;
      ErrorCode ret = xct_manager_->precommit_xct(context_, &commit_epoch);
      if (ret == kErrorCodeXctRaceAbort) {
        LOG(WARNING) << "oops, race abort during index-load 2. retry..";
        continue;
      }
      WRAP_ERROR_CODE(ret);
      break;
    }
  }

  index_watch.stop();
  LOG(INFO) << "Data Loader-" << partition_id_ << " inserted to TRADE's secondary index in"
    << main_watch.elapsed_sec() << " sec.";
  return kRetOk;
}

}  // namespace tpce
}  // namespace foedus
