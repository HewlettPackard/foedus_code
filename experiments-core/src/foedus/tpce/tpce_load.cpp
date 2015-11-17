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
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/hash/hash_id.hpp"
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

  // TODO(Hideaki): Load the 5 rows to trade_types_
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

  const SymbT max_symb_id = scale_.get_security_cardinality();
  const Datetime dts_to = get_current_datetime();
  const Datetime dts_from = dts_to - scale_.initial_trade_days_ * 8U * 3600U;
  uint64_t in_partition_count = 0;
  for (IdentT cid = customer_from; cid < customer_to; ++cid) {
    for (IdentT ordinal = 0; ordinal < kAccountsPerCustomer; ++ordinal) {
      const IdentT ca = to_ca(cid, ordinal);
      for (Datetime dts = dts_from; dts < dts_to; ++dts) {
        const SymbT symb_id = 0;  // TODO(Hideaki) zipfian random [0, max_symb_id)
        const SymbDtsKey secondary_key = to_symb_dts_key(symb_id, dts, partition_id_);
        const TradeT tid = get_new_trade_id(scale_, partition_id_, in_partition_count);
        DVLOG(3) << "tid=" << tid << ", secondary_key=" << secondary_key;

        // TODO(Hideaki): ... Load the trades and symb_dts_index.
        // Use the templated version of insert_record<TradeT> for trades,
        // and use the insert_record_normalized for symb_dts_index.
        // Commit and retry in case of race abort.
        // We might want to consider batching, but not mandatory.
        ++in_partition_count;  // count up only when it didn't abort.
      }
    }
  }
  return kRetOk;
}

}  // namespace tpce
}  // namespace foedus
