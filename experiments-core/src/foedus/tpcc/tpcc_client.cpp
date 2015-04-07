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
#include "foedus/tpcc/tpcc_client.hpp"

#include <glog/logging.h>

#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/debugging/rdtsc.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace tpcc {

ErrorStack tpcc_client_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  if (args.input_len_ != sizeof(TpccClientTask::Inputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  if (args.output_buffer_size_ < sizeof(TpccClientTask::Outputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  *args.output_used_ = sizeof(TpccClientTask::Outputs);
  const TpccClientTask::Inputs* inputs
    = reinterpret_cast<const TpccClientTask::Inputs*>(args.input_buffer_);
  TpccClientTask task(*inputs, reinterpret_cast<TpccClientTask::Outputs*>(args.output_buffer_));
  return task.run(context);
}


void TpccClientTask::update_timestring_if_needed() {
  uint64_t now = debugging::get_rdtsc();
  if (now  - previous_timestring_update_ > (1ULL << 30)) {
    timestring_.assign(get_current_time_string(ctime_buffer_));
    previous_timestring_update_ = now;
  }
  ASSERT_ND(timestring_.length() > 0);
}

const uint32_t kMaxUnexpectedErrors = 1;


ErrorStack TpccClientTask::run(thread::Thread* context) {
  context_ = context;
  engine_ = context->get_engine();
  storages_.initialize_tables(engine_);
  channel_ = reinterpret_cast<TpccClientChannel*>(
    engine_->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
  tmp_sids_memory_.alloc(
    kMaxOlCount * 21U * sizeof(storage::array::ArrayOffset),
    1U << 21,
    memory::AlignedMemory::kNumaAllocOnnode,
    context->get_numa_node());
  tmp_sids_ = reinterpret_cast<storage::array::ArrayOffset*>(tmp_sids_memory_.get_block());
  tmp_quantities_memory_.alloc(
    kMaxOlCount * 21U * sizeof(uint32_t),
    1U << 21,
    memory::AlignedMemory::kNumaAllocOnnode,
    context->get_numa_node());
  tmp_quantities_ = reinterpret_cast<uint32_t*>(tmp_quantities_memory_.get_block());

  ErrorStack result = run_impl(context);
  if (result.is_error()) {
    LOG(ERROR) << "TPCC Client-" << worker_id_ << " exit with an error:" << result;
  }
  ++channel_->exit_nodes_;
  return result;
}

ErrorStack TpccClientTask::run_impl(thread::Thread* context) {
  // std::memset(debug_wdcid_access_, 0, sizeof(debug_wdcid_access_));
  // std::memset(debug_wdid_access_, 0, sizeof(debug_wdid_access_));
  CHECK_ERROR(warmup(context));

  outputs_->processed_ = 0;
  outputs_->snapshot_cache_hits_ = 0;
  outputs_->snapshot_cache_misses_ = 0;
  timestring_.assign(get_current_time_string(ctime_buffer_));
  ASSERT_ND(timestring_.length() > 0);
  previous_timestring_update_ = debugging::get_rdtsc();
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();

  channel_->start_rendezvous_.wait();
  LOG(INFO) << "TPCC Client-" << worker_id_ << " started working! home wid="
    << from_wid_ << "-" << to_wid_;

  context->reset_snapshot_cache_counts();

  while (!is_stop_requested()) {
    Wid wid = from_wid_;  // home WID. some transaction randomly uses remote WID.
    uint16_t transaction_type = rnd_.uniform_within(1, 100);
    // remember the random seed to repeat the same transaction on abort/retry.
    uint64_t rnd_seed = rnd_.get_current_seed();

    // abort-retry loop
    while (!is_stop_requested()) {
      rnd_.set_current_seed(rnd_seed);
      update_timestring_if_needed();
      xct::IsolationLevel isolation
        = dirty_read_mode_ ? xct::kDirtyReadPreferVolatile : xct::kSerializable;
      WRAP_ERROR_CODE(xct_manager->begin_xct(context, isolation));
      ErrorCode ret;
      if (transaction_type <= kXctNewOrderPercent) {
        ret = do_neworder(wid);
      } else if (transaction_type <= kXctPaymentPercent) {
        ret = do_payment(wid);
      } else if (transaction_type <= kXctOrderStatusPercent) {
        ret = do_order_status(wid);
      } else if (transaction_type <= kXctDelieveryPercent) {
        ret = do_delivery(wid);
      } else {
        ret = do_stock_level(wid);
      }

      if (ret == kErrorCodeOk) {
        ASSERT_ND(!context->is_running_xct());
        break;
      }

      if (context->is_running_xct()) {
        WRAP_ERROR_CODE(xct_manager->abort_xct(context));
      }

      ASSERT_ND(!context->is_running_xct());

      if (ret == kErrorCodeXctUserAbort) {
        // Fine. This is as defined in the spec.
        increment_user_requested_aborts();
        break;
      } else if (ret == kErrorCodeXctRaceAbort) {
        increment_race_aborts();
        continue;
      } else if (ret == kErrorCodeXctPageVersionSetOverflow ||
        ret == kErrorCodeXctPointerSetOverflow ||
        ret == kErrorCodeXctReadSetOverflow ||
        ret == kErrorCodeXctWriteSetOverflow) {
        // this usually doesn't happen, but possible.
        increment_largereadset_aborts();
        continue;
#ifdef OLAP_MODE
      } else if (ret == kErrorCodeFsTooShortRead) {
        // this is an unexpected bug, but only happens in OLAP experiment once in a while.
        // wtf. TODO(Hideaki) figure this out.
        // This seems to happen only when log file moves on to next file.
        LOG(ERROR) << "The kErrorCodeFsTooShortRead error happened while OLAP experiment";
        rnd_seed = rnd_.next_uint64();  // re-roll, and go on. This is rare.
        continue;
#endif  // OLAP_MODE
      } else {
        increment_unexpected_aborts();
        LOG(WARNING) << "Unexpected error: " << get_error_name(ret);
        if (outputs_->unexpected_aborts_ > kMaxUnexpectedErrors) {
          LOG(ERROR) << "Too many unexpected errors. What's happening?" << get_error_name(ret);
          return ERROR_STACK(ret);
        } else {
          continue;
        }
      }
    }

    ++outputs_->processed_;
    if (UNLIKELY(outputs_->processed_ % (1U << 8) == 0)) {  // it's just stats. not too frequent
      outputs_->snapshot_cache_hits_ = context->get_snapshot_cache_hits();
      outputs_->snapshot_cache_misses_ = context->get_snapshot_cache_misses();
    }
  }

  outputs_->snapshot_cache_hits_ = context->get_snapshot_cache_hits();
  outputs_->snapshot_cache_misses_ = context->get_snapshot_cache_misses();
  return kRetOk;
}

ErrorCode TpccClientTask::lookup_customer_by_id_or_name(Wid wid, Did did, Cid *cid) {
  // 60% by name, 40% by ID
  bool by_name = rnd_.uniform_within(1, 100) <= 60;
  if (by_name) {
    char lastname[16];
    generate_lastname(rnd_.non_uniform_within(255, 0, kLnames - 1), lastname);
    CHECK_ERROR_CODE(lookup_customer_by_name(wid, did, lastname, cid));
  } else {
    *cid = rnd_.non_uniform_within(1023, 0, kCustomers - 1);
  }
  return kErrorCodeOk;
}

ErrorCode TpccClientTask::lookup_customer_by_name(Wid wid, Did did, const char* lname, Cid *cid) {
  char low_be[sizeof(Wid) + sizeof(Did) + 16];
  assorted::write_bigendian<Wid>(wid, low_be);
  assorted::write_bigendian<Did>(did, low_be + sizeof(Wid));
  std::memcpy(low_be + sizeof(Wid) + sizeof(Did), lname, 16);

  char high_be[sizeof(Wid) + sizeof(Did) + 16];
  std::memcpy(high_be, low_be, sizeof(high_be));

  // this increment never overflows because it's ASCII.
  ASSERT_ND(high_be[sizeof(high_be) - 1] + 1 > high_be[sizeof(high_be) - 1]);
  ++high_be[sizeof(high_be) - 1];

  storage::masstree::MasstreeCursor cursor(storages_.customers_secondary_, context_);
  CHECK_ERROR_CODE(cursor.open(low_be, sizeof(low_be), high_be, sizeof(high_be)));

  uint8_t cid_count = 0;
  const uint16_t offset = sizeof(Wid) + sizeof(Did) + 32;
  while (cursor.is_valid_record()) {
    const char* key_be = cursor.get_key();
    ASSERT_ND(assorted::betoh<Wid>(*reinterpret_cast<const Wid*>(key_be)) == wid);
    ASSERT_ND(assorted::betoh<Did>(*reinterpret_cast<const Did*>(key_be + sizeof(Wid))) == did);
    ASSERT_ND(std::memcmp(key_be, low_be, sizeof(low_be)) == 0);
    Cid cid = assorted::betoh<Cid>(*reinterpret_cast<const Cid*>(key_be + offset));
    if (UNLIKELY(cid_count >= kMaxCidsPerLname)) {
      return kErrorCodeInvalidParameter;
    }
    tmp_cids_[cid_count] = cid;
    ++cid_count;
    CHECK_ERROR_CODE(cursor.next());
  }

  if (UNLIKELY(cid_count == 0)) {
    return kErrorCodeStrKeyNotFound;
  }

  // take midpoint
  *cid = tmp_cids_[cid_count / 2];
  return kErrorCodeOk;
}

ErrorStack TpccClientTask::warmup(thread::Thread* context) {
  // Warmup snapshot cache for read-only tables. Install volatile pages for dynamic tables.

  // in olap mode, let's selectively prefetch because the data size is huge.
  if (olap_mode_) {
    CHECK_ERROR(warmup_olap(context));
    ++(channel_->warmup_complete_counter_);
    return kRetOk;
  }

  // item has no locality, but still we want to pre-load snapshot cache, so:
  if (channel_->preload_snapshot_pages_) {
    uint64_t items_per_warehouse = kItems / total_warehouses_;
    uint64_t from = items_per_warehouse * from_wid_;
    uint64_t to = items_per_warehouse * to_wid_;
    WRAP_ERROR_CODE(storages_.items_.prefetch_pages(context, false, true, from, to));
  }

  Wid wid_begin = from_wid_;
  Wid wid_end = to_wid_;
  {
    // customers arrays
    Wdcid from = combine_wdcid(combine_wdid(wid_begin, 0), 0);
    Wdcid to = combine_wdcid(combine_wdid(wid_end, 0), 0);
    if (channel_->preload_snapshot_pages_ || olap_mode_) {
      WRAP_ERROR_CODE(storages_.customers_static_.prefetch_pages(context, false, true, from, to));
    }
    if (olap_mode_) {
      WRAP_ERROR_CODE(storages_.customers_dynamic_.prefetch_pages(context, false, true, from, to));
      WRAP_ERROR_CODE(storages_.customers_history_.prefetch_pages(context, false, true, from, to));
    } else {
      WRAP_ERROR_CODE(storages_.customers_dynamic_.prefetch_pages(context, true, false, from, to));
      WRAP_ERROR_CODE(storages_.customers_history_.prefetch_pages(context, true, false, from, to));
    }
  }
  if (channel_->preload_snapshot_pages_ || olap_mode_) {
    // customers secondary
    storage::masstree::KeySlice from = static_cast<storage::masstree::KeySlice>(wid_begin) << 48U;
    storage::masstree::KeySlice to = static_cast<storage::masstree::KeySlice>(wid_end) << 48U;
    WRAP_ERROR_CODE(storages_.customers_secondary_.prefetch_pages_normalized(
      context,
      false,
      true,
      from,
      to));
  }
  {
    // stocks
    Sid from = combine_sid(wid_begin, 0);
    Sid to = combine_sid(wid_end, 0);
    if (olap_mode_) {
      WRAP_ERROR_CODE(storages_.stocks_.prefetch_pages(context, false, true, from, to));
    } else {
      WRAP_ERROR_CODE(storages_.stocks_.prefetch_pages(context, true, false, from, to));
    }
  }
  {
    // order/neworder
    Wdoid from = combine_wdoid(combine_wdid(wid_begin, 0), 0);
    Wdoid to = combine_wdoid(combine_wdid(wid_end, 0), 0);
    if (olap_mode_) {
      WRAP_ERROR_CODE(
        storages_.neworders_.prefetch_pages_normalized(context, false, true, from, to));
      WRAP_ERROR_CODE(storages_.orders_.prefetch_pages_normalized(context, false, true, from, to));
    } else {
      WRAP_ERROR_CODE(
        storages_.neworders_.prefetch_pages_normalized(context, true, false, from, to));
      WRAP_ERROR_CODE(storages_.orders_.prefetch_pages_normalized(context, true, false, from, to));
    }
  }
  {
    // order_secondary
    Wdcoid from = combine_wdcoid(combine_wdcid(combine_wdid(wid_begin, 0), 0), 0);
    Wdcoid to = combine_wdcoid(combine_wdcid(combine_wdid(wid_end, 0), 0), 0);
    WRAP_ERROR_CODE(storages_.orders_secondary_.prefetch_pages_normalized(
      context,
      olap_mode_ ? false : true,
      olap_mode_ ? true : false,
      from,
      to));
  }
  {
    // orderlines
    Wdol from = combine_wdol(combine_wdoid(combine_wdid(wid_begin, 0), 0), 0);
    Wdol to = combine_wdol(combine_wdoid(combine_wdid(wid_end, 0), 0), 0);
    WRAP_ERROR_CODE(storages_.orderlines_.prefetch_pages_normalized(
      context,
      olap_mode_ ? false : true,
      olap_mode_ ? true : false,
      from,
      to));
  }

  WRAP_ERROR_CODE(storages_.warehouses_static_.prefetch_pages(
    context,
    false,
    true,
    wid_begin,
    wid_end));
  WRAP_ERROR_CODE(storages_.warehouses_ytd_.prefetch_pages(
    context,
    olap_mode_ ? false : true,
    olap_mode_ ? true : false,
    wid_begin,
    wid_end));
  {
    Wdid from = combine_wdid(wid_begin, 0);
    Wdid to = combine_wdid(wid_end, 0);
    WRAP_ERROR_CODE(storages_.districts_static_.prefetch_pages(context, false, true, from, to));
    if (olap_mode_) {
      WRAP_ERROR_CODE(storages_.districts_ytd_.prefetch_pages(context, false, true, from, to));
      WRAP_ERROR_CODE(storages_.districts_next_oid_.prefetch_pages(context, false, true, from, to));
    } else {
      WRAP_ERROR_CODE(storages_.districts_ytd_.prefetch_pages(context, true, false, from, to));
      WRAP_ERROR_CODE(storages_.districts_next_oid_.prefetch_pages(context, true, false, from, to));
    }
  }

  // Warmup done!
  ++(channel_->warmup_complete_counter_);
  return kRetOk;
}

ErrorStack TpccClientTask::warmup_olap(thread::Thread* context) {
  // we so far use only order_status in OLAP mode. so, just prefetch that part
  if (storages_.orderlines_.get_metadata()->root_snapshot_page_id_ == 0) {
    LOG(INFO) << "No warmup needed for volatile OLAP experiment";
    return kRetOk;
  }

  Wid wid_begin = from_wid_;
  Wid wid_end = to_wid_;

  LOG(INFO) << "Client-" << from_wid_ << " OLAP Mode Warmup. customers secondary...";
  {
    // customers secondary
    storage::masstree::KeySlice from = static_cast<storage::masstree::KeySlice>(wid_begin) << 48U;
    storage::masstree::KeySlice to = static_cast<storage::masstree::KeySlice>(wid_end) << 48U;
    WRAP_ERROR_CODE(storages_.customers_secondary_.prefetch_pages_normalized(
      context,
      false,
      true,
      from,
      to));
  }
  LOG(INFO) << "Client-" << from_wid_ << " OLAP Mode Warmup. order_secondary...";
  {
    // order_secondary
    Wdcoid from = combine_wdcoid(combine_wdcid(combine_wdid(wid_begin, 0), 0), 0);
    Wdcoid to = combine_wdcoid(combine_wdcid(combine_wdid(wid_end, 0), 0), 0);
    WRAP_ERROR_CODE(storages_.orders_secondary_.prefetch_pages_normalized(
      context,
      false,
      true,
      from,
      to));
  }
  LOG(INFO) << "Client-" << from_wid_ << " OLAP Mode Warmup. orderlines...";
  {
    // orderlines. in OLAP experiment, this is HUGE.
    // let's report something for each 10k tuples or something.
    // it will take longer, instead.
    xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSnapshot));
    Wdol from = combine_wdol(combine_wdoid(combine_wdid(wid_begin, 0), 0), 0);
    Wdol to = combine_wdol(combine_wdoid(combine_wdid(wid_end, 0), 0), 0);
    storage::masstree::MasstreeCursor cursor(storages_.orderlines_, context);
    WRAP_ERROR_CODE(cursor.open_normalized(from, to));
    uint32_t cnt = 0;
    debugging::StopWatch watch;
    while (cursor.is_valid_record()) {
      ASSERT_ND(assorted::read_bigendian<Wdol>(cursor.get_key()) >= from);
      ASSERT_ND(assorted::read_bigendian<Wdol>(cursor.get_key()) < to);
      ASSERT_ND(cursor.get_key_length() == sizeof(Wdol));
      ASSERT_ND(cursor.get_payload_length() == sizeof(OrderlineData));
      ++cnt;
      if ((cnt % (1U << 19)) == 0) {
        LOG(INFO) << "Client-" << from_wid_ << " OLAP Mode Warmup. orderlines-" << cnt
          << ", elapsed so far = " << watch.peek_elapsed_ns() << "ns";
      }
      WRAP_ERROR_CODE(cursor.next());
    }
    WRAP_ERROR_CODE(xct_manager->abort_xct(context));
    watch.stop();
    LOG(INFO) << "Client-" << from_wid_ << " OLAP Mode Warmup. read " << cnt << " orderlines"
      << " in " << watch.elapsed_ms() << "ms";
    /*
    WRAP_ERROR_CODE(storages_.orderlines_.prefetch_pages_normalized(
      context,
      false,
      true,
      from,
      to));
    */
  }
  LOG(INFO) << "Client-" << from_wid_ << " OLAP Mode Warmup done.";
  return kRetOk;
}

}  // namespace tpcc
}  // namespace foedus
