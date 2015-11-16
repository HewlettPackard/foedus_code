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
#include "foedus/tpce/tpce_client.hpp"

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
namespace tpce {

ErrorStack tpce_client_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  if (args.input_len_ != sizeof(TpceClientTask::Inputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  if (args.output_buffer_size_ < sizeof(TpceClientTask::Outputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  *args.output_used_ = sizeof(TpceClientTask::Outputs);
  const TpceClientTask::Inputs* inputs
    = reinterpret_cast<const TpceClientTask::Inputs*>(args.input_buffer_);
  TpceClientTask task(*inputs, reinterpret_cast<TpceClientTask::Outputs*>(args.output_buffer_));
  return task.run(context);
}

const uint32_t kMaxUnexpectedErrors = 1;


ErrorStack TpceClientTask::run(thread::Thread* context) {
  context_ = context;
  engine_ = context->get_engine();
  storages_.initialize_tables(engine_);
  channel_ = reinterpret_cast<TpceClientChannel*>(
    engine_->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());

  ErrorStack result = run_impl(context);
  if (result.is_error()) {
    LOG(ERROR) << "TPCE Client-" << worker_id_ << " exit with an error:" << result;
  }
  ++channel_->exit_nodes_;
  return result;
}

ErrorStack TpceClientTask::run_impl(thread::Thread* context) {
  // std::memset(debug_wdcid_access_, 0, sizeof(debug_wdcid_access_));
  // std::memset(debug_wdid_access_, 0, sizeof(debug_wdid_access_));
  CHECK_ERROR(warmup(context));

  outputs_->processed_ = 0;
  outputs_->snapshot_cache_hits_ = 0;
  outputs_->snapshot_cache_misses_ = 0;
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();

  channel_->start_rendezvous_.wait();
  LOG(INFO) << "TPCE Client-" << worker_id_ << " started working!";

  context->reset_snapshot_cache_counts();

  while (!is_stop_requested()) {
    uint16_t transaction_type = rnd_.uniform_within(1, 100);
    const uint16_t kXctTradeOrderPercent = 80;
    // remember the random seed to repeat the same transaction on abort/retry.
    uint64_t rnd_seed = rnd_.get_current_seed();

    // abort-retry loop
    while (!is_stop_requested()) {
      rnd_.set_current_seed(rnd_seed);
      WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
      ErrorCode ret;
      if (transaction_type <= kXctTradeOrderPercent) {
        ret = do_trade_order();
      } else {
        ret = do_trade_update();
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

ErrorStack TpceClientTask::warmup(thread::Thread* /*context*/) {
  // Warmup snapshot cache for read-only tables. Install volatile pages for dynamic tables.

  // so far empty..

  // Warmup done!
  ++(channel_->warmup_complete_counter_);
  return kRetOk;
}

}  // namespace tpce
}  // namespace foedus
