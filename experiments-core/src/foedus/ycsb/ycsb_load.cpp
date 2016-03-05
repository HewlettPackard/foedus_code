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

#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/wait.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"
#include "foedus/ycsb/ycsb.hpp"

namespace foedus {
namespace ycsb {
#ifndef YCSB_HASH_STORAGE
ErrorStack ycsb_load_verify_task(const proc::ProcArguments& args) {
  LOG(INFO) << "[YCSB] Verifying loaded data";
  thread::Thread* context = args.context_;
  Engine* engine = context->get_engine();
  auto user_table = engine->get_storage_manager()->get_masstree("ycsb_user_table");
  auto* xct_manager = engine->get_xct_manager();
  COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  storage::masstree::MasstreeCursor cursor(user_table, context);
  COERCE_ERROR_CODE(cursor.open());
  while (cursor.is_valid_record()) {
    auto len = cursor.get_key_length();
    if (len == 0 || len > kKeyMaxLength) {
      COERCE_ERROR_CODE(kErrorCodeStrMasstreeFailedVerification);
    }
    len = cursor.get_payload_length();
    if (len != kFields * kFieldLength) {
      COERCE_ERROR_CODE(kErrorCodeStrMasstreeFailedVerification);
    }
    cursor.next();
  }
  Epoch commit_epoch;
  COERCE_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  COERCE_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  LOG(INFO) << "[YCSB] Data verified";
  return kRetOk;
}
#endif

ErrorStack ycsb_load_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  if (args.input_len_ != sizeof(YcsbLoadTask::Inputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  YcsbLoadTask task;
  const YcsbLoadTask::Inputs* inputs =
    reinterpret_cast<const YcsbLoadTask::Inputs*>(args.input_buffer_);
  return task.run(
    context,
    inputs->load_node_,
    inputs->user_records_per_thread_,
    inputs->extra_records_per_thread_,
    inputs->sort_load_keys_,
    inputs->user_table_spread_,
    inputs->extra_table_spread_);
}

ErrorStack YcsbLoadTask::run(
  thread::Thread* context,
  uint16_t node,
  uint64_t user_records_per_thread,
  uint64_t extra_records_per_thread,
  bool sort_load_keys,
  bool user_table_spread,
  bool extra_table_spread) {
  Engine* engine = context->get_engine();
#ifdef YCSB_HASH_STORAGE
  auto user_table = engine->get_storage_manager()->get_hash("ycsb_user_table");
  auto extra_table = engine->get_storage_manager()->get_hash("ycsb_extra_table");
#else
  auto user_table = engine->get_storage_manager()->get_masstree("ycsb_user_table");
  auto extra_table = engine->get_storage_manager()->get_masstree("ycsb_extra_table");
#endif

  // Now populate the table, round-robin for each worker id (as the high bits) in my group.
  debugging::StopWatch watch;
  auto& options = engine->get_options();
  uint64_t user_inserted = 0;
  uint64_t extra_inserted = 0;
  std::vector<YcsbKey> user_keys;
  std::vector<YcsbKey> extra_keys;

  // Insert (equal number of) records on behalf of each worker
  if (user_table_spread || node == 0) {
    for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ordinal++) {
      auto remaining_user_inserts = user_records_per_thread;
      uint32_t high = node * options.thread_.thread_count_per_group_ + ordinal, low = 0;
      YcsbKey key;
      while (remaining_user_inserts) {
        key.build(high, low++);
        user_keys.push_back(key);
        user_inserted++;
        --remaining_user_inserts;
      }
      ASSERT_ND(remaining_user_inserts == 0);
      if (!user_table_spread) {  // do it on behalf of only one worker
        break;
      }
    }
  }

  if (extra_table_spread || node == 0) {
    for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ordinal++) {
      if (!extra_table_spread && ordinal != 0) {
        break;
      }
      auto remaining_extra_inserts = extra_records_per_thread;
      uint32_t high = node * options.thread_.thread_count_per_group_ + ordinal, low = 0;
      YcsbKey key;
      while (remaining_extra_inserts) {
        key.build(high, low++);
        extra_keys.push_back(key);
        extra_inserted++;
        --remaining_extra_inserts;
      }
      ASSERT_ND(remaining_extra_inserts == 0);
      if (!extra_table_spread) {  // do it on behalf of only one worker
        break;
      }
    }
  }

  if (sort_load_keys) {
    ASSERT_ND(user_keys.size());
    std::sort(user_keys.begin(), user_keys.end());
    std::sort(extra_keys.begin(), extra_keys.end());
  }

  if (user_keys.size()) {
    COERCE_ERROR(load_table(context, user_keys, &user_table));
  }
  if (extra_keys.size()) {
    COERCE_ERROR(load_table(context, extra_keys, &extra_table));
  }

  watch.stop();
  LOG(INFO) << "[YCSB] Loaded " << user_inserted << " user table records, "
    << extra_inserted << " extra table records in " << watch.elapsed_sec() << "s";
  return kRetOk;
}

ErrorStack YcsbLoadTask::load_table(
  thread::Thread* context,
  std::vector<YcsbKey>& keys,
#ifndef YCSB_HASH_STORAGE
  storage::masstree::MasstreeStorage* table) {
#else
  storage::hash::HashStorage* table) {
#endif
  Engine* engine = context->get_engine();
  auto* xct_manager = engine->get_xct_manager();
  Epoch commit_epoch;

  for (auto &key : keys) {
    YcsbRecord r('a');
    bool succeeded = false;
    // With the try-retry rw-lock insertions during loading phase might
    // fail too due to conflict with page splits caused by other loading
    // threads. The splitters will retry until success.
    //
    // Retry 100 times (might be large, but it's loading phase anyway).
    // If we're super unlucky after 100 tries, sleep, retry 100 times again.
    const int kLoadingBackoffUs = 200;
    const int kLoadingInitialTries = 100;
    do {
      for (int t = 0; t < kLoadingInitialTries; ++t) {
        COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
        COERCE_ERROR_CODE(
          table->insert_record(context, key.ptr(), key.size(), &r, sizeof(r)));
        auto ret = xct_manager->precommit_xct(context, &commit_epoch);
        if (ret == kErrorCodeOk) {
          succeeded = true;
          break;
        }
        ASSERT_ND(ret == kErrorCodeXctRaceAbort || ret == kErrorCodeXctLockAbort);
        // sleep for a random number of ms
        std::this_thread::sleep_for(std::chrono::microseconds(kLoadingBackoffUs));
      }
    } while (!succeeded);
  }
  COERCE_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

}  // namespace ycsb
}  // namespace foedus
