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
#include <stdint.h>
#include <valgrind.h>  // just for RUNNING_ON_VALGRIND macro.
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
 * @file test_masstree_grow_sorted_race.cpp
 * This test case is similar to test_masstree_grow_race.cpp.
 * This one also tries to reproduce a bug around masstree growth.
 * The difference is that each thread in this testcase inserts keys in sorted order
 * rather than randomly as in test_masstree_grow_race.cpp.
 *
 * This reproduces bugs around no-record-split and append-only path of intermediate page split.
 * The bug was initially observed in ORDERLINE table of TPCC, which has 8-byte keys only.
 * This testcase thus uses 8-byte keys only, too.
 */
namespace foedus {
namespace storage {
namespace masstree {
DEFINE_TEST_CASE_PACKAGE(MasstreeGrowSortedRaceTest, foedus.storage.masstree);

/** Smaller to simulate Orderline. */
const uint32_t kPayload = 64U;

/** insertions per thread */
#ifndef NDEBUG
const uint32_t kRecords = 1U << 8;
#else  // NDEBUG
const uint32_t kRecords = 1U << 14;
// const uint32_t kRecords = 1U << 18;  // to really reproduce it.
#endif  // NDEBUG

/**
 * to make the execution time reasonable, we limit thread/socket count to this number.
 * Also, we do not run this test on valgrind.
 */
#ifndef NDEBUG
const uint32_t kMaxThreadsPerGroup = 1U;
#else  // NDEBUG
const uint32_t kMaxThreadsPerGroup = 2U;
#endif  // NDEBUG
const uint32_t kMaxThreadGroups = 4U;
// const uint32_t kMaxThreadsPerGroup = 8U;  // to really reproduce
// const uint32_t kMaxThreadGroups = 16U;  // to really reproduce

const StorageName kName("test");

const uint32_t kReps = 1U;
// const uint32_t kReps = 16U;  // to really reproduce it. don't enable this in committed code.

class TpccLoadTask {
 public:
  ErrorStack          run(thread::Thread* context);

  Engine* engine_;
  thread::Thread* context_;
  xct::XctManager* xct_manager_;
  uint32_t load_threads_;
  uint32_t thread_ordinal_;

  assorted::UniformRandom rnd_;

  /** Loads the Orderline Table */
  ErrorStack load_orderlines();
};

ErrorStack TpccLoadTask::run(thread::Thread* context) {
  context_ = context;
  engine_ = context->get_engine();
  xct_manager_ = engine_->get_xct_manager();
  load_threads_ = engine_->get_options().thread_.get_total_thread_count();
  thread_ordinal_ = context->get_thread_global_ordinal();
  ASSERT_ND(thread_ordinal_ < load_threads_);
  rnd_.set_current_seed(thread_ordinal_);
  LOG(INFO) << "Load Thread-" << thread_ordinal_ << " start";
  debugging::StopWatch watch;
  CHECK_ERROR(load_orderlines());
  watch.stop();
  LOG(INFO) << "Load-Thread-" << thread_ordinal_ << " done in " << watch.elapsed_sec() << "sec";
  return kRetOk;
}

ErrorStack TpccLoadTask::load_orderlines() {
  Epoch ep;
  MasstreeStorage storage(engine_, kName);
  const uint32_t kCommitBatch = 100;
  char payload[4096U];
  std::memset(payload, 0, sizeof(payload));
  KeySlice cur_slice = kRecords * thread_ordinal_;
  for (uint32_t i = 0; i < kRecords;) {
    uint32_t cur_batch_size = std::min<uint32_t>(kCommitBatch, kRecords - i);
    WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
    for (uint32_t j = 0; j < cur_batch_size; ++j) {
      WRAP_ERROR_CODE(storage.insert_record_normalized(context_, cur_slice, payload, kPayload));
      ++cur_slice;
    }
    WRAP_ERROR_CODE(xct_manager_->abort_xct(context_));
    i += cur_batch_size;
    if ((i % 1000) == 0) {
      std::cout << "Thread-" << thread_ordinal_ << " " << i << "/" << kRecords << std::endl;
    }
  }
  return kRetOk;
}

ErrorStack create_masstree(Engine* engine) {
  Epoch ep;
  MasstreeMetadata meta(kName);
  MasstreeStorage storage;
  EXPECT_FALSE(storage.exists());
  CHECK_ERROR(engine->get_storage_manager()->create_masstree(&meta, &storage, &ep));
  EXPECT_TRUE(storage.exists());
  return kRetOk;
}

ErrorStack tpcc_load_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  return TpccLoadTask().run(context);
}

ErrorStack verify_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  Engine* engine = args.engine_;
  MasstreeStorage storage(engine, kName);
  WRAP_ERROR_CODE(engine->get_xct_manager()->begin_xct(context, xct::kSnapshot));
  CHECK_ERROR(storage.verify_single_thread(context));
  WRAP_ERROR_CODE(engine->get_xct_manager()->abort_xct(context));
  // CHECK_ERROR(storage.debugout_single_thread(engine, true, 0xFFFFFF));
  return kRetOk;
}

void run_test() {
  if (RUNNING_ON_VALGRIND) {
    std::cout << "This testcase takes too long on Valgrind. exit" << std::endl;
    return;
  }
  EngineOptions options = get_tiny_options();
  EngineOptions native_options;
  std::cout << "kRecords=" << kRecords << std::endl;
  std::cout << "Physical core count = "
    << native_options.thread_.get_total_thread_count() << std::endl;
  options.thread_.group_count_
    = std::min<uint16_t>(native_options.thread_.group_count_, kMaxThreadGroups);
  options.thread_.thread_count_per_group_
    = std::min<uint16_t>(native_options.thread_.thread_count_per_group_, kMaxThreadsPerGroup);
  uint32_t test_core_count = options.thread_.get_total_thread_count();

  uint64_t kSplitMargin = 3;  // splits require tentative pages
  uint32_t vol_mb = 2U * test_core_count + 200;
  uint32_t records_per_border = (1U << 11) / kPayload;
  uint32_t total_borders = (test_core_count * kRecords) / records_per_border;
  uint32_t border_kb = total_borders * 4U * kSplitMargin;
  vol_mb += border_kb / 1024ULL;
  vol_mb *= 2;  // for rigorous_page_check
  std::cout << "vol_mb=" << vol_mb << std::endl;
  options.memory_.page_pool_size_mb_per_node_ = vol_mb;
  options.cache_.snapshot_cache_size_mb_per_node_ = 2 * test_core_count;

  Engine engine(options);
  engine.get_proc_manager()->pre_register("verify_task", verify_task);
  engine.get_proc_manager()->pre_register("tpcc_load_task", tpcc_load_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    engine.get_debug()->set_debug_log_min_threshold(
       debugging::DebuggingOptions::kDebugLogWarning);  // otherwise too many
    COERCE_ERROR(create_masstree(&engine));
    std::vector< thread::ImpersonateSession > sessions;
    for (uint32_t i = 0; i < test_core_count; ++i) {
      thread::ImpersonateSession session;
      bool ret = engine.get_thread_pool()->impersonate(
        "tpcc_load_task",
        nullptr,
        0,
        &session);
      EXPECT_TRUE(ret);
      ASSERT_ND(ret);
      LOG(INFO) << "session-" << i << ":" << session;
      sessions.emplace_back(std::move(session));
    }
    for (uint16_t i = 0; i < sessions.size(); ++i) {
      thread::ImpersonateSession& session = sessions[i];
      ErrorStack result = session.get_result();
      COERCE_ERROR(result);
      session.release();
    }
    engine.get_debug()->set_debug_log_min_threshold(debugging::DebuggingOptions::kDebugLogInfo);
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(MasstreeGrowSortedRaceTest, Contended) {
  for (uint32_t i = 0; i < kReps; ++i) {
    run_test();
  }
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreeGrowSortedRaceTest, foedus.storage.masstree);
