/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
 * @file test_masstree_grow_race.cpp
 * This testcase is to specifically reproduce a deadlock bug that happened when
 * a large number of threads grow a multi-layer Mastree.
 * It happens occasionally, when many threads are trying to adopt something.
 * I can easily reproduce it on 240 cores, but almost never on 16 cores.
 * When it happeneed, we got something like these:

Thread 125 (Thread 0x78d1c37fe700 (LWP 75894)):
#0  0x00000000004e7d80 in foedus::thread::ThreadPimpl::mcs_acquire_lock(foedus::xct::McsLock*) ()
#1  0x00000000004d6392 in foedus::storage::PageVersionLockScope::PageVersionLockScope(foedus::thread::Thread*, foedus::storage::PageVersion*, bool) ()
#2  0x0000000000544b78 in foedus::storage::masstree::MasstreeIntermediatePage::adopt_from_child(foedus::thread::Thread*, unsigned long, unsigned char, unsigned char, foedus::storage::masstree::MasstreePage*) ()
#3  0x00000000004cb381 in foedus::storage::masstree::MasstreeStoragePimpl::reserve_record(foedus::thread::Thread*, void const*, unsigned short, unsigned short, foedus::storage::masstree::MasstreeBorderPage**, unsigned char*, foedus::xct::XctId*) ()
#4  0x00000000004c296c in foedus::storage::masstree::MasstreeStorage::insert_record(foedus::thread::Thread*, void const*, unsigned short, void const*, unsigned short) ()
#5  0x000000000047cbee in foedus::tpcc::TpccLoadTask::load_customers_in_district(unsigned short, unsigned char) ()
#6  0x000000000047d5a0 in foedus::tpcc::TpccLoadTask::load_customers() ()
#7  0x000000000047d9bb in foedus::tpcc::TpccLoadTask::load_tables() ()
#8  0x000000000047e3ad in foedus::tpcc::TpccLoadTask::run(foedus::thread::Thread*) ()
#9  0x000000000047e81f in foedus::tpcc::tpcc_load_task(foedus::proc::ProcArguments const&) ()
#10 0x00000000004e582b in foedus::thread::ThreadPimpl::handle_tasks() ()
#11 0x00007fabfbf01da0 in ?? () from /lib64/libstdc++.so.6
#12 0x00007fabfc7f0df3 in start_thread () from /lib64/libpthread.so.0
#13 0x00007fabfb66a3dd in clone () from /lib64/libc.so.6
x several.

Thread 113 (Thread 0x78d1b27fc700 (LWP 75905)):
#0  0x00000000004e7abf in foedus::thread::Thread::mcs_release_lock(foedus::xct::McsLock*, unsigned int) ()
#1  0x00000000004d63d7 in foedus::storage::PageVersionLockScope::release() ()
#2  0x0000000000544be0 in foedus::storage::masstree::MasstreeIntermediatePage::adopt_from_child(foedus::thread::Thread*, unsigned long, unsigned char, unsigned char, foedus::storage::masstree::MasstreePage*) ()
#3  0x00000000004cb381 in foedus::storage::masstree::MasstreeStoragePimpl::reserve_record(foedus::thread::Thread*, void const*, unsigned short, unsigned short, foedus::storage::masstree::MasstreeBorderPage**, unsigned char*, foedus::xct::XctId*) ()
#4  0x00000000004c296c in foedus::storage::masstree::MasstreeStorage::insert_record(foedus::thread::Thread*, void const*, unsigned short, void const*, unsigned short) ()
#5  0x000000000047cbee in foedus::tpcc::TpccLoadTask::load_customers_in_district(unsigned short, unsigned char) ()
#6  0x000000000047d5a0 in foedus::tpcc::TpccLoadTask::load_customers() ()
#7  0x000000000047d9bb in foedus::tpcc::TpccLoadTask::load_tables() ()
#8  0x000000000047e3ad in foedus::tpcc::TpccLoadTask::run(foedus::thread::Thread*) ()
#9  0x000000000047e81f in foedus::tpcc::tpcc_load_task(foedus::proc::ProcArguments const&) ()
#10 0x00000000004e582b in foedus::thread::ThreadPimpl::handle_tasks() ()
#11 0x00007fabfbf01da0 in ?? () from /lib64/libstdc++.so.6
#12 0x00007fabfc7f0df3 in start_thread () from /lib64/libpthread.so.0
#13 0x00007fabfb66a3dd in clone () from /lib64/libc.so.6
just one

 * Because we need a tricky setting to reliably reproduce this with a limited number of cores,
 * We separated out this test from test_masstree_tpcc.cpp (it has the same test, but it can't
 * reproduce the bug).
 */
namespace foedus {
namespace storage {
namespace masstree {
DEFINE_TEST_CASE_PACKAGE(MasstreeGrowRaceTest, foedus.storage.masstree);

struct Key {
  uint64_t first_slice_;
  uint32_t second_slice_;
  uint32_t uniquefier_;

  void to_be(char* be) const {
    assorted::write_bigendian<uint64_t>(first_slice_, be);
    assorted::write_bigendian<uint32_t>(second_slice_, be + 8U);
    assorted::write_bigendian<uint32_t>(uniquefier_, be + 12U);
  }
  void from_be(const char* be) {
    first_slice_ = assorted::read_bigendian<uint64_t>(be);
    second_slice_ = assorted::read_bigendian<uint32_t>(be + 8U);
    uniquefier_ = assorted::read_bigendian<uint32_t>(be + 12U);
  }
};

/** Distinct values for first_slice_ */
const uint32_t kFirstDisinct = 10U;

/** Distinct values for second_slice_ */
const uint32_t kSecondDisinct = 50U;

/** BIG to make page splits often. */
const uint32_t kPayload = 500U;

/** insertions per thread */
#ifndef NDEBUG
const uint32_t kRecords = 1U << 8;
#else  // NDEBUG
const uint32_t kRecords = 1U << 14;
// const uint32_t kRecords = 1U << 18;  // to really reproduce it.
#endif  // NDEBUG

/**
 * to make the execution time reasonable, we run physical core count times this number of threads.
 * Also, we do not run this test on valgrind.
 */
#ifndef NDEBUG
const uint32_t kThreadsPerCore = 1U;
#else  // NDEBUG
const uint32_t kThreadsPerCore = 2U;
#endif  // NDEBUG

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

  /** Loads the Customer Table */
  ErrorStack load_customers();
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
  CHECK_ERROR(load_customers());
  watch.stop();
  LOG(INFO) << "Load-Thread-" << thread_ordinal_ << " done in " << watch.elapsed_sec() << "sec";
  return kRetOk;
}

ErrorStack TpccLoadTask::load_customers() {
  Epoch ep;
  MasstreeStorage storage(engine_, kName);
  const uint32_t kCommitBatch = 100;
  char key_be[sizeof(Key)];
  char payload[4096U];
  std::memset(payload, 0, sizeof(payload));
  Key key_ho;
  for (uint32_t i = 0; i < kRecords;) {
    uint32_t cur_batch_size = std::min<uint32_t>(kCommitBatch, kRecords - i);
    WRAP_ERROR_CODE(xct_manager_->begin_xct(context_, xct::kSerializable));
    for (uint32_t j = 0; j < cur_batch_size; ++j) {
      key_ho.first_slice_ = rnd_.next_uint32() % kFirstDisinct;
      key_ho.second_slice_ = rnd_.next_uint32() % kSecondDisinct;
      key_ho.uniquefier_ = i + j + kRecords * thread_ordinal_;
      key_ho.to_be(key_be);
      WRAP_ERROR_CODE(
        storage.insert_record(
          context_,
          key_be,
          sizeof(key_be),
          payload,
          kPayload));
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
  return kRetOk;
}

void run_test() {
  EngineOptions options = get_tiny_options();
  EngineOptions native_options;
  std::cout << "kRecords=" << kRecords << std::endl;
  std::cout << "Physical core count = "
    << native_options.thread_.get_total_thread_count() << std::endl;
  uint32_t test_core_count = native_options.thread_.get_total_thread_count() * 2U;
  if (RUNNING_ON_VALGRIND) {
    std::cout << "This testcase takes too long on Valgrind. exit" << std::endl;
    return;
  }
  if (test_core_count > 100U) {
    std::cout << "This testcase throttles thread count to 100." << std::endl;
    test_core_count = 100U;
  }

  options.thread_.group_count_ = 1;
  options.thread_.thread_count_per_group_ = test_core_count;
  uint32_t vol_mb = 2U * test_core_count;
  uint32_t records_per_border = (1U << 11) / kPayload;
  uint32_t total_borders = (test_core_count * kRecords) / records_per_border;
  uint32_t border_kb = total_borders * 4U * 3ULL;  // *3 as a margin. splits require tentative pages
  vol_mb += border_kb / 1024ULL;
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
      bool ret = engine.get_thread_pool()->impersonate_on_numa_core(
        i,
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

TEST(MasstreeGrowRaceTest, Contended) {
  for (uint32_t i = 0; i < kReps; ++i) {
    run_test();
  }
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreeGrowRaceTest, foedus.storage.masstree);
