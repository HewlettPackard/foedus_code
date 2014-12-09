/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/log_options.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

/**
 * @file test_log_marker_race.cpp
 * This one tests a bug that once existed.
 * It was because of epoch marker being inserted before the logs in thread-private buffers
 * in the epoch were picked by the logger. We reproduce the bug by racing between a few threads.
 *
 * The issue happened when the logger is well catching up each transaction.
 * So, we put waits here and there.
 */
namespace foedus {
namespace log {
DEFINE_TEST_CASE_PACKAGE(LogMarkerRaceTest, foedus.log);

// Even this number doesn't reliably reproduce the bug, but at least once in a few times.
// To avoid too long testcase, increase it only when you want to specifically test this.
// const uint32_t kTestMarkerRacesLastEpoch = 100000U;  // to reliably repro. don't push this!
// const uint32_t kTestMarkerRacesLastEpoch = 1000U;  // should be usually this.
const uint32_t kTestMarkerRacesLastEpoch = 100U;  // fast, but only occasionally repro.
const uint16_t kThreads = 4U;
const uint16_t kRecordsPerThread = 4U;

ErrorStack test_marker_races(const proc::ProcArguments& args) {
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  storage::array::ArrayStorage array(args.engine_, "test");
  ASSERT_ND(array.exists());
  thread::Thread* context = args.context_;


  uint16_t ordinal = context->get_thread_global_ordinal();
  uint32_t transactions = 0;
  while (true) {
    if (xct_manager->get_current_global_epoch() >= Epoch(kTestMarkerRacesLastEpoch)) {
      break;
    }
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kDirtyReadPreferVolatile));

    for (uint16_t i = 0; i < kRecordsPerThread; ++i) {
      storage::array::ArrayOffset offset = ordinal * kRecordsPerThread + i;
      WRAP_ERROR_CODE(array.overwrite_record_primitive<uint64_t>(context, offset, offset + 42, 0));
    }

    Epoch commit_epoch;
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    ++transactions;
    if (transactions % 10ULL == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (transactions % 1000ULL == 0) {
      std::cout << "Thread-" << ordinal << " flush. now " << transactions << " xcts" << std::endl;
      WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
    }
  }

  std::cout << "Thread-" << ordinal << ", processed " << transactions << " xcts" << std::endl;
  return kRetOk;
}

void test_main(bool take_savepoint) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;

  // make it extremely small so that we can also test wrap around
  options.log_.log_buffer_kb_ = 16;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("test_marker_races", test_marker_races);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);

    storage::array::ArrayMetadata meta("test", sizeof(uint64_t), 128);
    Epoch create_epoch;
    storage::array::ArrayStorage array;
    COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &array, &create_epoch));

    thread::ImpersonateSession sessions[kThreads];
    for (uint16_t i = 0; i < kThreads; ++i) {
      thread::ThreadPool* pool = engine.get_thread_pool();
      EXPECT_TRUE(pool->impersonate("test_marker_races", nullptr, 0, sessions + i));
    }

    engine.get_log_manager()->wakeup_loggers();

    // frequently advance to test the issue.
    xct::XctManager* xct_manager = engine.get_xct_manager();
    for (uint32_t i = 0; i < kTestMarkerRacesLastEpoch; ++i) {
      xct_manager->advance_current_global_epoch();
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      if (take_savepoint && i % 10 == 0) {
        Epoch savepoint_epoch = engine.get_log_manager()->get_durable_global_epoch();
        COERCE_ERROR(engine.get_savepoint_manager()->take_savepoint(savepoint_epoch));
      }
    }
    EXPECT_GT(xct_manager->get_current_global_epoch(), Epoch(kTestMarkerRacesLastEpoch));

    for (uint16_t i = 0; i < kThreads; ++i) {
      COERCE_ERROR(sessions[i].get_result());
      sessions[i].release();
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(LogMarkerRaceTest, NoSavePoint) { test_main(false); }
TEST(LogMarkerRaceTest, SavePoint) { test_main(true); }

}  // namespace log
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(LogMarkerRaceTest, foedus.log);
