/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/snapshot/snapshot_id.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/snapshot/snapshot_manager_pimpl.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

/**
 * @file test_snapshot_array.cpp
 * Snapshot for array storage.
 */
namespace foedus {
namespace snapshot {
DEFINE_TEST_CASE_PACKAGE(SnapshotArrayTest, foedus.snapshot);

const uint32_t kRecords = 1024;
const uint32_t kPayload = sizeof(storage::array::ArrayOffset);
const uint32_t kThreads = 2;
const storage::StorageName kName("test");

ErrorStack overwrites_task(const proc::ProcArguments& args) {
  EXPECT_EQ(sizeof(uint32_t), args.input_len_);
  uint32_t id = *reinterpret_cast<const uint32_t*>(args.input_buffer_);
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < kRecords / 2U; ++i) {
    storage::array::ArrayOffset rec = id * kRecords / 2U + i;
    WRAP_ERROR_CODE(array.overwrite_record(context, rec, &rec));
  }

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

ErrorStack increments_task(const proc::ProcArguments& args) {
  EXPECT_EQ(sizeof(uint32_t), args.input_len_);
  uint32_t id = *reinterpret_cast<const uint32_t*>(args.input_buffer_);
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < kRecords / 2U; ++i) {
    storage::array::ArrayOffset rec = id * kRecords / 2U + i;
    WRAP_ERROR_CODE(array.increment_record_oneshot<uint64_t>(context, rec, rec, 0));
  }
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

// This one also tests the log compression feature, combining two increments to one.
ErrorStack increments_twice_task(const proc::ProcArguments& args) {
  EXPECT_EQ(sizeof(uint32_t), args.input_len_);
  uint32_t id = *reinterpret_cast<const uint32_t*>(args.input_buffer_);
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < kRecords / 2U; ++i) {
    storage::array::ArrayOffset rec = id * kRecords / 2U + i;
    WRAP_ERROR_CODE(array.increment_record_oneshot<uint64_t>(context, rec, rec / 2ULL, 0));
  }
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  for (uint32_t i = 0; i < kRecords / 2U; ++i) {
    storage::array::ArrayOffset rec = id * kRecords / 2U + i;
    WRAP_ERROR_CODE(array.increment_record_oneshot<uint64_t>(context, rec, rec - (rec / 2ULL), 0));
  }

  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

ErrorStack verify_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < kRecords; ++i) {
    storage::array::ArrayOffset rec = i;
    storage::array::ArrayOffset data = 0;
    WRAP_ERROR_CODE(array.get_record(context, rec, &data));
    EXPECT_EQ(rec, data) << i;
  }

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  return kRetOk;
}

void test_run(const proc::ProcName& proc_name, bool multiple_loggers, bool multiple_partitions) {
  EngineOptions options = get_tiny_options();
  if (multiple_partitions) {
    options.thread_.thread_count_per_group_ = 1;
    options.thread_.group_count_ = 2;
    options.log_.loggers_per_node_ = 1;
    if (!is_multi_nodes()) {
      return;
    }
  } else {
    options.thread_.thread_count_per_group_ = kThreads;
    options.thread_.group_count_ = 1;
    options.log_.loggers_per_node_ = multiple_loggers ? kThreads : 1;
  }

  {
    Engine engine(options);
    engine.get_proc_manager()->pre_register("overwrites_task", overwrites_task);
    engine.get_proc_manager()->pre_register("increments_task", increments_task);
    engine.get_proc_manager()->pre_register("increments_twice_task", increments_twice_task);
    engine.get_proc_manager()->pre_register("verify_task", verify_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      storage::array::ArrayStorage out;
      Epoch commit_epoch;
      storage::array::ArrayMetadata meta(kName, kPayload, kRecords);
      COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &out, &commit_epoch));
      EXPECT_TRUE(out.exists());
      EXPECT_TRUE(commit_epoch.is_valid());

      thread::ThreadPool* pool = engine.get_thread_pool();
      for (uint32_t i = 0; i < kThreads; ++i) {
        if (multiple_partitions) {
          COERCE_ERROR(pool->impersonate_on_numa_node_synchronous(i, proc_name, &i, sizeof(i)));
        } else {
          COERCE_ERROR(pool->impersonate_on_numa_core_synchronous(i, proc_name, &i, sizeof(i)));
        }
      }

      EXPECT_TRUE(out.exists());
      COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify_task"));
      EXPECT_TRUE(out.exists());
      engine.get_snapshot_manager()->trigger_snapshot_immediate(true);
      EXPECT_TRUE(out.exists());

      COERCE_ERROR(engine.uninitialize());
    }
  }
  {
    Engine engine(options);
    engine.get_proc_manager()->pre_register("verify_task", verify_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify_task"));
      COERCE_ERROR(engine.uninitialize());
    }
  }
  cleanup_test(options);
}

const proc::ProcName kOv("overwrites_task");
const proc::ProcName kInc("increments_task");
const proc::ProcName kInc2("increments_twice_task");
TEST(SnapshotArrayTest, OverwritesOneLogger) { test_run(kOv, false, false); }
TEST(SnapshotArrayTest, OverwritesTwoLoggers) { test_run(kOv, true, false); }
TEST(SnapshotArrayTest, OverwritesTwoPartitions) { test_run(kOv, true, true); }
TEST(SnapshotArrayTest, IncrementsOneLogger) { test_run(kInc, false, false); }
TEST(SnapshotArrayTest, IncrementsTwoLoggers) { test_run(kInc, true, false); }
TEST(SnapshotArrayTest, IncrementsTwoPartitions) { test_run(kInc, true, true); }
TEST(SnapshotArrayTest, IncrementsTwiceOneLogger) { test_run(kInc2, false, false); }
TEST(SnapshotArrayTest, IncrementsTwiceTwoLoggers) { test_run(kInc2, true, false); }
TEST(SnapshotArrayTest, IncrementsTwiceTwoPartitions) { test_run(kInc2, true, true); }
}  // namespace snapshot
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SnapshotArrayTest, foedus.snapshot);
