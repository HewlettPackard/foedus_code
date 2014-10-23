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
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

/**
 * @file test_snapshot_masstree.cpp
 * Snapshot for masstree storage.
 */
namespace foedus {
namespace snapshot {
DEFINE_TEST_CASE_PACKAGE(SnapshotMasstreeTest, foedus.snapshot);

const uint32_t kRecords = 1024;
const uint32_t kThreads = 2;
const storage::StorageName kName("test");

ErrorStack inserts_normalized_task(const proc::ProcArguments& args) {
  EXPECT_EQ(sizeof(uint32_t), args.input_len_);
  uint32_t id = *reinterpret_cast<const uint32_t*>(args.input_buffer_);
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::masstree::MasstreeStorage masstree(args.engine_, kName);
  ASSERT_ND(masstree.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < kRecords / 2U; ++i) {
    uint64_t rec = id * kRecords / 2U + i;
    storage::masstree::KeySlice slice = storage::masstree::normalize_primitive<uint64_t>(rec);
    WRAP_ERROR_CODE(masstree.insert_record_normalized(context, slice, &rec, sizeof(rec)));
  }

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

ErrorStack verify_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  storage::masstree::MasstreeStorage masstree(args.engine_, kName);
  ASSERT_ND(masstree.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < kRecords; ++i) {
    uint64_t rec = i;
    storage::masstree::KeySlice slice = storage::masstree::normalize_primitive<uint64_t>(rec);
    uint64_t data;
    uint16_t capacity = sizeof(data);
    ErrorCode ret = masstree.get_record_normalized(context, slice, &data, &capacity);
    EXPECT_EQ(kErrorCodeOk, ret) << i;
    EXPECT_EQ(rec, data) << i;
    EXPECT_EQ(sizeof(data), capacity) << i;
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
    engine.get_proc_manager()->pre_register("inserts_normalized_task", inserts_normalized_task);
    engine.get_proc_manager()->pre_register("verify_task", verify_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      storage::masstree::MasstreeStorage out;
      Epoch commit_epoch;
      storage::masstree::MasstreeMetadata meta(kName);
      COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &out, &commit_epoch));
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

const proc::ProcName kInsN("inserts_normalized_task");
TEST(SnapshotMasstreeTest, InsertsNormalizedOneLogger) { test_run(kInsN, false, false); }
TEST(SnapshotMasstreeTest, InsertsNormalizedTwoLoggers) { test_run(kInsN, true, false); }
TEST(SnapshotMasstreeTest, InsertsNormalizedTwoPartitions) { test_run(kInsN, true, true); }
}  // namespace snapshot
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SnapshotMasstreeTest, foedus.snapshot);
