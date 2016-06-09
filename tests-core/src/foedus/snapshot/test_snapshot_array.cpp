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

const uint32_t k1LvRecords = 16;  // so that everything fits in one page; a special case
const uint32_t kMoreRecords = 1024;  // For 2 and 3 levels
// If a page contains just 2 records, 1024 rec = 512 leaf pages = at least 2 intermediate pages.
const uint32_t kTwoLevelPayload = sizeof(storage::array::ArrayOffset);  // For 1 and 2 levels
const uint32_t kThreeLevelPayload = 1500;
const uint32_t kThreads = 2;
const storage::StorageName kName("test");
const storage::StorageName kNameAnother("test2");

/** The input used by all tasks in this file */
struct TaskInput {
  uint32_t id;        // Logical ID of the thread (determines task assignment)
  uint32_t records;   // either k1LvRecords or kMoreRecords
};
const uint32_t kInput = sizeof(TaskInput);

ErrorStack overwrites_task(const proc::ProcArguments& args) {
  EXPECT_EQ(kInput, args.input_len_);
  const TaskInput* input = reinterpret_cast<const TaskInput*>(args.input_buffer_);
  const uint32_t records = input->records;
  uint32_t id = input->id;
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < records / 2U; ++i) {
    storage::array::ArrayOffset rec = id * records / 2U + i;
    WRAP_ERROR_CODE(array.overwrite_record(context, rec, &rec, 0, sizeof(rec)));
  }

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

ErrorStack increments_task(const proc::ProcArguments& args) {
  EXPECT_EQ(kInput, args.input_len_);
  const TaskInput* input = reinterpret_cast<const TaskInput*>(args.input_buffer_);
  const uint32_t records = input->records;
  uint32_t id = input->id;
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < records / 2U; ++i) {
    storage::array::ArrayOffset rec = id * records / 2U + i;
    WRAP_ERROR_CODE(array.increment_record_oneshot<uint64_t>(context, rec, rec, 0));
  }
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

// This one also tests the log compression feature, combining two increments to one.
ErrorStack increments_twice_task(const proc::ProcArguments& args) {
  EXPECT_EQ(kInput, args.input_len_);
  const TaskInput* input = reinterpret_cast<const TaskInput*>(args.input_buffer_);
  const uint32_t records = input->records;
  uint32_t id = input->id;
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < records / 2U; ++i) {
    storage::array::ArrayOffset rec = id * records / 2U + i;
    WRAP_ERROR_CODE(array.increment_record_oneshot<uint64_t>(context, rec, rec / 2ULL, 0));
  }
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  for (uint32_t i = 0; i < records / 2U; ++i) {
    storage::array::ArrayOffset rec = id * records / 2U + i;
    WRAP_ERROR_CODE(array.increment_record_oneshot<uint64_t>(context, rec, rec - (rec / 2ULL), 0));
  }

  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

ErrorStack two_arrays_task(const proc::ProcArguments& args) {
  EXPECT_EQ(kInput, args.input_len_);
  const TaskInput* input = reinterpret_cast<const TaskInput*>(args.input_buffer_);
  const uint32_t records = input->records;
  uint32_t id = input->id;
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  storage::array::ArrayStorage another(args.engine_, kNameAnother);
  ASSERT_ND(another.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < records / 2U; ++i) {
    storage::array::ArrayOffset rec = id * records / 2U + i;
    WRAP_ERROR_CODE(array.overwrite_record(context, rec, &rec, 0, sizeof(rec)));
    WRAP_ERROR_CODE(another.overwrite_record(context, records - rec - 1U, &rec, 0, sizeof(rec)));
  }

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

ErrorStack verify_task(const proc::ProcArguments& args) {
  EXPECT_EQ(kInput, args.input_len_);
  const TaskInput* input = reinterpret_cast<const TaskInput*>(args.input_buffer_);
  const uint32_t records = input->records;

  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  storage::array::ArrayStorage another(args.engine_, kNameAnother);
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < records; ++i) {
    storage::array::ArrayOffset rec = i;
    storage::array::ArrayOffset data = 0;
    WRAP_ERROR_CODE(array.get_record(context, rec, &data, 0, sizeof(data)));
    EXPECT_EQ(rec, data) << i;
    if (another.exists()) {
      WRAP_ERROR_CODE(another.get_record(context, records - rec - 1U, &data, 0, sizeof(data)));
      EXPECT_EQ(rec, data) << i;
    }
  }

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  return kRetOk;
}

ErrorStack overwrites_holes_task(const proc::ProcArguments& args) {
  EXPECT_EQ(kInput, args.input_len_);
  const TaskInput* input = reinterpret_cast<const TaskInput*>(args.input_buffer_);
  const uint32_t records = input->records;
  uint32_t id = input->id;
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  // holes at the beginning and end. (set only the middle half)
  // we might want to test a hole in the middle..
  uint32_t from = (id == 0) ? records / 4U : 0;
  for (uint32_t i = from; i < from + (records / 4U); ++i) {
    storage::array::ArrayOffset rec = id * records / 2U + i;
    WRAP_ERROR_CODE(array.overwrite_record(context, rec, &rec, 0, sizeof(rec)));
  }

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

ErrorStack verify_holes_task(const proc::ProcArguments& args) {
  EXPECT_EQ(kInput, args.input_len_);
  const TaskInput* input = reinterpret_cast<const TaskInput*>(args.input_buffer_);
  const uint32_t records = input->records;

  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  for (uint32_t i = 0; i < records; ++i) {
    storage::array::ArrayOffset rec = i;
    storage::array::ArrayOffset data = 0;
    WRAP_ERROR_CODE(array.get_record(context, rec, &data, 0, sizeof(data)));
    if (i < records / 4U || i >= (records / 2U) + (records / 4U)) {
      EXPECT_EQ(0, data) << i;
    } else {
      EXPECT_EQ(rec, data) << i;
    }
  }

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  return kRetOk;
}

const proc::ProcName kOv("overwrites_task");
const proc::ProcName kInc("increments_task");
const proc::ProcName kInc2("increments_twice_task");
const proc::ProcName kTwo("two_arrays_task");
const proc::ProcName kHoles("overwrites_holes_task");

void test_run(
  const proc::ProcName& proc_name,
  bool multiple_loggers,
  bool multiple_partitions,
  int levels) {
  ASSERT_ND(levels >= 1 && levels <= 3);
  const bool three_levels = levels == 3;
  uint16_t payload = three_levels ? kThreeLevelPayload : kTwoLevelPayload;
  EngineOptions options = get_tiny_options();
  if (multiple_partitions) {
    options.thread_.thread_count_per_group_ = 1;
    options.thread_.group_count_ = 2;
    options.log_.loggers_per_node_ = 1;
  } else {
    options.thread_.thread_count_per_group_ = kThreads;
    options.thread_.group_count_ = 1;
    options.log_.loggers_per_node_ = multiple_loggers ? kThreads : 1;
  }
  if (three_levels) {
    options.memory_.page_pool_size_mb_per_node_ *= 50;
    options.cache_.snapshot_cache_size_mb_per_node_ *= 50;
  }

  const uint32_t records = (levels == 1 ? k1LvRecords : kMoreRecords);
  TaskInput input = {0, records};
  proc::Proc verify_proc = proc_name == kHoles ? verify_holes_task : verify_task;
  {
    Engine engine(options);
    engine.get_proc_manager()->pre_register("overwrites_task", overwrites_task);
    engine.get_proc_manager()->pre_register("increments_task", increments_task);
    engine.get_proc_manager()->pre_register("increments_twice_task", increments_twice_task);
    engine.get_proc_manager()->pre_register("two_arrays_task", two_arrays_task);
    engine.get_proc_manager()->pre_register("overwrites_holes_task", overwrites_holes_task);
    engine.get_proc_manager()->pre_register("verify", verify_proc);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      storage::array::ArrayStorage out;
      Epoch commit_epoch;
      storage::array::ArrayMetadata meta(kName, payload, records);
      COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &out, &commit_epoch));
      EXPECT_TRUE(out.exists());
      EXPECT_TRUE(commit_epoch.is_valid());

      if (proc_name == kTwo) {
        storage::array::ArrayStorage another;
        storage::array::ArrayMetadata another_meta(kNameAnother, payload, records);
        COERCE_ERROR(engine.get_storage_manager()->create_array(
          &another_meta,
          &another,
          &commit_epoch));
        EXPECT_TRUE(another.exists());
      }

      thread::ThreadPool* pool = engine.get_thread_pool();
      for (uint32_t i = 0; i < kThreads; ++i) {
        input.id = i;
        if (multiple_partitions) {
          COERCE_ERROR(pool->impersonate_on_numa_node_synchronous(i, proc_name, &input, kInput));
        } else {
          COERCE_ERROR(pool->impersonate_on_numa_core_synchronous(i, proc_name, &input, kInput));
        }
      }

      EXPECT_TRUE(out.exists());
      COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify", &input, kInput));
      EXPECT_TRUE(out.exists());
      engine.get_snapshot_manager()->trigger_snapshot_immediate(true);
      EXPECT_TRUE(out.exists());

      COERCE_ERROR(engine.uninitialize());
    }
  }
  {
    Engine engine(options);
    engine.get_proc_manager()->pre_register("verify", verify_proc);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify", &input, kInput));
      COERCE_ERROR(engine.uninitialize());
    }
  }
  cleanup_test(options);
}

// Also test 1-level case. It might have a bug specific to this case because
// single-level array storage is treated differently in the composer. See Issue #127.
TEST(SnapshotArrayTest, OverwritesOneLogger) { test_run(kOv, false, false, 1); }
TEST(SnapshotArrayTest, OverwritesTwoLoggers) { test_run(kOv, true, false, 1); }
TEST(SnapshotArrayTest, OverwritesTwoPartitions) { test_run(kOv, true, true, 1); }
TEST(SnapshotArrayTest, IncrementsOneLogger) { test_run(kInc, false, false, 1); }
TEST(SnapshotArrayTest, IncrementsTwoLoggers) { test_run(kInc, true, false, 1); }
TEST(SnapshotArrayTest, IncrementsTwoPartitions) { test_run(kInc, true, true, 1); }
TEST(SnapshotArrayTest, IncrementsTwiceOneLogger) { test_run(kInc2, false, false, 1); }
TEST(SnapshotArrayTest, IncrementsTwiceTwoLoggers) { test_run(kInc2, true, false, 1); }
TEST(SnapshotArrayTest, IncrementsTwiceTwoPartitions) { test_run(kInc2, true, true, 1); }
TEST(SnapshotArrayTest, TwoArraysOneLogger) { test_run(kTwo, false, false, 1); }
TEST(SnapshotArrayTest, TwoArraysTwoLoggers) { test_run(kTwo, true, false, 1); }
TEST(SnapshotArrayTest, TwoArraysTwoPartitions) { test_run(kTwo, true, true, 1); }
TEST(SnapshotArrayTest, HolesOneLogger) { test_run(kHoles, false, false, 1); }
TEST(SnapshotArrayTest, HolesTwoLoggers) { test_run(kHoles, true, false, 1); }
TEST(SnapshotArrayTest, HolesTwoPartitions) { test_run(kHoles, true, true, 1); }

TEST(SnapshotArrayTest, OverwritesOneLogger2Lv) { test_run(kOv, false, false, 2); }
TEST(SnapshotArrayTest, OverwritesTwoLoggers2Lv) { test_run(kOv, true, false, 2); }
TEST(SnapshotArrayTest, OverwritesTwoPartitions2Lv) { test_run(kOv, true, true, 2); }
TEST(SnapshotArrayTest, IncrementsOneLogger2Lv) { test_run(kInc, false, false, 2); }
TEST(SnapshotArrayTest, IncrementsTwoLoggers2Lv) { test_run(kInc, true, false, 2); }
TEST(SnapshotArrayTest, IncrementsTwoPartitions2Lv) { test_run(kInc, true, true, 2); }
TEST(SnapshotArrayTest, IncrementsTwiceOneLogger2Lv) { test_run(kInc2, false, false, 2); }
TEST(SnapshotArrayTest, IncrementsTwiceTwoLoggers2Lv) { test_run(kInc2, true, false, 2); }
TEST(SnapshotArrayTest, IncrementsTwiceTwoPartitions2Lv) { test_run(kInc2, true, true, 2); }
TEST(SnapshotArrayTest, TwoArraysOneLogger2Lv) { test_run(kTwo, false, false, 2); }
TEST(SnapshotArrayTest, TwoArraysTwoLoggers2Lv) { test_run(kTwo, true, false, 2); }
TEST(SnapshotArrayTest, TwoArraysTwoPartitions2Lv) { test_run(kTwo, true, true, 2); }
TEST(SnapshotArrayTest, HolesOneLogger2Lv) { test_run(kHoles, false, false, 2); }
TEST(SnapshotArrayTest, HolesTwoLoggers2Lv) { test_run(kHoles, true, false, 2); }
TEST(SnapshotArrayTest, HolesTwoPartitions2Lv) { test_run(kHoles, true, true, 2); }

TEST(SnapshotArrayTest, OverwritesOneLogger3Lv) { test_run(kOv, false, false, 3); }
TEST(SnapshotArrayTest, OverwritesTwoLoggers3Lv) { test_run(kOv, true, false, 3); }
TEST(SnapshotArrayTest, OverwritesTwoPartitions3Lv) { test_run(kOv, true, true, 3); }
TEST(SnapshotArrayTest, IncrementsOneLogger3Lv) { test_run(kInc, false, false, 3); }
TEST(SnapshotArrayTest, IncrementsTwoLoggers3Lv) { test_run(kInc, true, false, 3); }
TEST(SnapshotArrayTest, IncrementsTwoPartitions3Lv) { test_run(kInc, true, true, 3); }
TEST(SnapshotArrayTest, IncrementsTwiceOneLogger3Lv) { test_run(kInc2, false, false, 3); }
TEST(SnapshotArrayTest, IncrementsTwiceTwoLoggers3Lv) { test_run(kInc2, true, false, 3); }
TEST(SnapshotArrayTest, IncrementsTwiceTwoPartitions3Lv) { test_run(kInc2, true, true, 3); }
TEST(SnapshotArrayTest, TwoArraysOneLogger3Lv) { test_run(kTwo, false, false, 3); }
TEST(SnapshotArrayTest, TwoArraysTwoLoggers3Lv) { test_run(kTwo, true, false, 3); }
TEST(SnapshotArrayTest, TwoArraysTwoPartitions3Lv) { test_run(kTwo, true, true, 3); }
TEST(SnapshotArrayTest, HolesOneLogger3Lv) { test_run(kHoles, false, false, 3); }
TEST(SnapshotArrayTest, HolesTwoLoggers3Lv) { test_run(kHoles, true, false, 3); }
TEST(SnapshotArrayTest, HolesTwoPartitions3Lv) { test_run(kHoles, true, true, 3); }

}  // namespace snapshot
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SnapshotArrayTest, foedus.snapshot);
