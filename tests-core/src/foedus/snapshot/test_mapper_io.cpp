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
 * @file test_mapper_io.cpp
 * Tests the bug in LogMapper's IO.
 * It reproduces the case where one log file contains more than mapper-io buffer.
 * @see https://github.com/hkimura/foedus_code/issues/100
 */
namespace foedus {
namespace snapshot {
DEFINE_TEST_CASE_PACKAGE(MapperIoTest, foedus.snapshot);

// This testcase needs to generate a large log file (2MB at least).
// Let's use a quite fat payload.
// kTwoIterationLogs * kPayload = 3 MB (+some), surely more than 2MB.
// kOneIterationLogs * kPayload = 0.6 MB (+some), surely less than 2MB.
const uint32_t kPayload = 3000;
const uint32_t kTwoIterationsLogs = 1024;
const uint32_t kOneIterationLogs = 200;
const uint32_t kRecords = 16;  // To speed up the test, actually we only use the first record
const storage::StorageName kName("test");

ErrorStack load_impl(const proc::ProcArguments& args, uint32_t log_count) {
  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();

  Epoch commit_epoch;
  char payload[kPayload];
  std::memset(payload, 0, kPayload);
  for (uint32_t i = 0; i < log_count;) {
    const uint32_t kLogsPerBatch = 50;
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    while (true) {
      *reinterpret_cast<uint64_t*>(payload) = i;
      WRAP_ERROR_CODE(array.overwrite_record(context, 0, payload));
      ++i;
      if (i % kLogsPerBatch == 0 || i >= log_count) {
        break;
      }
    }
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    uint64_t value = 0;
    WRAP_ERROR_CODE(array.get_record_primitive<uint64_t>(context, 0, &value, 0));
    ASSERT_ND(i == value + 1U);
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}
ErrorStack load_two_iterations_task(const proc::ProcArguments& args) {
  CHECK_ERROR(load_impl(args, kTwoIterationsLogs));
  return kRetOk;
}
ErrorStack load_one_iteration_task(const proc::ProcArguments& args) {
  CHECK_ERROR(load_impl(args, kOneIterationLogs));
  return kRetOk;
}

ErrorStack verify_impl(const proc::ProcArguments& args, uint32_t log_count) {
  thread::Thread* context = args.context_;
  storage::array::ArrayStorage array(args.engine_, kName);
  ASSERT_ND(array.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();

  char payload_correct[kPayload];
  std::memset(payload_correct, 0, kPayload);
  *reinterpret_cast<uint64_t*>(payload_correct) = static_cast<uint64_t>(log_count - 1);

  char payload[kPayload];
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  WRAP_ERROR_CODE(array.get_record(context, 0, payload));
  uint64_t payload_head = *reinterpret_cast<const uint64_t*>(payload);
  EXPECT_EQ(log_count, payload_head + 1U);
  EXPECT_EQ(std::string(payload_correct, kPayload), std::string(payload, kPayload));

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  return kRetOk;
}
ErrorStack verify_two_iterations_task(const proc::ProcArguments& args) {
  CHECK_ERROR(verify_impl(args, kTwoIterationsLogs));
  return kRetOk;
}
ErrorStack verify_one_iteration_task(const proc::ProcArguments& args) {
  CHECK_ERROR(verify_impl(args, kOneIterationLogs));
  return kRetOk;
}

void test_run(bool two_iterations) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = 1;
  options.log_.loggers_per_node_ = 1;
  options.snapshot_.log_mapper_io_buffer_mb_ = 2;

  {
    Engine engine(options);
    if (two_iterations) {
      engine.get_proc_manager()->pre_register("load_task", load_two_iterations_task);
      engine.get_proc_manager()->pre_register("verify_task", verify_two_iterations_task);
    } else {
      engine.get_proc_manager()->pre_register("load_task", load_one_iteration_task);
      engine.get_proc_manager()->pre_register("verify_task", verify_one_iteration_task);
    }
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      storage::array::ArrayStorage out;
      Epoch commit_epoch;
      storage::array::ArrayMetadata meta(kName, kPayload, kRecords);

      // allow dropping all volatile pages to test purely snapshot-page only cases.
      meta.snapshot_drop_volatile_pages_threshold_ = 0;
      COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &out, &commit_epoch));
      EXPECT_TRUE(out.exists());
      EXPECT_TRUE(commit_epoch.is_valid());

      thread::ThreadPool* pool = engine.get_thread_pool();
      COERCE_ERROR(pool->impersonate_synchronous("load_task"));
      COERCE_ERROR(pool->impersonate_synchronous("verify_task"));
      engine.get_snapshot_manager()->trigger_snapshot_immediate(true);
      COERCE_ERROR(pool->impersonate_synchronous("verify_task"));
      COERCE_ERROR(engine.uninitialize());
    }
  }
  {
    Engine engine(options);
    if (two_iterations) {
      engine.get_proc_manager()->pre_register("verify_task", verify_two_iterations_task);
    } else {
      engine.get_proc_manager()->pre_register("verify_task", verify_one_iteration_task);
    }
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify_task"));
      COERCE_ERROR(engine.uninitialize());
    }
  }
  cleanup_test(options);
}

TEST(MapperIoTest, OneIteration) { test_run(false); }
TEST(MapperIoTest, TwoIterations) { test_run(true); }

}  // namespace snapshot
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MapperIoTest, foedus.snapshot);
