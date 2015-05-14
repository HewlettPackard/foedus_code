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
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

/**
 * @file test_snapshot_sequential.cpp
 * Snapshot for sequential storage.
 */
namespace foedus {
namespace snapshot {
DEFINE_TEST_CASE_PACKAGE(SnapshotSequentialTest, foedus.snapshot);

const uint32_t kRecords = 1024;
const uint32_t kPayload = sizeof(uint64_t);
const uint32_t kThreads = 2;
const storage::StorageName kName("test");

ErrorStack appends_task(const proc::ProcArguments& args) {
  EXPECT_EQ(sizeof(uint32_t), args.input_len_);
  uint32_t id = *reinterpret_cast<const uint32_t*>(args.input_buffer_);
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::sequential::SequentialStorage sequential(args.engine_, kName);
  ASSERT_ND(sequential.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < kRecords / 2U; ++i) {
    uint64_t rec = id * kRecords / 2U + i;
    WRAP_ERROR_CODE(sequential.append_record(context, &rec, sizeof(rec)));
  }

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

/* TODO(Hideaki) We should have a verify. as soon as we add scan API.
ErrorStack verify_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  storage::sequential::SequentialStorage sequential(args.engine_, kName);
  ASSERT_ND(sequential.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < kRecords; ++i) {
    storage::sequential::SequentialOffset rec = i;
    storage::sequential::SequentialOffset data = 0;
    WRAP_ERROR_CODE(sequential.get_record(context, rec, &data));
    EXPECT_EQ(rec, data) << i;
  }

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  return kRetOk;
}
*/

void test_appends(bool multiple_loggers, bool multiple_partitions) {
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

  {
    Engine engine(options);
    const proc::ProcName kAppends("appends_task");
    engine.get_proc_manager()->pre_register(kAppends, appends_task);
    // engine.get_proc_manager()->pre_register("verify_task", verify_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      storage::sequential::SequentialStorage out;
      Epoch commit_epoch;
      storage::sequential::SequentialMetadata meta(kName);
      COERCE_ERROR(engine.get_storage_manager()->create_sequential(&meta, &out, &commit_epoch));
      EXPECT_TRUE(out.exists());
      EXPECT_TRUE(commit_epoch.is_valid());

      thread::ThreadPool* pool = engine.get_thread_pool();
      for (uint32_t i = 0; i < kThreads; ++i) {
        if (multiple_partitions) {
          COERCE_ERROR(pool->impersonate_on_numa_node_synchronous(i, kAppends, &i, sizeof(i)));
        } else {
          COERCE_ERROR(pool->impersonate_on_numa_core_synchronous(i, kAppends, &i, sizeof(i)));
        }
      }

      // COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify_task"));
      engine.get_snapshot_manager()->trigger_snapshot_immediate(true);

      COERCE_ERROR(engine.uninitialize());
    }
  }
  {
    Engine engine(options);
    // engine.get_proc_manager()->pre_register("verify_task", verify_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      // COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify_task"));
      COERCE_ERROR(engine.uninitialize());
    }
  }
  cleanup_test(options);
}

TEST(SnapshotSequentialTest, AppendsOneLogger) { test_appends(false, false); }
TEST(SnapshotSequentialTest, AppendsTwoLoggers) { test_appends(true, false); }
TEST(SnapshotSequentialTest, AppendsTwoPartitions) { test_appends(true, true); }

}  // namespace snapshot
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SnapshotSequentialTest, foedus.snapshot);
