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
 * @file test_snapshot_array_issue_127.cpp
 * Specifically reproduce Issue #127, using the exact same setting as nvdimm_test.cpp.
 */
namespace foedus {
namespace snapshot {
DEFINE_TEST_CASE_PACKAGE(SnapshotArrayIssue127Test, foedus.snapshot);

const storage::StorageName kStorageName("aaa");

ErrorStack the_task(const proc::ProcArguments& args) {
  std::cout << "==================================================" << std::endl;
  std::cout << "=====   NOW the task starts!" << std::endl;
  std::cout << "==================================================" << std::endl << std::flush;
  auto* engine = args.engine_;
  auto* str_manager = engine->get_storage_manager();
  auto* xct_manager = engine->get_xct_manager();
  constexpr uint16_t kRecords = 16;
  storage::array::ArrayStorage the_storage(engine, kStorageName);
  Epoch commit_epoch;
  if (the_storage.exists()) {
    std::cout << "Ok, the storage exists. This is after recovery." << std::endl;

    WRAP_ERROR_CODE(xct_manager->begin_xct(args.context_, xct::kSerializable));
    for (uint16_t i = 0; i < kRecords; ++i) {
      uint64_t data = 0;
      WRAP_ERROR_CODE(the_storage.get_record_primitive<uint64_t>(args.context_, i, &data, 0));
      std::cout << "Record-" << i << "=" << data << std::endl;
      EXPECT_EQ(i + 42U, data);
    }
    WRAP_ERROR_CODE(xct_manager->precommit_xct(args.context_, &commit_epoch));
    std::cout << "Read the record. done!" << std::endl;
  } else {
    std::cout << "the storage doesn't exist. This must be the initial run." << std::endl;
    storage::array::ArrayMetadata meta(kStorageName, sizeof(uint64_t), kRecords);
    CHECK_ERROR(str_manager->create_array(&meta, &the_storage, &commit_epoch));
    ASSERT_ND(the_storage.exists());
    std::cout << "Created the storage. making it durable..." << std::endl;

    std::cout << "Made the storage durable. Populating it..." << std::endl;
    WRAP_ERROR_CODE(xct_manager->begin_xct(args.context_, xct::kSerializable));
    for (uint16_t i = 0; i < kRecords; ++i) {
      WRAP_ERROR_CODE(the_storage.overwrite_record_primitive<uint64_t>(
        args.context_,
        i,
        i + 42U,
        0));
    }
    WRAP_ERROR_CODE(xct_manager->precommit_xct(args.context_, &commit_epoch));

    std::cout << "Populated the storage. making it durable..." << std::endl;
    WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  }

  return kRetOk;
}

TEST(SnapshotArrayIssue127Test, Reproduce) {
  EngineOptions options = get_tiny_options();
  options.thread_.group_count_ = 1;
  options.thread_.thread_count_per_group_ = 1;
  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogWarning;

  for (uint16_t i = 0; i < 2U; ++i) {
    Engine engine(options);
    engine.get_proc_manager()->pre_register("the_task", the_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      ErrorStack ret = engine.get_thread_pool()->impersonate_synchronous("the_task");
      COERCE_ERROR(ret);
      COERCE_ERROR(engine.uninitialize());
    }
  }
}

}  // namespace snapshot
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SnapshotArrayIssue127Test, foedus.snapshot);
