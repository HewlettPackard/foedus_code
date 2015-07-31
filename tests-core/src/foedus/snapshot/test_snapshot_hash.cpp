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
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

/**
 * @file test_snapshot_hash.cpp
 * Snapshot for hash storage.
 */
namespace foedus {
namespace snapshot {
DEFINE_TEST_CASE_PACKAGE(SnapshotHashTest, foedus.snapshot);
const uint32_t kRecords = 1024;
const uint32_t kThreads = 2;
const storage::StorageName kName("test");
const uint64_t kDataAddendum = 42U;

ErrorStack inserts_fixed_len_task(const proc::ProcArguments& args) {
  EXPECT_EQ(sizeof(uint32_t), args.input_len_);
  uint32_t id = *reinterpret_cast<const uint32_t*>(args.input_buffer_);
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::hash::HashStorage hash(args.engine_, kName);
  ASSERT_ND(hash.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < kRecords / 2U; ++i) {
    uint64_t key = id * kRecords / 2U + i;
    uint64_t data = key + kDataAddendum;
    WRAP_ERROR_CODE(hash.insert_record(context, &key, sizeof(key), &data, sizeof(data)));
  }

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

ErrorStack verify_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  storage::hash::HashStorage hash(args.engine_, kName);
  ASSERT_ND(hash.exists());
  CHECK_ERROR(hash.verify_single_thread(context));
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  for (uint32_t i = 0; i < kRecords; ++i) {
    uint64_t key = i;
    uint64_t data;
    uint16_t capacity = sizeof(data);
    ErrorCode ret = hash.get_record(context, &key, sizeof(key), &data, &capacity);
    EXPECT_EQ(kErrorCodeOk, ret) << i;
    EXPECT_EQ(i, key) << i;
    EXPECT_EQ(i + kDataAddendum, data) << i;
    EXPECT_EQ(sizeof(data), capacity) << i;
  }

  Epoch commit_epoch;
  ErrorCode committed = xct_manager->precommit_xct(context, &commit_epoch);
  EXPECT_EQ(kErrorCodeOk, committed);
  return kRetOk;
}

ErrorStack inserts_varlen_task(const proc::ProcArguments& args) {
  EXPECT_EQ(sizeof(uint32_t), args.input_len_);
  uint32_t id = *reinterpret_cast<const uint32_t*>(args.input_buffer_);
  EXPECT_NE(id, 2U);

  thread::Thread* context = args.context_;
  storage::hash::HashStorage hash(args.engine_, kName);
  ASSERT_ND(hash.exists());
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  Epoch commit_epoch;

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  char buffer[16];
  std::memset(buffer, 0, sizeof(buffer));
  for (uint32_t i = 0; i < kRecords / 2U; ++i) {
    uint64_t rec = id * kRecords / 2U + i;
    // first 8 bytes, mod 17 to have next layers.
    assorted::write_bigendian<uint64_t>(static_cast<uint64_t>(rec % 17U), buffer);
    // and 1-4 bytes of decimal representation in text
    std::string str = std::to_string(rec);
    std::memcpy(buffer + sizeof(uint64_t), str.data(), str.size());
    uint16_t len = sizeof(uint64_t) + str.size();
    uint64_t data = rec + kDataAddendum;
    ErrorCode ret = hash.insert_record(context, buffer, len, &data, sizeof(data));
    EXPECT_EQ(kErrorCodeOk, ret) << i;
  }

  // CHECK_ERROR(hash.debugout_single_thread(args.engine_));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  // CHECK_ERROR(hash.debugout_single_thread(args.engine_));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return kRetOk;
}

ErrorStack verify_varlen_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  storage::hash::HashStorage hash(args.engine_, kName);
  ASSERT_ND(hash.exists());
  CHECK_ERROR(hash.verify_single_thread(context));
  // CHECK_ERROR(hash.debugout_single_thread(args.engine_));
  xct::XctManager* xct_manager = args.engine_->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  char buffer[16];
  std::memset(buffer, 0, sizeof(buffer));
  for (uint32_t i = 0; i < kRecords; ++i) {
    uint64_t rec = i;
    assorted::write_bigendian<uint64_t>(static_cast<uint64_t>(rec % 17U), buffer);
    std::string str = std::to_string(rec);
    std::memcpy(buffer + sizeof(uint64_t), str.data(), str.size());
    uint16_t len = sizeof(uint64_t) + str.size();

    uint64_t data;
    uint16_t capacity = sizeof(data);
    ErrorCode ret = hash.get_record(context, buffer, len, &data, &capacity);
    EXPECT_EQ(kErrorCodeOk, ret) << i;
    EXPECT_EQ(i + kDataAddendum, data) << i;
    EXPECT_EQ(sizeof(data), capacity) << i;
  }

  Epoch commit_epoch;
  ErrorCode committed = xct_manager->precommit_xct(context, &commit_epoch);
  EXPECT_EQ(kErrorCodeOk, committed);
  return kRetOk;
}

void test_run(
  const proc::ProcName& proc_name,
  const proc::ProcName& verify_name,
  uint8_t bin_bits,
  bool multiple_loggers,
  bool multiple_partitions) {
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
  options.memory_.page_pool_size_mb_per_node_ = 10;
  options.memory_.page_pool_size_mb_per_node_ *= 2U;  // for rigorous_check
  options.cache_.snapshot_cache_size_mb_per_node_ = 10;
  options.cache_.snapshot_cache_size_mb_per_node_ *= 2U;  // for rigorous_check

  {
    Engine engine(options);
    engine.get_proc_manager()->pre_register("inserts_fixed_len_task", inserts_fixed_len_task);
    engine.get_proc_manager()->pre_register("inserts_varlen_task", inserts_varlen_task);
    engine.get_proc_manager()->pre_register("verify_task", verify_task);
    engine.get_proc_manager()->pre_register("verify_varlen_task", verify_varlen_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      storage::hash::HashStorage out;
      Epoch commit_epoch;
      storage::hash::HashMetadata meta(kName, bin_bits);
      COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &out, &commit_epoch));
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
      COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous(verify_name));
      EXPECT_TRUE(out.exists());
      engine.get_snapshot_manager()->trigger_snapshot_immediate(true);
      EXPECT_TRUE(out.exists());

      COERCE_ERROR(engine.uninitialize());
    }
  }
  {
    Engine engine(options);
    engine.get_proc_manager()->pre_register("verify_task", verify_task);
    engine.get_proc_manager()->pre_register("verify_varlen_task", verify_varlen_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous(verify_name));
      COERCE_ERROR(engine.uninitialize());
    }
  }
  cleanup_test(options);
}

// the hash composer logic significantly differs if it's 1-level. test them separately.
// 2Lv <-> 3Lv is also slightly different. maybe we should separate it too
const uint8_t k1Lv = 7;
const uint8_t k2Lv = 12;

const proc::ProcName kInsN("inserts_fixed_len_task");
const proc::ProcName kInsV("inserts_varlen_task");
const proc::ProcName kVerN("verify_task");
const proc::ProcName kVerV("verify_varlen_task");

TEST(SnapshotHashTest, InsertsFixedLenOneLogger1Lv) { test_run(kInsN, kVerN, k1Lv, false, false); }
TEST(SnapshotHashTest, InsertsFixedLenOneLogger2Lv) { test_run(kInsN, kVerN, k2Lv, false, false); }

TEST(SnapshotHashTest, InsertsFixedLenTwoLoggers1Lv) { test_run(kInsN, kVerN, k1Lv, true, false); }
TEST(SnapshotHashTest, InsertsFixedLenTwoLoggers2Lv) { test_run(kInsN, kVerN, k2Lv, true, false); }

TEST(SnapshotHashTest, InsertsFixedLenTwoPartitions1Lv) {
                                                  test_run(kInsN, kVerN, k1Lv, true, true); }
TEST(SnapshotHashTest, InsertsFixedLenTwoPartitions2Lv) {
                                                  test_run(kInsN, kVerN, k2Lv, true, true); }

TEST(SnapshotHashTest, InsertsVarlenOneLogger1Lv) { test_run(kInsV, kVerV, k1Lv, false, false); }
TEST(SnapshotHashTest, InsertsVarlenOneLogger2Lv) { test_run(kInsV, kVerV, k2Lv, false, false); }

TEST(SnapshotHashTest, InsertsVarlenTwoLoggers1Lv) { test_run(kInsV, kVerV, k1Lv, true, false); }
TEST(SnapshotHashTest, InsertsVarlenTwoLoggers2Lv) { test_run(kInsV, kVerV, k2Lv, true, false); }

TEST(SnapshotHashTest, InsertsVarlenTwoPartitions1Lv) { test_run(kInsV, kVerV, k1Lv, true, true); }
TEST(SnapshotHashTest, InsertsVarlenTwoPartitions2Lv) { test_run(kInsV, kVerV, k2Lv, true, true); }

}  // namespace snapshot
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SnapshotHashTest, foedus.snapshot);
