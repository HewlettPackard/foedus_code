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

#include <cstring>
#include <iostream>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_route.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/array/array_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace array {
DEFINE_TEST_CASE_PACKAGE(ArrayBasicTest, foedus.storage.array);
TEST(ArrayBasicTest, RangeCalculation) {
  LookupRoute route;
  route.route[0] = 3;
  route.route[1] = 4;
  route.route[2] = 5;
  route.route[3] = 7;
  const uint8_t kLevels = 4;
  const uint16_t kPayload = 200;  // about 20 per leaf page
  const uint64_t kRecordsInLeaf = to_records_in_leaf(kPayload);
  const uint64_t kArraySize = kRecordsInLeaf * kInteriorFanout * kInteriorFanout * kInteriorFanout;
  std::vector<uint64_t> pages = ArrayStoragePimpl::calculate_required_pages(kArraySize, kPayload);
  EXPECT_EQ(kLevels, pages.size());
  uint64_t base = 7 * kRecordsInLeaf * kInteriorFanout * kInteriorFanout;
  ArrayRange range = route.calculate_page_range(0, kLevels, kPayload, kArraySize);
  EXPECT_EQ(4 * kRecordsInLeaf + 5 * kRecordsInLeaf * kInteriorFanout + base, range.begin_);
  EXPECT_EQ(5 * kRecordsInLeaf + 5 * kRecordsInLeaf * kInteriorFanout + base, range.end_);
  range = route.calculate_page_range(1, kLevels, kPayload, kArraySize);
  EXPECT_EQ(5 * kRecordsInLeaf * kInteriorFanout + base, range.begin_);
  EXPECT_EQ(6 * kRecordsInLeaf * kInteriorFanout + base, range.end_);
  range = route.calculate_page_range(2, kLevels, kPayload, kArraySize);
  EXPECT_EQ(base, range.begin_);
  EXPECT_EQ(base + kRecordsInLeaf * kInteriorFanout * kInteriorFanout, range.end_);
  range = route.calculate_page_range(3, kLevels, kPayload, kArraySize);
  EXPECT_EQ(0, range.begin_);
  EXPECT_EQ(kRecordsInLeaf * kInteriorFanout * kInteriorFanout * kInteriorFanout, range.end_);
}
TEST(ArrayBasicTest, RangeCalculation2) {
  LookupRoute route;
  route.route[0] = 3;
  route.route[1] = 4;
  route.route[2] = 5;
  route.route[3] = kInteriorFanout - 1;
  const uint8_t kLevels = 4;
  const uint16_t kPayload = 200;
  const uint64_t kRecordsInLeaf = to_records_in_leaf(kPayload);
  const uint64_t kArraySize
    = kRecordsInLeaf * kInteriorFanout * kInteriorFanout * kInteriorFanout - 10U;
  std::vector<uint64_t> pages = ArrayStoragePimpl::calculate_required_pages(kArraySize, kPayload);
  EXPECT_EQ(kLevels, pages.size());
  uint64_t base = (kInteriorFanout - 1U) * kRecordsInLeaf * kInteriorFanout * kInteriorFanout;
  ArrayRange range = route.calculate_page_range(0, kLevels, kPayload, kArraySize);
  EXPECT_EQ(4 * kRecordsInLeaf + 5 * kRecordsInLeaf * kInteriorFanout + base, range.begin_);
  EXPECT_EQ(5 * kRecordsInLeaf + 5 * kRecordsInLeaf * kInteriorFanout + base, range.end_);
  range = route.calculate_page_range(1, kLevels, kPayload, kArraySize);
  EXPECT_EQ(5 * kRecordsInLeaf * kInteriorFanout + base, range.begin_);
  EXPECT_EQ(6 * kRecordsInLeaf * kInteriorFanout + base, range.end_);
  range = route.calculate_page_range(2, kLevels, kPayload, kArraySize);
  EXPECT_EQ(base, range.begin_);
  EXPECT_EQ(kArraySize, range.end_);
  range = route.calculate_page_range(3, kLevels, kPayload, kArraySize);
  EXPECT_EQ(0, range.begin_);
  EXPECT_EQ(kArraySize, range.end_);
}

TEST(ArrayBasicTest, Create) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    ArrayMetadata meta("test", 16, 100);
    ArrayStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack query_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  ArrayStorage array = context->get_engine()->get_storage_manager()->get_array("test2");
  char buf[16];
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));

  CHECK_ERROR(array.get_record(context, 24, buf));

  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(ArrayBasicTest, CreateAndQuery) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("query_task", query_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    ArrayMetadata meta("test2", 16, 100);
    ArrayStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("query_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(ArrayBasicTest, CreateAndDrop) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    ArrayMetadata meta("dd", 16, 100);
    ArrayStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_storage_manager()->drop_storage(storage.get_id(), &epoch));
    EXPECT_FALSE(storage.exists());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack write_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  ArrayStorage array = context->get_engine()->get_storage_manager()->get_array("test3");
  char buf[16];
  std::memset(buf, 2, 16);
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));

  CHECK_ERROR(array.overwrite_record(context, 24, buf));

  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(ArrayBasicTest, CreateAndWrite) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("write_task", write_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    ArrayMetadata meta("test3", 16, 100);
    ArrayStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("write_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack read_write_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  ArrayStorage array = context->get_engine()->get_storage_manager()->get_array("test4");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();

  // Write values first
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  for (int i = 0; i < 100; ++i) {
    uint64_t buf[2];
    buf[0] = i * 46 + 123;
    buf[1] = i * 6534 + 665;
    CHECK_ERROR(array.overwrite_record(context, i, buf));
  }
  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));

  // Then, read values
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  for (int i = 0; i < 100; ++i) {
    uint64_t buf[2];
    CHECK_ERROR(array.get_record(context, i, buf));
    EXPECT_EQ(i * 46 + 123, buf[0]);
    EXPECT_EQ(i * 6534 + 665, buf[1]);
  }
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(ArrayBasicTest, CreateAndReadWrite) {
  EngineOptions options = get_tiny_options();
  options.log_.log_buffer_kb_ = 1 << 10;  // larger to do all writes in one shot
  Engine engine(options);
  engine.get_proc_manager()->pre_register("read_write_task", read_write_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    ArrayMetadata meta("test4", 16, 100);
    ArrayStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("read_write_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace array
}  // namespace storage
}  // namespace foedus


TEST_MAIN_CAPTURE_SIGNALS(ArrayBasicTest, foedus.storage.array);
