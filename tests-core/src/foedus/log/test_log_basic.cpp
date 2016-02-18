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
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_options.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

/**
 * @file test_log_basic.cpp
 * Basic testcases for logging.
 */
namespace foedus {
namespace log {
DEFINE_TEST_CASE_PACKAGE(LogBasicTest, foedus.log);

constexpr storage::StorageId kDummyStorageId = 12345;
constexpr storage::StorageId kDummyPayload = 8;
storage::array::ArrayPage* prepare_dummy_page(
  thread::Thread* context,
  memory::PagePoolOffset offset) {
  storage::array::ArrayPage* dummy_page
    = reinterpret_cast<storage::array::ArrayPage*>(
        context->get_local_volatile_page_resolver().resolve_offset_newpage(offset));
  storage::VolatilePagePointer page_id
    = storage::combine_volatile_page_pointer(context->get_numa_node(), 0, 0, offset);
  storage::array::ArrayRange dummy_range(0, 128);
  dummy_page->initialize_volatile_page(
    Epoch(1U),
    kDummyStorageId,
    page_id,
    kDummyPayload,
    0,
    dummy_range);
  return dummy_page;
}

ErrorStack test_write_log(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  ThreadLogBuffer& buffer = context->get_thread_log_buffer();

  uint64_t committed_before = buffer.get_offset_committed();
  uint64_t durable_before = buffer.get_offset_durable();
  uint64_t tail_before = buffer.get_offset_tail();
  EXPECT_EQ(0, committed_before);
  EXPECT_EQ(0, durable_before);
  EXPECT_EQ(0, tail_before);

  buffer.assert_consistent();
  FillerLogType* filler = reinterpret_cast<FillerLogType*>(buffer.reserve_new_log(400));
  filler->populate(400);

  buffer.assert_consistent();
  filler = reinterpret_cast<FillerLogType*>(buffer.reserve_new_log(512));
  filler->populate(512);
  buffer.assert_consistent();

  // just to make this transaction read-write.
  auto dummy_offset = context->get_thread_memory()->grab_free_volatile_page();
  storage::array::ArrayPage* dummy_page = prepare_dummy_page(context, dummy_offset);
  context->get_current_xct().add_to_write_set(
    kDummyStorageId,
    &dummy_page->get_leaf_record(0, kDummyPayload)->owner_id_,
    dummy_page->get_leaf_record(0, kDummyPayload)->payload_,
    reinterpret_cast<RecordLogType*>(filler));

  EXPECT_EQ(committed_before, buffer.get_offset_committed());
  EXPECT_EQ(durable_before, buffer.get_offset_durable());
  EXPECT_EQ(tail_before + 400 + 512, buffer.get_offset_tail());

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  buffer.assert_consistent();

  EXPECT_EQ(committed_before + 400 + 512, buffer.get_offset_committed());
  EXPECT_EQ(tail_before + 400 + 512, buffer.get_offset_tail());
  buffer.assert_consistent();

  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  EXPECT_EQ(committed_before + 400 + 512, buffer.get_offset_committed());
  EXPECT_EQ(tail_before + 400 + 512, buffer.get_offset_tail());
  EXPECT_EQ(buffer.get_offset_durable(), tail_before + 400 + 512);
  buffer.assert_consistent();

  context->get_thread_memory()->release_free_volatile_page(dummy_offset);
  return kRetOk;
}

TEST(LogBasicTest, WriteLog) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register(proc::ProcAndName("test_write_log", test_write_log));
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("test_write_log"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack test_buffer_wrap_around(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  ThreadLogBuffer& buffer = context->get_thread_log_buffer();

  uint64_t committed_before = buffer.get_offset_committed();
  uint64_t durable_before = buffer.get_offset_durable();
  uint64_t tail_before = buffer.get_offset_tail();
  EXPECT_EQ(0, committed_before);
  EXPECT_EQ(0, durable_before);
  EXPECT_EQ(0, tail_before);

  buffer.assert_consistent();
  const uint16_t kBufferSize = 1 << 14;
  FillerLogType* filler = reinterpret_cast<FillerLogType*>(buffer.reserve_new_log(
    kBufferSize - 128));
  filler->populate(kBufferSize - 128);

  auto dummy_offset = context->get_thread_memory()->grab_free_volatile_page();
  storage::array::ArrayPage* dummy_page = prepare_dummy_page(context, dummy_offset);
  context->get_current_xct().add_to_write_set(
    kDummyStorageId,
    &dummy_page->get_leaf_record(0, kDummyPayload)->owner_id_,
    dummy_page->get_leaf_record(0, kDummyPayload)->payload_,
    reinterpret_cast<RecordLogType*>(filler));

  buffer.assert_consistent();
  EXPECT_EQ(0, buffer.get_offset_committed());
  EXPECT_EQ(0, buffer.get_offset_durable());
  EXPECT_EQ(kBufferSize - 128, buffer.get_offset_tail());

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  buffer.assert_consistent();
  EXPECT_EQ(kBufferSize - 128, buffer.get_offset_committed());
  EXPECT_EQ(kBufferSize - 128, buffer.get_offset_tail());

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  buffer.assert_consistent();
  // this should cause wrap around
  filler = reinterpret_cast<FillerLogType*>(buffer.reserve_new_log(256));
  filler->populate(256);
  EXPECT_EQ(kBufferSize - 128, buffer.get_offset_committed());
  EXPECT_EQ(256, buffer.get_offset_tail());  // a log doesn't span the end of buffer.
  buffer.assert_consistent();

  // just to make this transaction read-write.
  context->get_current_xct().add_to_write_set(
    kDummyStorageId,
    &dummy_page->get_leaf_record(0, kDummyPayload)->owner_id_,
    dummy_page->get_leaf_record(0, kDummyPayload)->payload_,
    reinterpret_cast<RecordLogType*>(filler));
  buffer.assert_consistent();

  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  EXPECT_EQ(256, buffer.get_offset_committed());
  EXPECT_EQ(256, buffer.get_offset_tail());
  buffer.assert_consistent();

  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  EXPECT_EQ(256, buffer.get_offset_committed());
  EXPECT_EQ(256, buffer.get_offset_durable());
  EXPECT_EQ(256, buffer.get_offset_tail());
  buffer.assert_consistent();
  return kRetOk;
}

TEST(LogBasicTest, BufferWrapAround) {
  EngineOptions options = get_tiny_options();

  // make it extremely small so that we can test wrap around
  options.log_.log_buffer_kb_ = 16;
  Engine engine(options);
  engine.get_proc_manager()->pre_register(proc::ProcAndName(
    "test_buffer_wrap_around",
    test_buffer_wrap_around));
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("test_buffer_wrap_around"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace log
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(LogBasicTest, foedus.log);
