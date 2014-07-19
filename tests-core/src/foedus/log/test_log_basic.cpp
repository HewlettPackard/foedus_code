/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/log/common_log_types.hpp"
#include "foedus/log/log_options.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_inl.hpp"
#include "foedus/xct/xct_manager.hpp"

/**
 * @file test_log_basic.cpp
 * Basic testcases for logging.
 */
namespace foedus {
namespace log {
DEFINE_TEST_CASE_PACKAGE(LogBasicTest, foedus.log);

class WriteLogTask : public thread::ImpersonateTask {
 public:
  ErrorStack run(thread::Thread* context) {
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
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

    // hacky. just to make this transaction read-write.
    xct::XctId dummy_record;
    context->get_current_xct().add_to_write_set(
      reinterpret_cast<storage::Storage*>(&dummy_record),
      reinterpret_cast<storage::Record*>(&dummy_record),
      reinterpret_cast<RecordLogType*>(filler));

    EXPECT_EQ(committed_before, buffer.get_offset_committed());
    EXPECT_EQ(durable_before, buffer.get_offset_durable());
    EXPECT_EQ(tail_before + 400 + 512, buffer.get_offset_tail());

    Epoch commit_epoch;
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    buffer.assert_consistent();

    EXPECT_EQ(committed_before + 400 + 512, buffer.get_offset_committed());
    EXPECT_EQ(tail_before + 400 + 512, buffer.get_offset_tail());
    buffer.assert_consistent();

    WRAP_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
    EXPECT_EQ(committed_before + 400 + 512, buffer.get_offset_committed());
    EXPECT_EQ(tail_before + 400 + 512, buffer.get_offset_tail());
    EXPECT_EQ(buffer.get_offset_durable(), tail_before + 400 + 512);
    buffer.assert_consistent();
    return kRetOk;
  }
};

TEST(LogBasicTest, WriteLog) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    WriteLogTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

class BufferWrapAroundTask : public thread::ImpersonateTask {
 public:
  ErrorStack run(thread::Thread* context) {
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
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

    xct::XctId dummy_record;
    context->get_current_xct().add_to_write_set(
      reinterpret_cast<storage::Storage*>(&dummy_record),
      reinterpret_cast<storage::Record*>(&dummy_record),
      reinterpret_cast<RecordLogType*>(filler));

    buffer.assert_consistent();
    EXPECT_EQ(0, buffer.get_offset_committed());
    EXPECT_EQ(0, buffer.get_offset_durable());
    EXPECT_EQ(kBufferSize - 128, buffer.get_offset_tail());

    Epoch commit_epoch;
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

    buffer.assert_consistent();
    EXPECT_EQ(kBufferSize - 128, buffer.get_offset_committed());
    EXPECT_EQ(kBufferSize - 128, buffer.get_offset_tail());

    committed_before = buffer.get_offset_committed();
    durable_before = buffer.get_offset_durable();
    tail_before = buffer.get_offset_tail();

    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    buffer.assert_consistent();
    // this should cause wrap around
    filler = reinterpret_cast<FillerLogType*>(buffer.reserve_new_log(256));
    filler->populate(256);
    EXPECT_EQ(kBufferSize - 128, buffer.get_offset_committed());
    EXPECT_EQ(kBufferSize - 128, buffer.get_offset_durable());
    EXPECT_EQ(256, buffer.get_offset_tail());  // a log doesn't span the end of buffer.
    buffer.assert_consistent();

    // hacky. just to make this transaction read-write.
    context->get_current_xct().add_to_write_set(
      reinterpret_cast<storage::Storage*>(&dummy_record),
      reinterpret_cast<storage::Record*>(&dummy_record),
      reinterpret_cast<RecordLogType*>(filler));
    buffer.assert_consistent();

    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    EXPECT_EQ(256, buffer.get_offset_committed());
    EXPECT_EQ(kBufferSize - 128, buffer.get_offset_durable());
    EXPECT_EQ(256, buffer.get_offset_tail());
    buffer.assert_consistent();

    WRAP_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
    EXPECT_EQ(256, buffer.get_offset_committed());
    EXPECT_EQ(256, buffer.get_offset_durable());
    EXPECT_EQ(256, buffer.get_offset_tail());
    buffer.assert_consistent();
    return kRetOk;
  }
};

TEST(LogBasicTest, BufferWrapAround) {
  EngineOptions options = get_tiny_options();

  // make it extremely small so that we can test wrap around
  options.log_.log_buffer_kb_ = 16;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    BufferWrapAroundTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace log
}  // namespace foedus
