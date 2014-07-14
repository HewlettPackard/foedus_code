/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <cstring>
#include <iostream>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace sequential {
DEFINE_TEST_CASE_PACKAGE(SequentialBasicTest, foedus.storage.sequential);
TEST(SequentialBasicTest, Create) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    SequentialStorage* out;
    Epoch commit_epoch;
    SequentialMetadata meta("test");
    COERCE_ERROR(engine.get_storage_manager().create_sequential(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(SequentialBasicTest, CreateAndDrop) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    SequentialStorage* out;
    Epoch commit_epoch;
    SequentialMetadata meta("dd");
    COERCE_ERROR(engine.get_storage_manager().create_sequential(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    COERCE_ERROR(engine.get_storage_manager().drop_storage(out->get_id(), &commit_epoch));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

class WriteTask : public thread::ImpersonateTask {
 public:
  ErrorStack run(thread::Thread* context) {
    SequentialStorage *sequential =
      dynamic_cast<SequentialStorage*>(
        context->get_engine()->get_storage_manager().get_storage("test3"));
    char buf[16];
    std::memset(buf, 2, 16);
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));

    WRAP_ERROR_CODE(sequential->append_record(context, buf, 16));

    Epoch commit_epoch;
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    WRAP_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
    return foedus::kRetOk;
  }
};

TEST(SequentialBasicTest, CreateAndWrite) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    SequentialStorage* out;
    Epoch commit_epoch;
    SequentialMetadata meta("test3");
    COERCE_ERROR(engine.get_storage_manager().create_sequential(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    WriteTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
