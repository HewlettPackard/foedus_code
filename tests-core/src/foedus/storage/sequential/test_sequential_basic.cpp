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
#include "foedus/proc/proc_manager.hpp"
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
    SequentialMetadata meta("test");
    SequentialStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_sequential(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
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
    SequentialMetadata meta("dd");
    SequentialStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_sequential(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_storage_manager()->drop_storage(storage.get_id(), &epoch));
    EXPECT_FALSE(storage.exists());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack write_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  SequentialStorage sequential
    = context->get_engine()->get_storage_manager()->get_sequential("test3");
  EXPECT_TRUE(sequential.exists());
  char buf[16];
  std::memset(buf, 2, 16);
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

  WRAP_ERROR_CODE(sequential.append_record(context, buf, 16));

  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(SequentialBasicTest, CreateAndWrite) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register(proc::ProcAndName("write_task", write_task));
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    SequentialMetadata meta("test3");
    SequentialStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_sequential(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("write_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SequentialBasicTest, foedus.storage.sequential);
