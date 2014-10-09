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
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace hash {
DEFINE_TEST_CASE_PACKAGE(HashBasicTest, foedus.storage.hash);
TEST(HashBasicTest, Create) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("test", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack query_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  HashStorage hash = context->get_engine()->get_storage_manager()->get_hash("test2");
  char buf[16];
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  char key[100];
  std::memset(key, 0, 100);
  uint16_t payload_capacity = 16;
  ErrorCode result = hash.get_record(context, key, 100, buf, &payload_capacity);
  if (result == kErrorCodeStrKeyNotFound) {
    std::cout << "Key not found!" << std::endl;
  } else if (result != kErrorCodeOk) {
    return ERROR_STACK(result);
  }
  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(HashBasicTest, CreateAndQuery) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register(proc::ProcAndName("query_task", query_task));
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("test2", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("query_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack insert_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  HashStorage hash = context->get_engine()->get_storage_manager()->get_hash("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  uint64_t key = 12345ULL;
  uint64_t data = 897565433333126ULL;
  CHECK_ERROR(hash.insert_record(context, &key, sizeof(key), &data, sizeof(data)));
  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(HashBasicTest, CreateAndInsert) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register(proc::ProcAndName("insert_task", insert_task));
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("ggg", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("insert_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack insert_and_read_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  HashStorage hash = context->get_engine()->get_storage_manager()->get_hash("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  uint64_t key = 12345ULL;
  uint64_t data = 897565433333126ULL;
  CHECK_ERROR(hash.insert_record(context, &key, sizeof(key), &data, sizeof(data)));
  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  uint64_t data2;
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  uint16_t data_capacity = sizeof(data2);
  CHECK_ERROR(hash.get_record(context, &key, sizeof(key), &data2, &data_capacity));
  EXPECT_EQ(data, data2);
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(HashBasicTest, CreateAndInsertAndRead) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("insert_and_read_task", insert_and_read_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("ggg", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("insert_and_read_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack overwrite_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  HashStorage hash = context->get_engine()->get_storage_manager()->get_hash("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  uint64_t key = 12345ULL;
  uint64_t data = 897565433333126ULL;
  CHECK_ERROR(hash.insert_record(context, &key, sizeof(key), &data, sizeof(data)));
  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  uint64_t data2 = 321654987ULL;
  CHECK_ERROR(hash.overwrite_record(context, key, &data2, 0, sizeof(data2)));
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  uint64_t data3;
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(hash.get_record_primitive(context, key, &data3, 0));
  EXPECT_EQ(data2, data3);
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  CHECK_ERROR(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(HashBasicTest, Overwrite) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("overwrite_task", overwrite_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("ggg", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("overwrite_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}
TEST(HashBasicTest, CreateAndDrop) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashMetadata meta("dd", 8);
    HashStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_hash(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_storage_manager()->drop_storage(storage.get_id(), &epoch));
    EXPECT_TRUE(!storage.exists());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(HashBasicTest, foedus.storage.hash);
