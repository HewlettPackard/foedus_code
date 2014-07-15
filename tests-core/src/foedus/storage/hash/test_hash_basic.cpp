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
    HashStorage* out;
    Epoch commit_epoch;
    HashMetadata meta("test", 8);
    COERCE_ERROR(engine.get_storage_manager().create_hash(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

class QueryTask : public thread::ImpersonateTask {
 public:
  ErrorStack run(thread::Thread* context) {
    HashStorage *hash =
      dynamic_cast<HashStorage*>(
        context->get_engine()->get_storage_manager().get_storage("test2"));
    char buf[16];
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    CHECK_ERROR(xct_manager.begin_xct(context, xct::kSerializable));
    char key[100];
    std::memset(key, 0, 100);
    uint16_t payload_capacity = 16;
    ErrorCode result = hash->get_record(context, key, 100, buf, &payload_capacity);
    if (result == kErrorCodeStrKeyNotFound) {
      std::cout << "Key not found!" << std::endl;
    } else if (result != kErrorCodeOk) {
      return ERROR_STACK(result);
    }
    Epoch commit_epoch;
    CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
    CHECK_ERROR(xct_manager.wait_for_commit(commit_epoch));
    return foedus::kRetOk;
  }
};

TEST(HashBasicTest, CreateAndQuery) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    HashStorage* out;
    Epoch commit_epoch;
    HashMetadata meta("test2", 8);
    COERCE_ERROR(engine.get_storage_manager().create_hash(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    QueryTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
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
    HashStorage* out;
    Epoch commit_epoch;
    HashMetadata meta("dd", 8);
    COERCE_ERROR(engine.get_storage_manager().create_hash(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    COERCE_ERROR(engine.get_storage_manager().drop_storage(out->get_id(), &commit_epoch));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(HashBasicTest, Test1) {  // (name of package, test case's name) //gtest
  /*
  int a = 2 * 3;
  int *array = new int[3];
  array[1000] = 6;
  EXPECT_EQ(7, a);
  */
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus
