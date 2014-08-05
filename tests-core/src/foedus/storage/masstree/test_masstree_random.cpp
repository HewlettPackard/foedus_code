/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <cstring>
#include <iostream>
#include <set>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace masstree {
DEFINE_TEST_CASE_PACKAGE(MasstreeRandomTest, foedus.storage.masstree);

class InsertManyNormalizedTask : public thread::ImpersonateTask {
 public:
  ErrorStack run(thread::Thread* context) {
    MasstreeStorage *masstree =
      dynamic_cast<MasstreeStorage*>(
        context->get_engine()->get_storage_manager().get_storage("test2"));
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();

    // insert a lot
    const uint32_t kCount = 10000U;
    const uint16_t kBufSize = 200;
    char buf[kBufSize];
    std::memset(buf, 0, kBufSize);
    Epoch commit_epoch;
    std::set<KeySlice> inserted;
    {
      assorted::UniformRandom uniform_random(123456L);
      WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
      for (uint32_t i = 0; i < kCount; ++i) {
        KeySlice key = normalize_primitive<uint64_t>(uniform_random.next_uint64());
        if (inserted.find(key) != inserted.end()) {
          std::cout << "already inserted" << key << std::endl;
          continue;
        }
        inserted.insert(key);
        *reinterpret_cast<uint64_t*>(buf + 123) = key;
        WRAP_ERROR_CODE(masstree->insert_record_normalized(context, key, buf, kBufSize));
        if (i % 50 == 0) {
          WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
          WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
          std::cout << "inserting:" << i << "/" << kCount << std::endl;
        }
        if (i % 1000 == 0) {
          CHECK_ERROR(masstree->verify_single_thread(context));
        }
      }
      WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    }

    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    CHECK_ERROR(masstree->verify_single_thread(context));
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

    // read it back
    {
      char buf2[kBufSize];
      assorted::UniformRandom uniform_random(123456L);
      WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
      for (uint32_t i = 0; i < kCount; ++i) {
        KeySlice key = normalize_primitive<uint64_t>(uniform_random.next_uint64());
        *reinterpret_cast<uint64_t*>(buf + 123) = key;
        uint16_t capacity = kBufSize;
        WRAP_ERROR_CODE(masstree->get_record_normalized(context, key, buf2, &capacity));
        EXPECT_EQ(kBufSize, capacity);
        EXPECT_EQ(std::string(buf, kBufSize), std::string(buf2, kBufSize));
        if (i % 20 == 0) {
          WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
          WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
          std::cout << "reading:" << i << "/" << kCount << std::endl;
        }
      }
      WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    }

    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    CHECK_ERROR(masstree->verify_single_thread(context));
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

    WRAP_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
    return foedus::kRetOk;
  }
};

TEST(MasstreeBasicTest, InsertManyNormalized) {
  EngineOptions options = get_tiny_options();
  options.memory_.page_pool_size_mb_per_node_ = 64;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeStorage* out;
    Epoch commit_epoch;
    MasstreeMetadata meta("test2");
    COERCE_ERROR(engine.get_storage_manager().create_masstree(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    InsertManyNormalizedTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

class InsertManyTask : public thread::ImpersonateTask {
 public:
  ErrorStack run(thread::Thread* context) {
    MasstreeStorage *masstree =
      dynamic_cast<MasstreeStorage*>(
        context->get_engine()->get_storage_manager().get_storage("test2"));
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();

    // insert a lot of variable-length keys
    const uint32_t kCount = 10000U;
    const uint16_t kBufSize = 200;
    char buf[kBufSize];
    const uint16_t kMaxLen = 32;
    char key_buf[kMaxLen];
    std::memset(buf, 0, kBufSize);
    Epoch commit_epoch;
    std::set<std::string> inserted;
    std::string answers[kCount];
    {
      assorted::UniformRandom uniform_random(123456L);
      WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
      for (uint32_t i = 0; i < kCount; ++i) {
        uint8_t len = 1 + uniform_random.next_uint32() % 24;
        for (uint32_t j = 0; j < len; ++j) {
          key_buf[j] = static_cast<char>(uniform_random.next_uint32());
        }
        std::string key(key_buf, len);
        if (inserted.find(key) != inserted.end()) {
          std::cout << "already inserted" << key << std::endl;
          continue;
        }
        inserted.insert(key);
        answers[i] = key;
        *reinterpret_cast<uint64_t*>(buf + 123) = i;
        WRAP_ERROR_CODE(masstree->insert_record(context, key_buf, len, buf, kBufSize));
        if (i % 50 == 0) {
          WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
          WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
          std::cout << "inserting:" << i << "/" << kCount << std::endl;
        }
        if (i % 1000 == 0) {
          CHECK_ERROR(masstree->verify_single_thread(context));
        }
      }
      WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    }

    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    CHECK_ERROR(masstree->verify_single_thread(context));
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

    // read it back
    {
      char buf2[kBufSize];
      WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
      for (uint32_t i = 0; i < kCount; ++i) {
        *reinterpret_cast<uint64_t*>(buf + 123) = i;
        uint16_t capacity = kBufSize;
        std::string key = answers[i];
        WRAP_ERROR_CODE(masstree->get_record(context, key.data(), key.size(), buf2, &capacity));
        EXPECT_EQ(kBufSize, capacity);
        EXPECT_EQ(std::string(buf, kBufSize), std::string(buf2, kBufSize));
        if (i % 20 == 0) {
          WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
          WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
          std::cout << "reading:" << i << "/" << kCount << std::endl;
        }
      }
      WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    }

    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    CHECK_ERROR(masstree->verify_single_thread(context));
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

    WRAP_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
    return foedus::kRetOk;
  }
};

TEST(MasstreeBasicTest, InsertMany) {
  /* TODO(Hideaki) implemented next-layer moved bit tracking
  EngineOptions options = get_tiny_options();
  options.memory_.page_pool_size_mb_per_node_ = 64;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeStorage* out;
    Epoch commit_epoch;
    MasstreeMetadata meta("test2");
    COERCE_ERROR(engine.get_storage_manager().create_masstree(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    InsertManyTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
  */
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
