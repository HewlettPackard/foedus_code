/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <cstring>
#include <iostream>
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
DEFINE_TEST_CASE_PACKAGE(MasstreeSplitTest, foedus.storage.masstree);

class SplitBorderTask : public thread::ImpersonateTask {
 public:
  ErrorStack run(thread::Thread* context) {
    MasstreeStorage *masstree =
      dynamic_cast<MasstreeStorage*>(
        context->get_engine()->get_storage_manager().get_storage("ggg"));
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    assorted::UniformRandom uniform_random(123456);
    uint64_t keys[32];
    std::string answers[32];
    Epoch commit_epoch;
    for (uint32_t rep = 0; rep < 32; ++rep) {
      WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
      uint64_t key = uniform_random.next_uint64();
      keys[rep] = key;
      char data[200];
      std::memset(data, 0, 200);
      std::memcpy(data + 123, &key, sizeof(key));
      answers[rep] = std::string(data, 200);
      WRAP_ERROR_CODE(masstree->insert_record(context, &key, sizeof(key), data, sizeof(data)));
      WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    }

    // now read
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    for (uint32_t rep = 0; rep < 32; ++rep) {
      uint64_t key = keys[rep];
      char data[500];
      uint16_t capacity = 500;
      WRAP_ERROR_CODE(masstree->get_record(context, &key, sizeof(key), data, &capacity));
      EXPECT_EQ(200, capacity);
      EXPECT_EQ(answers[rep], std::string(data, 200));
    }
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    CHECK_ERROR(masstree->verify_single_thread(context));
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    WRAP_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
    return foedus::kRetOk;
  }
};

TEST(MasstreeBasicTest, SplitBorder) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeStorage* out;
    Epoch commit_epoch;
    MasstreeMetadata meta("ggg");
    COERCE_ERROR(engine.get_storage_manager().create_masstree(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    SplitBorderTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}


class SplitBorderNormalizedTask : public thread::ImpersonateTask {
 public:
  ErrorStack run(thread::Thread* context) {
    MasstreeStorage *masstree =
      dynamic_cast<MasstreeStorage*>(
        context->get_engine()->get_storage_manager().get_storage("ggg"));
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    assorted::UniformRandom uniform_random(123456);
    KeySlice keys[32];
    std::string answers[32];
    Epoch commit_epoch;
    for (uint32_t rep = 0; rep < 32; ++rep) {
      WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
      KeySlice key = normalize_primitive<uint64_t>(uniform_random.next_uint64());
      keys[rep] = key;
      char data[200];
      std::memset(data, 0, 200);
      std::memcpy(data + 123, &key, sizeof(key));
      answers[rep] = std::string(data, 200);
      WRAP_ERROR_CODE(masstree->insert_record_normalized(context, key, data, sizeof(data)));
      WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    }

    // now read
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    for (uint32_t rep = 0; rep < 32; ++rep) {
      KeySlice key = keys[rep];
      char data[500];
      uint16_t capacity = 500;
      WRAP_ERROR_CODE(masstree->get_record_normalized(context, key, data, &capacity));
      EXPECT_EQ(200, capacity);
      EXPECT_EQ(answers[rep], std::string(data, 200));
    }
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    CHECK_ERROR(masstree->verify_single_thread(context));
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    WRAP_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
    return foedus::kRetOk;
  }
};

TEST(MasstreeBasicTest, SplitBorderNormalized) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeStorage* out;
    Epoch commit_epoch;
    MasstreeMetadata meta("ggg");
    COERCE_ERROR(engine.get_storage_manager().create_masstree(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    SplitBorderNormalizedTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

class SplitInNextLayerTask : public thread::ImpersonateTask {
 public:
  ErrorStack run(thread::Thread* context) {
    MasstreeStorage *masstree =
      dynamic_cast<MasstreeStorage*>(
        context->get_engine()->get_storage_manager().get_storage("ggg"));
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    assorted::UniformRandom uniform_random(123456);
    std::string keys[32];
    std::string answers[32];
    Epoch commit_epoch;
    for (uint32_t rep = 0; rep < 32; ++rep) {
      WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
      uint64_t key_int = uniform_random.next_uint64();
      char key_string[16];
      std::memset(key_string, 42, 8);
      reinterpret_cast<uint64_t*>(key_string)[1] = key_int;
      keys[rep] = std::string(key_string, 16);
      char data[200];
      std::memset(data, 0, 200);
      std::memcpy(data + 123, &key_int, sizeof(key_int));
      answers[rep] = std::string(data, 200);
      WRAP_ERROR_CODE(masstree->insert_record(context, key_string, 16, data, sizeof(data)));
      WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    }

    // now read
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    for (uint32_t rep = 0; rep < 32; ++rep) {
      char data[500];
      uint16_t capacity = 500;
      WRAP_ERROR_CODE(masstree->get_record(context, keys[rep].data(), 16, data, &capacity));
      EXPECT_EQ(200, capacity);
      EXPECT_EQ(answers[rep], std::string(data, 200));
    }
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    CHECK_ERROR(masstree->verify_single_thread(context));
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    WRAP_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
    return foedus::kRetOk;
  }
};

TEST(MasstreeBasicTest, SplitInNextLayer) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeStorage* out;
    Epoch commit_epoch;
    MasstreeMetadata meta("ggg");
    COERCE_ERROR(engine.get_storage_manager().create_masstree(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    SplitInNextLayerTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

class SplitIntermediateSequentialTask : public thread::ImpersonateTask {
 public:
  ErrorStack run(thread::Thread* context) {
    MasstreeStorage *masstree =
      dynamic_cast<MasstreeStorage*>(
        context->get_engine()->get_storage_manager().get_storage("ggg"));
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    Epoch commit_epoch;
    // 1000 bytes payload -> only 2 tuples per page.
    // one intermediate page can point to about 150 pages.
    // inserting 400 tuples surely causes 3 levels
    for (uint32_t rep = 0; rep < 400; ++rep) {
      WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
      CHECK_ERROR(masstree->verify_single_thread(context));
      KeySlice key = normalize_primitive<uint64_t>(rep);
      char data[1000];
      std::memset(data, 0, 1000);
      std::memcpy(data + 123, &key, sizeof(key));
      WRAP_ERROR_CODE(masstree->insert_record_normalized(context, key, data, sizeof(data)));
      WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    }

    // now read
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    for (uint32_t rep = 0; rep < 400; ++rep) {
      KeySlice key = normalize_primitive<uint64_t>(rep);
      char data[1000];
      uint16_t capacity = 1000;
      WRAP_ERROR_CODE(masstree->get_record_normalized(context, key, data, &capacity));
      EXPECT_EQ(1000, capacity);
      char correct_data[1000];
      std::memset(correct_data, 0, 1000);
      std::memcpy(correct_data + 123, &key, sizeof(key));
      EXPECT_EQ(std::string(correct_data, 1000), std::string(data, capacity)) << rep;
    }
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    CHECK_ERROR(masstree->verify_single_thread(context));
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    WRAP_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
    return foedus::kRetOk;
  }
};

TEST(MasstreeBasicTest, SplitIntermediateSequential) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeStorage* out;
    Epoch commit_epoch;
    MasstreeMetadata meta("ggg");
    COERCE_ERROR(engine.get_storage_manager().create_masstree(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out != nullptr);
    SplitIntermediateSequentialTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
