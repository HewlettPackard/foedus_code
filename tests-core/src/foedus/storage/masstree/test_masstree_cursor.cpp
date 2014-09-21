/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <cstring>
#include <iostream>
#include <map>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace masstree {
DEFINE_TEST_CASE_PACKAGE(MasstreeCursorTest, foedus.storage.masstree);

ErrorStack empty_task(
  thread::Thread* context,
  const void* /*input_buffer*/,
  uint32_t /*input_len*/,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  MasstreeStorage masstree = context->get_engine()->get_storage_manager().get_masstree("test2");
  xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
  char key[100];
  std::memset(key, 0, 100);
  char key2[100];
  std::memset(key2, 0xFFU, 100);
  MasstreeCursor cursor(masstree, context);
  WRAP_ERROR_CODE(cursor.open(key, 100, key2, 100));
  EXPECT_FALSE(cursor.is_valid_record());
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(MasstreeBasicTest, Empty) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager().pre_register("empty_task", empty_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("test2");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager().create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    thread::ImpersonateSession session;
    EXPECT_TRUE(engine.get_thread_pool().impersonate("empty_task", nullptr, 0, &session));
    COERCE_ERROR(session.get_result());
    session.release();
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack one_page_task(
  thread::Thread* context,
  const void* /*input_buffer*/,
  uint32_t /*input_len*/,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  MasstreeStorage masstree = context->get_engine()->get_storage_manager().get_masstree("test2");
  xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
  Epoch commit_epoch;

  const uint16_t kCount = 10;
  std::map<std::string, std::string> answers;
  assorted::UniformRandom uniform_random(1234);
  WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
  for (uint16_t i = 0; i < kCount; ++i) {
    std::string key = std::to_string(uniform_random.next_uint64());
    std::string datum = std::string("data_") + key;
    answers.insert(std::map<std::string, std::string>::value_type(key, datum));
    WRAP_ERROR_CODE(masstree.insert_record(
      context,
      key.data(),
      key.size(),
      datum.data(),
      datum.size()));
  }
  WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
  MasstreeCursor cursor(masstree, context);
  WRAP_ERROR_CODE(cursor.open());
  EXPECT_TRUE(cursor.is_valid_record());
  int count = 0;
  while (cursor.is_valid_record()) {
    std::string key(cursor.get_key(), cursor.get_key_length());
    std::string datum(cursor.get_payload(), cursor.get_payload_length());
    const auto& it = answers.find(key);
    EXPECT_NE(it, answers.end()) << count;
    if (it != answers.end()) {
      EXPECT_EQ(it->second, datum);
      std::cout << key << ":" << datum << std::endl;
      answers.erase(key);
    }
    ++count;
    WRAP_ERROR_CODE(cursor.next());
  }
  EXPECT_EQ(kCount, count);
  EXPECT_EQ(0U, answers.size());
  WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}


TEST(MasstreeBasicTest, OnePage) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager().pre_register("one_page_task", one_page_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("test2");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager().create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    thread::ImpersonateSession session;
    EXPECT_TRUE(engine.get_thread_pool().impersonate("one_page_task", nullptr, 0, &session));
    COERCE_ERROR(session.get_result());
    session.release();
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(MasstreeBasicTest, OneLayer) {
}
TEST(MasstreeBasicTest, TwoLayers) {
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
