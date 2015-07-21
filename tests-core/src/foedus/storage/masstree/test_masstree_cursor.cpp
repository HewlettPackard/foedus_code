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

ErrorStack empty_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("test2");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  char key[100];
  std::memset(key, 0, 100);
  char key2[100];
  std::memset(key2, 0xFFU, 100);
  MasstreeCursor cursor(masstree, context);
  WRAP_ERROR_CODE(cursor.open(key, 100, key2, 100));
  EXPECT_FALSE(cursor.is_valid_record());
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(MasstreeCursorTest, Empty) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("empty_task", empty_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("test2");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("empty_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack one_page_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("test2");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  Epoch commit_epoch;

  const uint16_t kCount = 10;
  std::map<std::string, std::string> answers;
  assorted::UniformRandom uniform_random(1234);
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
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
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
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
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}


TEST(MasstreeCursorTest, OnePage) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("one_page_task", one_page_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("test2");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("one_page_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(MasstreeCursorTest, OneLayer) {
}
TEST(MasstreeCursorTest, TwoLayers) {
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreeCursorTest, foedus.storage.masstree);
