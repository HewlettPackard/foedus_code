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
#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/proc/proc_manager.hpp"
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

ErrorStack split_border_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  assorted::UniformRandom uniform_random(123456);
  uint64_t keys[32];
  std::string answers[32];
  Epoch commit_epoch;
  for (uint32_t rep = 0; rep < 32; ++rep) {
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    uint64_t key = uniform_random.next_uint64();
    keys[rep] = key;
    char data[200];
    std::memset(data, 0, 200);
    std::memcpy(data + 123, &key, sizeof(key));
    answers[rep] = std::string(data, 200);
    WRAP_ERROR_CODE(masstree.insert_record(context, &key, sizeof(key), data, sizeof(data)));
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    CHECK_ERROR(masstree.verify_single_thread(context));
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }

  // now read
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  for (uint32_t rep = 0; rep < 32; ++rep) {
    uint64_t key = keys[rep];
    char data[500];
    uint16_t capacity = 500;
    WRAP_ERROR_CODE(masstree.get_record(context, &key, sizeof(key), data, &capacity, true));
    EXPECT_EQ(200, capacity);
    EXPECT_EQ(answers[rep], std::string(data, 200));
  }
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(MasstreeSplitTest, SplitBorder) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("split_border_task", split_border_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("split_border_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}


ErrorStack split_border_normalized_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  assorted::UniformRandom uniform_random(123456);
  KeySlice keys[32];
  std::string answers[32];
  Epoch commit_epoch;
  for (uint32_t rep = 0; rep < 32; ++rep) {
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    KeySlice key = normalize_primitive<uint64_t>(uniform_random.next_uint64());
    keys[rep] = key;
    char data[200];
    std::memset(data, 0, 200);
    std::memcpy(data + 123, &key, sizeof(key));
    answers[rep] = std::string(data, 200);
    WRAP_ERROR_CODE(masstree.insert_record_normalized(context, key, data, sizeof(data)));
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }

  // now read
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  for (uint32_t rep = 0; rep < 32; ++rep) {
    KeySlice key = keys[rep];
    char data[500];
    uint16_t capacity = 500;
    WRAP_ERROR_CODE(masstree.get_record_normalized(context, key, data, &capacity, true));
    EXPECT_EQ(200, capacity);
    EXPECT_EQ(answers[rep], std::string(data, 200));
  }
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(MasstreeSplitTest, SplitBorderNormalized) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("the_task", split_border_normalized_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("the_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack split_in_next_layer_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  assorted::UniformRandom uniform_random(123456);
  std::string keys[32];
  std::string answers[32];
  Epoch commit_epoch;
  for (uint32_t rep = 0; rep < 32; ++rep) {
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    uint64_t key_int = uniform_random.next_uint64();
    char key_string[16];
    std::memset(key_string, 42, 8);
    reinterpret_cast<uint64_t*>(key_string)[1] = key_int;
    keys[rep] = std::string(key_string, 16);
    char data[200];
    std::memset(data, 0, 200);
    std::memcpy(data + 123, &key_int, sizeof(key_int));
    answers[rep] = std::string(data, 200);
    WRAP_ERROR_CODE(masstree.insert_record(context, key_string, 16, data, sizeof(data)));
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }

  // now read
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  for (uint32_t rep = 0; rep < 32; ++rep) {
    char data[500];
    uint16_t capacity = 500;
    WRAP_ERROR_CODE(masstree.get_record(context, keys[rep].data(), 16, data, &capacity, true));
    EXPECT_EQ(200, capacity);
    EXPECT_EQ(answers[rep], std::string(data, 200));
  }
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

void test_split_in_next_layer(bool with_hint) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("split_in_next_layer_task", split_in_next_layer_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    if (with_hint) {
      meta.min_layer_hint_ = 1U;
    }
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("split_in_next_layer_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(MasstreeSplitTest, SplitInNextLayer)         { test_split_in_next_layer(false); }
TEST(MasstreeSplitTest, SplitInNextLayerWithHint) { test_split_in_next_layer(true); }

ErrorStack split_intermediate_sequential_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  Epoch commit_epoch;
  // 1000 bytes payload -> only 2 tuples per page.
  // one intermediate page can point to about 150 pages.
  // inserting 400 tuples surely causes 3 levels
  for (uint32_t rep = 0; rep < 400; ++rep) {
    if (rep == 321) {
      rep = 321;
    }
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    CHECK_ERROR(masstree.verify_single_thread(context));
    KeySlice key = normalize_primitive<uint64_t>(rep);
    char data[1000];
    std::memset(data, 0, 1000);
    std::memcpy(data + 123, &key, sizeof(key));
    WRAP_ERROR_CODE(masstree.insert_record_normalized(context, key, data, sizeof(data)));
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    CHECK_ERROR(masstree.verify_single_thread(context));
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }

  // now read
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  for (uint32_t rep = 0; rep < 400; ++rep) {
    KeySlice key = normalize_primitive<uint64_t>(rep);
    char data[1000];
    uint16_t capacity = 1000;
    WRAP_ERROR_CODE(masstree.get_record_normalized(context, key, data, &capacity, true));
    EXPECT_EQ(1000, capacity);
    char correct_data[1000];
    std::memset(correct_data, 0, 1000);
    std::memcpy(correct_data + 123, &key, sizeof(key));
    EXPECT_EQ(std::string(correct_data, 1000), std::string(data, capacity)) << rep;
  }
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

void test_split_intermediate_sequential(bool with_hint) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("the_task", split_intermediate_sequential_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    if (with_hint) {
      meta.min_layer_hint_ = 1U;
    }
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("the_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}
TEST(MasstreeSplitTest, SplitIntermediateSequential) { test_split_intermediate_sequential(false); }
TEST(MasstreeSplitTest, SplitIntermediateSequentialWithHint) {
  test_split_intermediate_sequential(true);
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreeSplitTest, foedus.storage.masstree);
