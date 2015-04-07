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
DEFINE_TEST_CASE_PACKAGE(MasstreeBasicTest, foedus.storage.masstree);
TEST(MasstreeBasicTest, Create) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("test");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    COERCE_ERROR(storage.debugout_single_thread(&engine));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack query_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("test2");
  char buf[16];
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  char key[100];
  std::memset(key, 0, 100);
  uint16_t payload_capacity = 16;
  ErrorCode result = masstree.get_record(context, key, 100, buf, &payload_capacity);
  EXPECT_EQ(kErrorCodeStrKeyNotFound, result);
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(MasstreeBasicTest, CreateAndQuery) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("query_task", query_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("test2");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("query_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack insert_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  KeySlice key = normalize_primitive(12345ULL);
  uint64_t data = 897565433333126ULL;
  WRAP_ERROR_CODE(masstree.insert_record_normalized(context, key, &data, sizeof(data)));
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(MasstreeBasicTest, CreateAndInsert) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("insert_task", insert_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("insert_task"));
    COERCE_ERROR(storage.debugout_single_thread(&engine));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack insert_read_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  KeySlice key =  normalize_primitive(12345ULL);
  uint64_t data = 897565433333126ULL;
  WRAP_ERROR_CODE(masstree.insert_record_normalized(context, key, &data, sizeof(data)));
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  uint64_t data2;
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  uint16_t data_capacity = sizeof(data2);
  WRAP_ERROR_CODE(masstree.get_record_normalized(context, key, &data2, &data_capacity));
  EXPECT_EQ(data, data2);
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));


  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(MasstreeBasicTest, CreateAndInsertAndRead) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("insert_read_task", insert_read_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("insert_read_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack overwrite_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  KeySlice key = normalize_primitive(12345ULL);
  uint64_t data = 897565433333126ULL;
  WRAP_ERROR_CODE(masstree.insert_record_normalized(context, key, &data, sizeof(data)));
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  uint64_t data2 = 321654987ULL;
  WRAP_ERROR_CODE(masstree.overwrite_record_normalized(context, key, &data2, 0, sizeof(data2)));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  uint64_t data3;
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  WRAP_ERROR_CODE(masstree.get_record_primitive_normalized<uint64_t>(context, key, &data3, 0));
  EXPECT_EQ(data2, data3);
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(MasstreeBasicTest, Overwrite) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("overwrite_task", overwrite_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("overwrite_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack next_layer_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  char key1[16];
  for (int i = 0; i < 16; ++i) {
    key1[i] = i;
  }
  uint64_t data1 = 897565433333126ULL;
  WRAP_ERROR_CODE(masstree.insert_record(context, key1, 16, &data1, sizeof(data1)));
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  // differs only in second slice
  char key2[16];
  for (int i = 0; i < 16; ++i) {
    key2[i] = i;
  }
  key2[10] = 40;
  uint64_t data2 = 9234723466543ULL;
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  WRAP_ERROR_CODE(masstree.insert_record(context, key2, 16, &data2, sizeof(data2)));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  // now read both
  uint64_t data;
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  WRAP_ERROR_CODE(masstree.get_record_primitive<uint64_t>(context, key1, 16, &data, 0));
  EXPECT_EQ(data1, data);
  WRAP_ERROR_CODE(masstree.get_record_primitive<uint64_t>(context, key2, 16, &data, 0));
  EXPECT_EQ(data2, data);
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));


  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(MasstreeBasicTest, NextLayer) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("next_layer_task", next_layer_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("next_layer_task"));
    COERCE_ERROR(storage.debugout_single_thread(&engine));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(MasstreeBasicTest, CreateAndDrop) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("dd");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    MasstreeStorage storage2;
    storage2 = storage;
    EXPECT_TRUE(storage2.exists());
    COERCE_ERROR(engine.get_storage_manager()->drop_storage(storage.get_id(), &epoch));
    EXPECT_FALSE(storage.exists());
    EXPECT_FALSE(storage2.exists());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreeBasicTest, foedus.storage.masstree);
