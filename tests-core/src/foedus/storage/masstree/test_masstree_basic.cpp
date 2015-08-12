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
#include "foedus/storage/hash/hash_hashinate.hpp"
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

ErrorStack insert_task_long_retry(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  uint64_t remaining_inserts = 2000;
  uint32_t low = 0;
  char data[1000];
  memset(data, 'a', 1000);
  Epoch commit_epoch;
  while (remaining_inserts) {
    COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    ErrorCode ret;
    // Emulate two key partitions
    for (uint64_t high = 0; high < 2; high++) {
      uint64_t keynum = (high << 32) | (uint64_t)low++;
      keynum = (uint64_t)foedus::storage::hash::hashinate(&keynum, sizeof(keynum));
      foedus::assorted::FixedString<36> key;
      key.assign("user" + std::to_string(keynum));
      ret = masstree.insert_record(context, key.data(), key.length(), data, 1000);
      if (ret != kErrorCodeOk || !--remaining_inserts)
        break;
    }
    if (ret != kErrorCodeOk) {
      COERCE_ERROR_CODE(xct_manager->abort_xct(context));
      remaining_inserts += 2;
      continue;
    }

    ret = xct_manager->precommit_xct(context, &commit_epoch);
    if (ret != kErrorCodeOk) {  // retry
      remaining_inserts += 2;
      continue;
    }
  }
  return foedus::kRetOk;
}

// Same as insert_task_long_retry, except that this one doesn't allow failures
ErrorStack insert_task_long_coerce(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  uint64_t remaining_inserts = 2000;
  uint32_t low = 0;
  char data[1000];
  memset(data, 'a', 1000);
  Epoch commit_epoch;
  while (remaining_inserts) {
    COERCE_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    // Emulate two key partitions
    for (uint64_t high = 0; high < 2; high++) {
      uint64_t keynum = (high << 32) | (uint64_t)low++;
      keynum = (uint64_t)foedus::storage::hash::hashinate(&keynum, sizeof(keynum));
      foedus::assorted::FixedString<36> key;
      key.assign("user" + std::to_string(keynum));
      COERCE_ERROR_CODE(masstree.insert_record(context, key.data(), key.length(), data, 1000));
      if (!--remaining_inserts)
        break;
    }
    COERCE_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }
  return foedus::kRetOk;
}

// CreateAndInsertLong[Retry, Coerce]
// These two guys test inserting records with large payload (e.g., 1000 bytes)
// and more complex keys (e.g., worker-id partitioned and hashed key space).
TEST(MasstreeBasicTest, CreateAndInsertLongRetry) {
  EngineOptions options = get_tiny_options();
  options.memory_.page_pool_size_mb_per_node_ = 128;
  options.memory_.page_pool_size_mb_per_node_ *= 2U;  // for rigorous_check
  Engine engine(options);
  engine.get_proc_manager()->pre_register("insert_task_long_retry", insert_task_long_retry);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("insert_task_long_retry"));
    COERCE_ERROR(storage.debugout_single_thread(&engine));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(MasstreeBasicTest, CreateAndInsertLongCoerce) {
  EngineOptions options = get_tiny_options();
  options.memory_.page_pool_size_mb_per_node_ = 128;
  options.memory_.page_pool_size_mb_per_node_ *= 2U;  // for rigorous_check
  Engine engine(options);
  engine.get_proc_manager()->pre_register("insert_task_long_coerce", insert_task_long_coerce);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("insert_task_long_coerce"));
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


struct ExpandTaskInput {
  bool update_case_;
  bool normalized_case_;
  bool next_layer_case_;
};

ErrorStack expand_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  const ExpandTaskInput* inputs = reinterpret_cast<const ExpandTaskInput*>(args.input_buffer_);
  ASSERT_ND(args.input_len_ == sizeof(ExpandTaskInput));
  MasstreeStorage storage(args.engine_, "ggg");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  Epoch commit_epoch;

  // Use two keys and expand in-turn so that we quickly use up the space in the page.
  // If we have only one-key, the new page layout allows just expanding the only record,
  // thus we don't cause any page-split (which is good, but we want to test tricky cases).
  const KeySlice kKeyNormalized[2] = {
    normalize_primitive<uint64_t>(12345ULL),
    normalize_primitive<uint64_t>(12346ULL),
  };
  std::string kKey[2];
  if (inputs->next_layer_case_) {
    // Differ only in next layer. We have a single record in first layer
    kKey[0] = std::string("key1234567890");
    kKey[1] = std::string("key1234567891");
  } else {
    // Uses only one layer
    kKey[0] = std::string("key1234567890");
    kKey[1] = std::string("key1235abcdef");
  }
  EXPECT_EQ(kKey[0].size(), kKey[1].size());
  KeyLength kKeyLen = kKey[0].size();
  char data[512];
  for (PayloadLength c = 0; c < sizeof(data); ++c) {
    data[c] = static_cast<char>(c);
  }

  const PayloadLength kInitialLen = 6;
  const PayloadLength kExpandLen = 5;
  const uint16_t kRep = 80;
  ASSERT_ND(kInitialLen + kExpandLen * kRep <= sizeof(data));

  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  for (int i = 0; i < 2; ++i) {
    if (inputs->normalized_case_) {
      CHECK_ERROR(storage.insert_record_normalized(context, kKeyNormalized[i], data, kInitialLen));
    } else {
      CHECK_ERROR(storage.insert_record(context, kKey[i].data(), kKeyLen, data, kInitialLen));
    }
  }
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

  CHECK_ERROR(storage.verify_single_thread(context));

  // expand the record many times. this will create a few pages.
  PayloadLength len = kInitialLen;
  for (uint16_t rep = 0; rep < kRep; ++rep) {
    len += kExpandLen;
    for (int i = 0; i < 2; ++i) {
      KeySlice norm_key = kKeyNormalized[i];
      const char* key = kKey[i].data();
      if (!inputs->update_case_) {
        // in this case we move a deleted record, using insert
        CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
        if (inputs->normalized_case_) {
          CHECK_ERROR(storage.delete_record_normalized(context, norm_key));
        } else {
          CHECK_ERROR(storage.delete_record(context, key, kKeyLen));
        }
        CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
        CHECK_ERROR(storage.verify_single_thread(context));

        CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
        if (inputs->normalized_case_) {
          CHECK_ERROR(storage.insert_record_normalized(context, norm_key, data, len));
        } else {
          CHECK_ERROR(storage.insert_record(context, key, kKeyLen, data, len));
        }
        CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
      } else {
        // in this case we move an active record, using upsert
        CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
        if (inputs->normalized_case_) {
          CHECK_ERROR(storage.upsert_record_normalized(context, norm_key, data, len));
        } else {
          CHECK_ERROR(storage.upsert_record(context, key, kKeyLen, data, len));
        }
        CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
      }

      CHECK_ERROR(storage.verify_single_thread(context));
    }
  }

  // verify that the record exists
  CHECK_ERROR(storage.verify_single_thread(context));

  for (int i = 0; i < 2; ++i) {
    CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
    char retrieved[sizeof(data)];
    std::memset(retrieved, 42, sizeof(retrieved));
    PayloadLength retrieved_capacity = sizeof(retrieved);
    if (inputs->normalized_case_) {
      CHECK_ERROR(storage.get_record_normalized(
        context,
        kKeyNormalized[i],
        retrieved,
        &retrieved_capacity));
    } else {
      CHECK_ERROR(storage.get_record(
        context,
        kKey[i].data(),
        kKeyLen,
        retrieved,
        &retrieved_capacity));
    }
    CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));

    EXPECT_EQ(kInitialLen + kRep * kExpandLen, retrieved_capacity);
    for (PayloadLength c = 0; c < retrieved_capacity; ++c) {
      EXPECT_EQ(static_cast<char>(c), retrieved[c]) << c;
    }
    for (PayloadLength c = retrieved_capacity; c < sizeof(retrieved); ++c) {
      EXPECT_EQ(42, retrieved[c]) << c;
    }
  }

  CHECK_ERROR(storage.verify_single_thread(context));

  // CHECK_ERROR(storage.debugout_single_thread(context->get_engine()));

  return foedus::kRetOk;
}

void test_expand(bool update_case, bool normalized, bool next_layer) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  engine.get_proc_manager()->pre_register("expand_task", expand_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("ggg");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    ExpandTaskInput inputs = { update_case, normalized, next_layer };
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous(
      "expand_task",
      &inputs,
      sizeof(inputs)));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(MasstreeBasicTest, ExpandInsert) { test_expand(false, false, false); }
TEST(MasstreeBasicTest, ExpandInsertNextLayer) { test_expand(false, false, true); }
TEST(MasstreeBasicTest, ExpandInsertNormalized) { test_expand(false, true, false); }
TEST(MasstreeBasicTest, ExpandUpdate) { test_expand(true, false, false); }
TEST(MasstreeBasicTest, ExpandUpdateNextLayer) { test_expand(true, false, true); }
TEST(MasstreeBasicTest, ExpandUpdateNormalized) { test_expand(true, true, false); }
// TASK(Hideaki): we don't have multi-thread cases here. it's not a "basic" test.
// no multi-key cases either.

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreeBasicTest, foedus.storage.masstree);
