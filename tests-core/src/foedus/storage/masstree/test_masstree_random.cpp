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
#include <set>
#include <string>
#include <vector>

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
DEFINE_TEST_CASE_PACKAGE(MasstreeRandomTest, foedus.storage.masstree);

ErrorStack insert_many_normalized_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("test2");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();

  // insert a lot
  const uint32_t kCount = 10000U;
  const uint16_t kBufSize = 200;
  char buf[kBufSize];
  std::memset(buf, 0, kBufSize);
  Epoch commit_epoch;
  std::set<KeySlice> inserted;
  {
    assorted::UniformRandom uniform_random(123456L);
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    for (uint32_t i = 0; i < kCount; ++i) {
      KeySlice key = normalize_primitive<uint64_t>(uniform_random.next_uint64());
      if (inserted.find(key) != inserted.end()) {
        std::cout << "already inserted" << key << std::endl;
        continue;
      }
      inserted.insert(key);
      *reinterpret_cast<uint64_t*>(buf + 123) = key;
      WRAP_ERROR_CODE(masstree.insert_record_normalized(context, key, buf, kBufSize));
      if (i % 50 == 0) {
        WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
        WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
        std::cout << "inserting:" << i << "/" << kCount << std::endl;
      }
      if (i % 1000 == 0) {
        CHECK_ERROR(masstree.verify_single_thread(context));
      }
    }
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  // read it back
  {
    char buf2[kBufSize];
    assorted::UniformRandom uniform_random(123456L);
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    for (uint32_t i = 0; i < kCount; ++i) {
      KeySlice key = normalize_primitive<uint64_t>(uniform_random.next_uint64());
      *reinterpret_cast<uint64_t*>(buf + 123) = key;
      uint16_t capacity = kBufSize;
      WRAP_ERROR_CODE(masstree.get_record_normalized(context, key, buf2, &capacity));
      EXPECT_EQ(kBufSize, capacity);
      EXPECT_EQ(std::string(buf, kBufSize), std::string(buf2, kBufSize));
      if (i % 20 == 0) {
        WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
        WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
        std::cout << "reading:" << i << "/" << kCount << std::endl;
      }
    }
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(MasstreeRandomTest, InsertManyNormalized) {
  EngineOptions options = get_tiny_options();
  options.memory_.page_pool_size_mb_per_node_ = 64;
  options.memory_.page_pool_size_mb_per_node_ *= 2U;  // for rigorous_check
  Engine engine(options);
  engine.get_proc_manager()->pre_register(
    "insert_many_normalized_task",
    insert_many_normalized_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("test2");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("insert_many_normalized_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack insert_many_normalized_mt_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  EXPECT_EQ(sizeof(uint32_t), args.input_len_);
  uint32_t id = *reinterpret_cast<const uint32_t*>(args.input_buffer_);
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("test2");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();

  // insert a lot
  const uint32_t kCount = 1000U;
  const uint16_t kBufSize = 20;
  char buf[kBufSize];
  std::memset(buf, 0, kBufSize);
  Epoch commit_epoch;
  std::set<KeySlice> inserted;
  {
    assorted::UniformRandom uniform_random(123456L);
    for (uint32_t i = 0; i < kCount; ++i) {
      KeySlice key = normalize_primitive<uint64_t>(uniform_random.next_uint64()) * 8 + id;
      if (inserted.find(key) != inserted.end()) {
        std::cout << "already inserted" << key << std::endl;
        continue;
      }
      inserted.insert(key);
      *reinterpret_cast<uint64_t*>(buf + 3) = key;
      while (true) {
        WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
        WRAP_ERROR_CODE(masstree.insert_record_normalized(context, key, buf, kBufSize));
        ErrorCode code = xct_manager->precommit_xct(context, &commit_epoch);
        if (code == kErrorCodeOk) {
          break;
        } else {
          EXPECT_EQ(kErrorCodeXctRaceAbort, code) << id << ":" << i;
          if (code != kErrorCodeXctRaceAbort) {
            break;
          }
        }
      }
    }
  }

  // read it back
  {
    char buf2[kBufSize];
    assorted::UniformRandom uniform_random(123456L);
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kDirtyRead));
    for (uint32_t i = 0; i < kCount; ++i) {
      KeySlice key = normalize_primitive<uint64_t>(uniform_random.next_uint64()) * 8 + id;
      *reinterpret_cast<uint64_t*>(buf + 3) = key;
      uint16_t capacity = kBufSize;
      WRAP_ERROR_CODE(masstree.get_record_normalized(context, key, buf2, &capacity));
      EXPECT_EQ(kBufSize, capacity);
      EXPECT_EQ(std::string(buf, kBufSize), std::string(buf2, kBufSize));
    }
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }

  return foedus::kRetOk;
}

ErrorStack insert_many_normalized_verify_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("test2");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  Epoch commit_epoch;
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  return kRetOk;
}

TEST(MasstreeRandomTest, InsertManyNormalizedMt) {
  EngineOptions options = get_tiny_options();
  options.memory_.page_pool_size_mb_per_node_ = 64;
  options.memory_.page_pool_size_mb_per_node_ *= 2U;  // for rigorous_check
  const uint32_t kThreads = 4;
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("mt_task", insert_many_normalized_mt_task);
  engine.get_proc_manager()->pre_register("verify_task", insert_many_normalized_verify_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeMetadata meta("test2");
    MasstreeStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &storage, &epoch));
    EXPECT_TRUE(storage.exists());
    std::vector<thread::ImpersonateSession> sessions;
    for (uint32_t i = 0; i < kThreads; ++i) {
      thread::ImpersonateSession session;
      EXPECT_TRUE(engine.get_thread_pool()->impersonate(
        "mt_task",
        &i,
        sizeof(i),
        &session));
      sessions.emplace_back(std::move(session));
    }
    for (uint32_t i = 0; i < kThreads; ++i) {
      COERCE_ERROR(sessions[i].get_result());
    }
    for (uint32_t i = 0; i < kThreads; ++i) {
      sessions[i].release();
    }
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

ErrorStack insert_many_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  MasstreeStorage masstree = context->get_engine()->get_storage_manager()->get_masstree("test2");
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();

  // insert a lot of variable-length keys
  const uint32_t kCount = 10000U;
  const uint16_t kBufSize = 200;
  char buf[kBufSize];
  const uint16_t kMaxLen = 32;
  char key_buf[kMaxLen];
  std::memset(buf, 0, kBufSize);
  Epoch commit_epoch;
  std::set<std::string> inserted;
  std::set<uint32_t> skipped_i;
  std::string answers[kCount];
  {
    assorted::UniformRandom uniform_random(123456L);
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    for (uint32_t i = 0; i < kCount; ++i) {
      uint8_t len = 1 + uniform_random.next_uint32() % 24;
      for (uint32_t j = 0; j < len; ++j) {
        key_buf[j] = static_cast<char>(uniform_random.next_uint32());
      }
      std::string key(key_buf, len);
      if (inserted.find(key) != inserted.end()) {
        std::cout << "already inserted" << key << std::endl;
        skipped_i.insert(i);
        continue;
      }
      inserted.insert(key);
      answers[i] = key;
      *reinterpret_cast<uint64_t*>(buf + 123) = i;
      WRAP_ERROR_CODE(masstree.insert_record(context, key_buf, len, buf, kBufSize));
      if (i % 50 == 0) {
        WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
        WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
        std::cout << "inserting:" << i << "/" << kCount << std::endl;
      }
      if (i % 1000 == 0) {
        CHECK_ERROR(masstree.verify_single_thread(context));
      }
    }
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  // read it back
  {
    char buf2[kBufSize];
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
    for (uint32_t i = 0; i < kCount; ++i) {
      if (skipped_i.find(i) != skipped_i.end()) {
        continue;
      }
      *reinterpret_cast<uint64_t*>(buf + 123) = i;
      uint16_t capacity = kBufSize;
      std::string key = answers[i];
      ErrorCode ret = masstree.get_record(context, key.data(), key.size(), buf2, &capacity);
      EXPECT_EQ(kErrorCodeOk, ret) << i;
      if (kErrorCodeOk == ret) {
        EXPECT_EQ(kBufSize, capacity);
        EXPECT_EQ(std::string(buf, kBufSize), std::string(buf2, kBufSize));
      }
      if (i % 20 == 0) {
        WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
        WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
        std::cout << "reading:" << i << "/" << kCount << std::endl;
      }
    }
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
  }

  WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));
  CHECK_ERROR(masstree.verify_single_thread(context));
  WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

  WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
  return foedus::kRetOk;
}

TEST(MasstreeRandomTest, InsertMany) {
  EngineOptions options = get_tiny_options();
  options.memory_.page_pool_size_mb_per_node_ = 64;
  options.memory_.page_pool_size_mb_per_node_ *= 2U;  // for rigorous_check
  Engine engine(options);
  engine.get_proc_manager()->pre_register("insert_many_task", insert_many_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    MasstreeStorage out;
    Epoch commit_epoch;
    MasstreeMetadata meta("test2");
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &out, &commit_epoch));
    ASSERT_ND(out.exists());
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("insert_many_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(MasstreeRandomTest, foedus.storage.masstree);
