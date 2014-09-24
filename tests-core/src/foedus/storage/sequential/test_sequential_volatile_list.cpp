/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"

/**
 * @file test_sequential_volatile_list.cpp
 * This specifically tests the volatile part of sequential storage.
 */

namespace foedus {
namespace storage {
namespace sequential {

DEFINE_TEST_CASE_PACKAGE(SequentialVolatileListTest, foedus.storage.sequential);
TEST(SequentialVolatileListTest, Empty) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    SequentialMetadata meta("test_seq");
    SequentialStorage storage;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_sequential(&meta, &storage, &epoch));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

const uint32_t kAppendsPerThread = 2000;
const uint32_t kStartEpoch = 3;
const uint32_t kAppendsPerEpoch = 100;

ErrorStack append_task(
  thread::Thread* context,
  const void* input_buffer,
  uint32_t input_len,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  EXPECT_EQ(sizeof(uint32_t), input_len);
  uint32_t task_id = *reinterpret_cast<const uint32_t*>(input_buffer);
  SequentialStorage target = context->get_engine()->get_storage_manager()->get_sequential("seq");
  EXPECT_TRUE(target.exists());
  SequentialStoragePimpl pimpl(&target);
  for (uint32_t i = 0; i < kAppendsPerThread; ++i) {
    xct::XctId owner_id;
    uint32_t epoch = kStartEpoch + (i / kAppendsPerEpoch);
    owner_id.set(epoch, i);
    std::string value("value_");
    value += std::to_string(task_id);
    value += "_";
    value += std::to_string(i);
    const char* payload = value.data();
    uint16_t payload_length = value.size();
    pimpl.append_record(context, owner_id, payload, payload_length);
  }

  return foedus::kRetOk;
}

ErrorStack verify_result(
  thread::Thread* context,
  const void* input_buffer,
  uint32_t input_len,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  EXPECT_EQ(sizeof(uint16_t), input_len);
  uint16_t thread_count = *reinterpret_cast<const uint16_t*>(input_buffer);
  SequentialStorage target = context->get_engine()->get_storage_manager()->get_sequential("seq");
  EXPECT_TRUE(target.exists());
  std::map<std::string, xct::XctId> answers;
  for (int task_id = 0; task_id < thread_count; ++task_id) {
    for (uint32_t i = 0; i < kAppendsPerThread; ++i) {
      xct::XctId owner_id;
      uint32_t epoch = kStartEpoch + (i / kAppendsPerEpoch);
      owner_id.set(epoch, i);
      std::string value("value_");
      value += std::to_string(task_id);
      value += "_";
      value += std::to_string(i);
      answers.insert(std::pair<std::string, xct::XctId>(value, owner_id));
    }
  }

  SequentialStoragePimpl pimpl(&target);
  pimpl.for_every_page(
    [&](SequentialPage* page){
      uint16_t record_count = page->get_record_count();
      const char* record_pointers[kMaxSlots];
      uint16_t payload_lengthes[kMaxSlots];
      page->get_all_records_nosync(&record_count, record_pointers, payload_lengthes);

      for (uint16_t rec = 0; rec < record_count; ++rec) {
        const xct::LockableXctId* owner_id = reinterpret_cast<const xct::LockableXctId*>(
          record_pointers[rec]);
        ASSERT_ND(!owner_id->lock_.is_keylocked());
        ASSERT_ND(owner_id->lock_.get_version() == 0);
        uint16_t payload_length = payload_lengthes[rec];
        EXPECT_GT(payload_length, 0);
        EXPECT_LE(payload_length, kMaxPayload);
        std::string value(record_pointers[rec] + kRecordOverhead, payload_length);
        EXPECT_TRUE(answers.find(value) != answers.end()) << "found value=" << value;
        if (answers.find(value) != answers.end()) {
          xct::XctId correct_owner_id = answers.find(value)->second;
          EXPECT_EQ(correct_owner_id, owner_id->xct_id_) << "found value=" << value;
          answers.erase(value);
        }
      }
      return kErrorCodeOk;
  });

  EXPECT_EQ(0, answers.size());
  std::map<std::string, xct::XctId> empty_map;
  EXPECT_EQ(empty_map, answers);
  return kRetOk;
}

void execute_test(uint16_t thread_count) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = thread_count;
  Engine engine(options);
  engine.get_proc_manager()->pre_register(proc::ProcAndName("append_task", append_task));
  engine.get_proc_manager()->pre_register(proc::ProcAndName("verify_result", verify_result));
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    SequentialMetadata meta("seq");
    SequentialStorage target;
    Epoch epoch;
    COERCE_ERROR(engine.get_storage_manager()->create_sequential(&meta, &target, &epoch));
    std::cout << "target before:" << target << std::endl;
    {
      std::vector<thread::ImpersonateSession> sessions;
      for (uint32_t task_id = 0; task_id < thread_count; ++task_id) {
        thread::ImpersonateSession session;
        EXPECT_TRUE(engine.get_thread_pool()->impersonate(
          "append_task",
          &task_id,
          sizeof(task_id),
          &session));
        sessions.emplace_back(std::move(session));
      }
      for (int i = 0; i < thread_count; ++i) {
        COERCE_ERROR(sessions[i].get_result());
      }
    }
    std::cout << "target after:" << target << std::endl;
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous(
      "verify_result",
      &thread_count,
      sizeof(thread_count)));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(SequentialVolatileListTest, SingleThread) { execute_test(1); }
TEST(SequentialVolatileListTest, TwoThreads) { execute_test(2); }
TEST(SequentialVolatileListTest, FourThreads) { execute_test(4); }

}  // namespace sequential
}  // namespace storage
}  // namespace foedus
