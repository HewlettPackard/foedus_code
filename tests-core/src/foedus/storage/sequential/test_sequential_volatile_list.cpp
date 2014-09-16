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
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"

/**
 * @file test_sequential_volatile_list.cpp
 * This specifically tests SequentialVolatileList class, which is a part of sequential storage.
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
    storage::StorageId storage_id = 1;
    SequentialVolatileList target(&engine, storage_id);
    COERCE_ERROR(target.initialize());
    COERCE_ERROR(target.uninitialize());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

const uint32_t kAppendsPerThread = 2000;
const uint32_t kStartEpoch = 3;
const uint32_t kAppendsPerEpoch = 100;

class AppendTask : public thread::ImpersonateTask {
 public:
  explicit AppendTask(
    int task_id,
    thread::Rendezvous* start_rendezvous,
    SequentialVolatileList *target)
    : task_id_(task_id), start_rendezvous_(start_rendezvous), target_(target) {
  }
  ErrorStack run(thread::Thread* context) {
    start_rendezvous_->wait();
    for (uint32_t i = 0; i < kAppendsPerThread; ++i) {
      xct::XctId owner_id;
      uint32_t epoch = kStartEpoch + (i / kAppendsPerEpoch);
      owner_id.set(epoch, i);
      std::string value("value_");
      value += std::to_string(task_id_);
      value += "_";
      value += std::to_string(i);
      const char* payload = value.data();
      uint16_t payload_length = value.size();
      target_->append_record(context, owner_id, payload, payload_length);
    }

    return foedus::kRetOk;
  }

 private:
  const int task_id_;
  thread::Rendezvous* const start_rendezvous_;
  SequentialVolatileList* const target_;
};

void verify_result(
  uint16_t thread_count,
  const SequentialVolatileList &target) {
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

  target.for_every_page(
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
}

void execute_test(uint16_t thread_count) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = thread_count;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    storage::StorageId storage_id = 1;
    SequentialVolatileList target(&engine, storage_id);
    COERCE_ERROR(target.initialize());
    std::cout << "target before:" << target << std::endl;
    {
      thread::Rendezvous start_rendezvous;
      std::vector<AppendTask*> tasks;
      std::vector<thread::ImpersonateSession> sessions;
      for (int i = 0; i < thread_count; ++i) {
        tasks.push_back(new AppendTask(i, &start_rendezvous, &target));
        sessions.emplace_back(engine.get_thread_pool().impersonate(tasks[i]));
        if (!sessions[i].is_valid()) {
          COERCE_ERROR(sessions[i].invalid_cause_);
        }
      }
      start_rendezvous.signal();
      for (int i = 0; i < thread_count; ++i) {
        COERCE_ERROR(sessions[i].get_result());
        delete tasks[i];
      }
    }
    std::cout << "target after:" << target << std::endl;
    verify_result(thread_count, target);
    COERCE_ERROR(target.uninitialize());
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
