/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <stdint.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_rendezvous.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_access.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace xct {
DEFINE_TEST_CASE_PACKAGE(XctCommitConflictTest, foedus.xct);


struct Payload {
  uint64_t id_;
  uint64_t data_;
};
const int kRecords = 10;
const int kThreads = 10;
static_assert(kRecords >= kThreads, "booo!");

ErrorStack init_task(
  thread::Thread* context,
  const void* /*input_buffer*/,
  uint32_t /*input_len*/,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  storage::StorageManager* str_manager = context->get_engine()->get_storage_manager();
  Epoch commit_epoch;
  storage::array::ArrayStorage storage;
  storage::array::ArrayMetadata meta("test", sizeof(Payload), kRecords);
  CHECK_ERROR(str_manager->create_array(&meta, &storage, &commit_epoch));

  CHECK_ERROR(xct_manager->begin_xct(context, kSerializable));

  for (int i = 0; i < kRecords; ++i) {
    Payload payload;
    payload.id_ = i;
    payload.data_ = 0;
    CHECK_ERROR(storage.overwrite_record(context, i, &payload));
  }

  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  return kRetOk;
}

ErrorStack try_transaction(
  thread::Thread* context,
  storage::array::ArrayStorage* storage,
  uint64_t offset,
  uint64_t amount) {
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, kSerializable));
  Payload payload;
  CHECK_ERROR(storage->get_record(context, offset, &payload));
  EXPECT_EQ(offset, payload.id_);
  payload.data_ += amount;
  CHECK_ERROR(storage->overwrite_record(context, offset, &payload));

  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  return kRetOk;
}

ErrorStack test_task(
  thread::Thread* context,
  const void* input_buffer,
  uint32_t input_len,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  EXPECT_EQ(input_len, sizeof(uint64_t) * 2U);
  uint64_t offset = reinterpret_cast<const uint64_t*>(input_buffer)[0];
  uint64_t amount = reinterpret_cast<const uint64_t*>(input_buffer)[1];
  void* user_memory
    = context->get_engine()->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory();
  soc::SharedRendezvous* rendezvous = reinterpret_cast<soc::SharedRendezvous*>(user_memory);
  rendezvous->wait();
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  storage::StorageManager* str_manager = context->get_engine()->get_storage_manager();
  storage::array::ArrayStorage storage = str_manager->get_array("test");
  while (true) {
    ErrorStack error_stack = try_transaction(context, &storage, offset, amount);
    if (!error_stack.is_error()) {
      break;
    } else if (error_stack.get_error_code() == kErrorCodeXctRaceAbort) {
      // abort and retry
      if (context->is_running_xct()) {
        CHECK_ERROR(xct_manager->abort_xct(context));
      }
    } else {
      COERCE_ERROR(error_stack);
    }
  }
  return kRetOk;
}

ErrorStack get_all_records_task(
  thread::Thread* context,
  const void* /*input_buffer*/,
  uint32_t /*input_len*/,
  void* output_buffer,
  uint32_t output_buffer_size,
  uint32_t* output_used) {
  ASSERT_ND(output_buffer_size >= sizeof(Payload) * kRecords);
  *output_used = sizeof(Payload) * kRecords;
  Payload* output = reinterpret_cast<Payload*>(output_buffer);
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, kSerializable));

  storage::StorageManager* str_manager = context->get_engine()->get_storage_manager();
  storage::array::ArrayStorage storage = str_manager->get_array("test");
  for (int i = 0; i < kRecords; ++i) {
    CHECK_ERROR(storage.get_record(context, i, output + i));
  }

  Epoch commit_epoch;
  CHECK_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  return kRetOk;
}

template <typename ASSIGN_FUNC>
void run_test(Engine *engine, ASSIGN_FUNC assign_func) {
  COERCE_ERROR(engine->get_thread_pool()->impersonate_synchronous("init_task"));
  {
    soc::SharedRendezvous* start_rendezvous
      = reinterpret_cast<soc::SharedRendezvous*>(
        engine->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
    start_rendezvous->initialize();
    std::vector<thread::ImpersonateSession> sessions;
    for (int i = 0; i < kThreads; ++i) {
      thread::ImpersonateSession session;
      uint64_t inputs[2];
      inputs[0] = assign_func(i);
      inputs[1] = i * 20 + 4;
      EXPECT_TRUE(engine->get_thread_pool()->impersonate(
        "test_task",
        inputs,
        sizeof(inputs),
        &session));
      sessions.emplace_back(std::move(session));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    start_rendezvous->signal();
    for (int i = 0; i < kThreads; ++i) {
      COERCE_ERROR(sessions[i].get_result());
    }
    start_rendezvous->uninitialize();
  }

  uint64_t answers[kThreads];
  std::memset(answers, 0, sizeof(answers));
  for (int i = 0; i < kThreads; ++i) {
    answers[assign_func(i)] += i * 20 + 4;
  }
  Payload payloads[kRecords];
  {
    thread::ImpersonateSession session;
    EXPECT_TRUE(engine->get_thread_pool()->impersonate(
      "get_all_records_task",
      nullptr,
      0,
      &session));
    COERCE_ERROR(session.get_result());
    EXPECT_EQ(sizeof(payloads), session.get_output_size());
    session.get_output(payloads);
    session.release();
  }
  for (int i = 0; i < kThreads; ++i) {
    EXPECT_EQ(i, payloads[i].id_);
    EXPECT_EQ(answers[i], payloads[i].data_);
  }
}

template <typename ASSIGN_FUNC>
void test_main(ASSIGN_FUNC assign_func) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("init_task", init_task);
  engine.get_proc_manager()->pre_register("test_task", test_task);
  engine.get_proc_manager()->pre_register("get_all_records_task", get_all_records_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    run_test(&engine, assign_func);
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(XctCommitConflictTest, NoConflict) {
  test_main([] (int i) { return i; } );  // no conflict
}

TEST(XctCommitConflictTest, LightConflict) {
  test_main([] (int i) { return i / 2; } );  // two threads per record
}

TEST(XctCommitConflictTest, HeavyConflict) {
  test_main([] (int i) { return i / 5; } );
}

TEST(XctCommitConflictTest, ExtremeConflict) {
  test_main([] (int /*i*/) { return 0; } );
}

}  // namespace xct
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(XctCommitConflictTest, foedus.xct);
