/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <stdint.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_rendezvous.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/thread/impersonate_session.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"

namespace foedus {
namespace thread {
DEFINE_TEST_CASE_PACKAGE(ThreadPoolTest, foedus.thread);

ErrorStack dummy_task(
  thread::Thread* context,
  const void* /*input_buffer*/,
  uint32_t /*input_len*/,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  void* user_memory
    = context->get_engine()->get_soc_manager().get_shared_memory_repo()->get_global_user_memory();
  soc::SharedRendezvous* rendezvous = reinterpret_cast<soc::SharedRendezvous*>(user_memory);
  rendezvous->wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  return kRetOk;
}

void run_test(int pooled_count, int impersonate_count) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = pooled_count;
  Engine engine(options);
  engine.get_proc_manager().pre_register("dummy_task", dummy_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    // Use global user memory for rendezvous.
    void* user_memory = engine.get_soc_manager().get_shared_memory_repo()->get_global_user_memory();
    soc::SharedRendezvous* rendezvous = reinterpret_cast<soc::SharedRendezvous*>(user_memory);
    for (int rep = 0; rep < 10; ++rep) {
      rendezvous->initialize();
      std::vector<ImpersonateSession> sessions;
      for (int i = 0; i < impersonate_count; ++i) {
        thread::ImpersonateSession session;
        EXPECT_TRUE(engine.get_thread_pool().impersonate("dummy_task", nullptr, 0, &session));
        sessions.emplace_back(std::move(session));
      }

      rendezvous->signal();

      for (int i = 0; i < impersonate_count; ++i) {
        COERCE_ERROR(sessions[i].get_result());
      }
      rendezvous->uninitialize();
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(ThreadPoolTest, ImpersonateOneOne) { run_test(1, 1); }
TEST(ThreadPoolTest, ImpersonateTwoOne) { run_test(2, 1); }
TEST(ThreadPoolTest, ImpersonateFourOne) { run_test(4, 1); }
TEST(ThreadPoolTest, ImpersonateFourFour) { run_test(4, 4); }
TEST(ThreadPoolTest, ImpersonateTenFour) { run_test(10, 4); }
TEST(ThreadPoolTest, ImpersonateManyFour) { run_test(16, 4); }
TEST(ThreadPoolTest, ImpersonateManyMany) { run_test(16, 16); }

void run_sched(ThreadPolicy policy, ThreadPriority priority) {
  EngineOptions options = get_tiny_options();
  options.thread_.overwrite_thread_schedule_ = true;
  options.thread_.thread_policy_ = policy;
  options.thread_.thread_priority_ = priority;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(ThreadPoolTest, SchedIdle) { run_sched(kScheduleIdle, kPriorityIdle); }
TEST(ThreadPoolTest, SchedNormal) { run_sched(kScheduleRr, kPriorityDefault); }
TEST(ThreadPoolTest, SchedLowest) { run_sched(kScheduleRr, kPriorityLowest); }
TEST(ThreadPoolTest, SchedRealtime) { run_sched(kScheduleFifo, kPriorityHighest); }

}  // namespace thread
}  // namespace foedus
