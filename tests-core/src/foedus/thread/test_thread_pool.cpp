/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>
#include <stdint.h>

#include <chrono>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"
#include "foedus/thread/impersonate_session.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"

namespace foedus {
namespace thread {
DEFINE_TEST_CASE_PACKAGE(ThreadPoolTest, foedus.thread);

struct DummyTask : public ImpersonateTask {
  explicit DummyTask(Rendezvous *rendezvous) : rendezvous_(rendezvous) {}
  ErrorStack run(Thread* /*context*/) {
    rendezvous_->wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    return kRetOk;
  }
  Rendezvous *rendezvous_;
};

void run_test(int pooled_count, int impersonate_count) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = pooled_count;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    for (int rep = 0; rep < 10; ++rep) {
      Rendezvous rendezvous;
      std::vector<DummyTask*> tasks;
      std::vector<ImpersonateSession> sessions;
      for (int i = 0; i < impersonate_count; ++i) {
        tasks.push_back(new DummyTask(&rendezvous));
        sessions.push_back(engine.get_thread_pool().impersonate(tasks[i]));
      }

      rendezvous.signal();

      for (int i = 0; i < impersonate_count; ++i) {
        COERCE_ERROR(sessions[i].get_result());
        delete tasks[i];
      }
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

}  // namespace thread
}  // namespace foedus
