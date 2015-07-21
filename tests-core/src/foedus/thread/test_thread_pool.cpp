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
#include "foedus/thread/thread_ref.hpp"

namespace foedus {
namespace thread {
DEFINE_TEST_CASE_PACKAGE(ThreadPoolTest, foedus.thread);

ErrorStack dummy_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  void* user_memory
    = context->get_engine()->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory();
  soc::SharedRendezvous* rendezvous = reinterpret_cast<soc::SharedRendezvous*>(user_memory);
  rendezvous->wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  return kRetOk;
}

void run_test(int pooled_count, int impersonate_count) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = pooled_count;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("dummy_task", dummy_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    // Use global user memory for rendezvous.
    void* user_memory
      = engine.get_soc_manager()->get_shared_memory_repo()->get_global_user_memory();
    soc::SharedRendezvous* rendezvous = reinterpret_cast<soc::SharedRendezvous*>(user_memory);
    for (int rep = 0; rep < 10; ++rep) {
      rendezvous->initialize();
      std::vector<ImpersonateSession> sessions;
      for (int i = 0; i < impersonate_count; ++i) {
        thread::ImpersonateSession session;
        EXPECT_TRUE(engine.get_thread_pool()->impersonate("dummy_task", nullptr, 0, &session));
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

TEST(ThreadPoolTest, CheckCbAddresses) {
  const ThreadGroupId kGroups = 2;
  const ThreadLocalOrdinal kCoresPerGroup = 8;
  EngineOptions options = get_tiny_options();
  options.thread_.group_count_ = kGroups;
  options.thread_.thread_count_per_group_ = kCoresPerGroup;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    ThreadPool* pool = engine.get_thread_pool();
    for (auto g = 0; g < kGroups; ++g) {
      ThreadGroupRef* group_ref = pool->get_group_ref(g);
      EXPECT_EQ(g, group_ref->get_group_id());
      for (auto c = 0; c < kCoresPerGroup; ++c) {
        ThreadId thread_id = compose_thread_id(g, c);
        ThreadRef* ref = group_ref->get_thread(c);
        EXPECT_EQ(thread_id, ref->get_thread_id());
        EXPECT_EQ(ref, pool->get_thread_ref(thread_id));
        EXPECT_EQ(ref->get_control_block(), group_ref->get_thread_ref(c).get_control_block());
      }
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace thread
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(ThreadPoolTest, foedus.thread);
