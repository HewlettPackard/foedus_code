/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/thread/impersonate_session.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread.hpp>
#include <stdint.h>
#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <vector>

namespace foedus {
namespace thread {

class DummyTask : public ImpersonateTask {
 public:
    ErrorStack run(Thread* /*context*/) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        return RET_OK;
    }
};

void run_test(int pooled_count, int impersonate_count) {
    EngineOptions options = get_tiny_options();
    options.thread_.thread_count_per_group_ = pooled_count;
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
        UninitializeGuard guard(&engine);
        std::vector<DummyTask*> tasks;
        std::vector<ImpersonateSession> sessions;
        for (int i = 0; i < impersonate_count; ++i) {
            tasks.push_back(new DummyTask());
            sessions.push_back(engine.get_thread_pool().impersonate(tasks[i]));
        }

        for (int i = 0; i < impersonate_count; ++i) {
            COERCE_ERROR(sessions[i].get_result());
        }

        for (DummyTask* task : tasks) {
            delete task;
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
