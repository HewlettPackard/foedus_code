/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <stdint.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/thread/impersonate_session.hpp"
#include "foedus/thread/impersonate_task.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace xct {
DEFINE_TEST_CASE_PACKAGE(XctIdLockTest, foedus.xct);

const int kThreads = 10;
const int kKeys = 100;

LockableXctId keys[kKeys];
bool  locked[kThreads];
bool  done[kThreads];
bool  signaled;
std::atomic<int> locked_count;
std::atomic<int> done_count;

void sleep_enough() {
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

void init() {
  for (int i = 0; i < kKeys; ++i) {
    keys[i].reset();
    EXPECT_FALSE(keys[i].xct_id_.is_valid());
    EXPECT_FALSE(keys[i].is_deleted());
    EXPECT_FALSE(keys[i].is_keylocked());
    EXPECT_FALSE(keys[i].is_moved());
  }
  for (int i = 0; i < kThreads; ++i) {
    done[i] = false;
    locked[i] = false;
  }
  locked_count = 0;
  done_count = 0;
  signaled = false;
}

class NoConflictTask : public thread::ImpersonateTask {
 public:
  explicit NoConflictTask(int id) : id_(id) {}
  ErrorStack run(thread::Thread* context)  {
    XctManager& xct_manager = context->get_engine()->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, kSerializable));
    McsBlockIndex block = context->mcs_acquire_lock(keys[id_].get_key_lock());
    locked[id_] = true;
    ++locked_count;
    while (!signaled) {
      sleep_enough();
      assorted::memory_fence_acquire();
    }
    context->mcs_release_lock(keys[id_].get_key_lock(), block);
    WRAP_ERROR_CODE(xct_manager.abort_xct(context));
    done[id_] = true;
    ++done_count;
    return foedus::kRetOk;
  }
  int id_;
};


class ConflictTask : public thread::ImpersonateTask {
 public:
  explicit ConflictTask(int id) : id_(id) {}
  ErrorStack run(thread::Thread* context) {
    int l = id_ < kThreads / 2 ? id_ : id_ - kThreads / 2;
    XctManager& xct_manager = context->get_engine()->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, kSerializable));
    McsBlockIndex block = context->mcs_acquire_lock(keys[l].get_key_lock());
    locked[id_] = true;
    ++locked_count;
    while (!signaled) {
      sleep_enough();
      assorted::memory_fence_acquire();
    }
    context->mcs_release_lock(keys[l].get_key_lock(), block);
    WRAP_ERROR_CODE(xct_manager.abort_xct(context));
    done[id_] = true;
    ++done_count;
    return foedus::kRetOk;
  }
  int id_;
};


class RandomTask : public thread::ImpersonateTask {
 public:
  explicit RandomTask(int id) : id_(id) {}
  ErrorStack run(thread::Thread* context) {
    assorted::UniformRandom r(id_);
    XctManager& xct_manager = context->get_engine()->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, kSerializable));
    for (uint32_t i = 0; i < 1000; ++i) {
      uint32_t k = r.uniform_within(0, kKeys - 1);
      McsBlockIndex block = context->mcs_acquire_lock(keys[k].get_key_lock());
      context->mcs_release_lock(keys[k].get_key_lock(), block);
    }
    WRAP_ERROR_CODE(xct_manager.abort_xct(context));
    ++done_count;
    done[id_] = true;
    return foedus::kRetOk;
  }
  int id_;
};

TEST(XctIdLockTest, NoConflict) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    init();
    std::vector<NoConflictTask*> tasks;
    std::vector<thread::ImpersonateSession> sessions;
    for (int i = 0; i < kThreads; ++i) {
      tasks.push_back(new NoConflictTask(i));
      sessions.emplace_back(engine.get_thread_pool().impersonate(tasks.back()));
      if (!sessions.back().is_valid()) {
        COERCE_ERROR(sessions.back().invalid_cause_);
      }
    }

    while (locked_count < kThreads) {
      sleep_enough();
    }

    assorted::memory_fence_acquire();
    for (int i = 0; i < kThreads; ++i) {
      EXPECT_FALSE(keys[i].xct_id_.is_valid());
      EXPECT_FALSE(keys[i].is_deleted());
      EXPECT_TRUE(keys[i].is_keylocked());
      EXPECT_FALSE(keys[i].is_moved());
      EXPECT_TRUE(locked[i]);
      EXPECT_FALSE(done[i]);
    }
    assorted::memory_fence_release();
    signaled = true;
    while (done_count < kThreads) {
      sleep_enough();
    }
    for (int i = 0; i < kThreads; ++i) {
      EXPECT_TRUE(locked[i]);
      EXPECT_TRUE(done[i]);
      EXPECT_FALSE(keys[i].is_keylocked());
    }
    for (int i = 0; i < kThreads; ++i) {
      if (sessions[i].get_result().is_error()) {
        COERCE_ERROR(sessions[i].get_result());
      }
      delete tasks[i];
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(XctIdLockTest, Conflict) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    init();
    std::vector<ConflictTask*> tasks;
    std::vector<thread::ImpersonateSession> sessions;
    for (int i = 0; i < kThreads / 2; ++i) {
      tasks.push_back(new ConflictTask(i));
      sessions.emplace_back(engine.get_thread_pool().impersonate(tasks.back()));
      if (!sessions.back().is_valid()) {
        COERCE_ERROR(sessions.back().invalid_cause_);
      }
    }
    while (locked_count < kThreads / 2) {
      sleep_enough();
    }
    for (int i = kThreads / 2; i < kThreads; ++i) {
      tasks.push_back(new ConflictTask(i));
      sessions.emplace_back(engine.get_thread_pool().impersonate(tasks.back()));
      if (!sessions.back().is_valid()) {
        COERCE_ERROR(sessions.back().invalid_cause_);
      }
    }
    for (int i = 0; i < 4; ++i) {
      sleep_enough();
    }
    assorted::memory_fence_acquire();
    for (int i = 0; i < kThreads; ++i) {
      int l = i < kThreads / 2 ? i : i - kThreads / 2;
      EXPECT_FALSE(keys[l].xct_id_.is_valid()) << i;
      EXPECT_FALSE(keys[l].is_deleted()) << i;
      EXPECT_TRUE(keys[l].is_keylocked()) << i;
      EXPECT_FALSE(keys[l].is_moved()) << i;
      if (i < kThreads / 2) {
        EXPECT_TRUE(locked[i]) << i;
      } else {
        EXPECT_FALSE(locked[i]) << i;
      }
      EXPECT_FALSE(done[i]) << i;
    }
    assorted::memory_fence_release();
    signaled = true;
    while (done_count < kThreads) {
      sleep_enough();
    }
    for (int i = 0; i < kThreads; ++i) {
      EXPECT_TRUE(locked[i]) << i;
      EXPECT_TRUE(done[i]) << i;
      EXPECT_FALSE(keys[i].is_keylocked()) << i;
    }
    for (int i = 0; i < kThreads; ++i) {
      if (sessions[i].get_result().is_error()) {
        COERCE_ERROR(sessions[i].get_result());
      }
      delete tasks[i];
    }

    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(XctIdLockTest, Random) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    init();
    std::vector<RandomTask*> tasks;
    std::vector<thread::ImpersonateSession> sessions;
    for (int i = 0; i < kThreads; ++i) {
      tasks.push_back(new RandomTask(i));
      sessions.emplace_back(engine.get_thread_pool().impersonate(tasks.back()));
      if (!sessions.back().is_valid()) {
        COERCE_ERROR(sessions.back().invalid_cause_);
      }
    }

    while (done_count < kThreads) {
      sleep_enough();
    }

    assorted::memory_fence_acquire();
    for (int i = 0; i < kKeys; ++i) {
      EXPECT_FALSE(keys[i].xct_id_.is_valid());
      EXPECT_FALSE(keys[i].is_deleted());
      EXPECT_FALSE(keys[i].is_keylocked());
      EXPECT_FALSE(keys[i].is_moved());
    }
    for (int i = 0; i < kThreads; ++i) {
      EXPECT_TRUE(done[i]) << i;
    }
    assorted::memory_fence_release();
    for (int i = 0; i < kThreads; ++i) {
      if (sessions[i].get_result().is_error()) {
        COERCE_ERROR(sessions[i].get_result());
      }
      delete tasks[i];
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace xct
}  // namespace foedus
