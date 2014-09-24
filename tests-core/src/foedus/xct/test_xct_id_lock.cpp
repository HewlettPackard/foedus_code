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
#include "foedus/proc/proc_manager.hpp"
#include "foedus/thread/impersonate_session.hpp"
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
ErrorStack no_conflict_task(
  thread::Thread* context,
  const void* input_buffer,
  uint32_t input_len,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  EXPECT_EQ(input_len, sizeof(int));
  int id = *reinterpret_cast<const int*>(input_buffer);
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  McsBlockIndex block = context->mcs_acquire_lock(keys[id].get_key_lock());
  locked[id] = true;
  ++locked_count;
  while (!signaled) {
    sleep_enough();
    assorted::memory_fence_acquire();
  }
  context->mcs_release_lock(keys[id].get_key_lock(), block);
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
  done[id] = true;
  ++done_count;
  return foedus::kRetOk;
}


ErrorStack conflict_task(
  thread::Thread* context,
  const void* input_buffer,
  uint32_t input_len,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  EXPECT_EQ(input_len, sizeof(int));
  int id = *reinterpret_cast<const int*>(input_buffer);
  int l = id < kThreads / 2 ? id : id - kThreads / 2;
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  McsBlockIndex block = context->mcs_acquire_lock(keys[l].get_key_lock());
  locked[id] = true;
  ++locked_count;
  while (!signaled) {
    sleep_enough();
    assorted::memory_fence_acquire();
  }
  context->mcs_release_lock(keys[l].get_key_lock(), block);
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
  done[id] = true;
  ++done_count;
  return foedus::kRetOk;
}


ErrorStack random_task(
  thread::Thread* context,
  const void* input_buffer,
  uint32_t input_len,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  EXPECT_EQ(input_len, sizeof(int));
  int id = *reinterpret_cast<const int*>(input_buffer);
  assorted::UniformRandom r(id);
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  for (uint32_t i = 0; i < 1000; ++i) {
    uint32_t k = r.uniform_within(0, kKeys - 1);
    McsBlockIndex block = context->mcs_acquire_lock(keys[k].get_key_lock());
    context->mcs_release_lock(keys[k].get_key_lock(), block);
  }
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
  ++done_count;
  done[id] = true;
  return foedus::kRetOk;
}

TEST(XctIdLockTest, NoConflict) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("no_conflict_task", no_conflict_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    init();
    std::vector<thread::ImpersonateSession> sessions;
    for (int i = 0; i < kThreads; ++i) {
      thread::ImpersonateSession session;
      EXPECT_TRUE(engine.get_thread_pool()->impersonate(
        "no_conflict_task",
        &i,
        sizeof(i),
        &session));
      sessions.emplace_back(std::move(session));
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
      COERCE_ERROR(sessions[i].get_result());
      sessions[i].release();
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(XctIdLockTest, Conflict) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("conflict_task", conflict_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    init();
    std::vector<thread::ImpersonateSession> sessions;
    for (int i = 0; i < kThreads / 2; ++i) {
      thread::ImpersonateSession session;
      EXPECT_TRUE(engine.get_thread_pool()->impersonate(
        "conflict_task",
        &i,
        sizeof(i),
        &session));
      sessions.emplace_back(std::move(session));
    }
    while (locked_count < kThreads / 2) {
      sleep_enough();
    }
    for (int i = kThreads / 2; i < kThreads; ++i) {
      thread::ImpersonateSession session;
      EXPECT_TRUE(engine.get_thread_pool()->impersonate(
        "conflict_task",
        &i,
        sizeof(i),
        &session));
      sessions.emplace_back(std::move(session));
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
      COERCE_ERROR(sessions[i].get_result());
      sessions[i].release();
    }

    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(XctIdLockTest, Random) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("random_task", random_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    init();
    std::vector<thread::ImpersonateSession> sessions;
    for (int i = 0; i < kThreads; ++i) {
      thread::ImpersonateSession session;
      EXPECT_TRUE(engine.get_thread_pool()->impersonate(
        "random_task",
        &i,
        sizeof(i),
        &session));
      sessions.emplace_back(std::move(session));
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
      COERCE_ERROR(sessions[i].get_result());
      sessions[i].release();
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace xct
}  // namespace foedus
