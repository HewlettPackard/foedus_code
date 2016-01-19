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
DEFINE_TEST_CASE_PACKAGE(XctIdRwTimeoutLockTest, foedus.xct);

const int kThreads = 10;
const int kKeys = 100;

RwLockableXctId keys[kKeys];
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

// Everyone is reader, and tries to acquire the same lock
ErrorStack read_only_task(const proc::ProcArguments& args) {
#ifdef MCS_RW_TIMEOUT_LOCK
  thread::Thread* context = args.context_;
  EXPECT_EQ(args.input_len_, sizeof(int));
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  const int kAcquires = 10;
  int id = *reinterpret_cast<const int*>(args.input_buffer_);
  assorted::UniformRandom rnd(id);
  for (int i = 0; i < kAcquires; ++i) {
    McsBlockIndex block = 0;
    uint32_t timeout = rnd.uniform_within(0, 1000000);  // 1 million cycles max
    auto result = context->mcs_acquire_reader_lock(keys[0].get_key_lock(), &block, timeout);
    // It is possible to fail, because of the timeout, even if we only have readers:
    // e.g., suppose T1, T2, T3 came in order to acquire, T1 is waiting, and T2 and T3
    // attached one after another in waiting state (the CAS to set WAITING | READER_SUCCESSOR
    // all succeeded). T2 and T3 might time out, before T1 found itself is granted. Note we
    // have different timeout durations for each acquire.
    if (result == kErrorCodeOk) {
      EXPECT_EQ(result, kErrorCodeOk);
      EXPECT_GT(block, 0);
      std::this_thread::sleep_for(std::chrono::nanoseconds(rnd.uniform_within(0, 100)));
      context->mcs_release_reader_lock(keys[0].get_key_lock(), block);
    }
  }
  ++done_count;
  locked[id] = true;
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
#endif
  return foedus::kRetOk;
}

// Only a single reader, acquire and release for 100 times
ErrorStack single_reader_task(const proc::ProcArguments& args) {
#ifdef MCS_RW_TIMEOUT_LOCK
  thread::Thread* context = args.context_;
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  const int kAcquires = 100;
  assorted::UniformRandom rnd(1234567);
  for (int i = 0; i < kAcquires; ++i) {
    McsBlockIndex block = 0;
    uint32_t timeout = rnd.uniform_within(0, 1000000);  // 1 million cycles max
    auto result = context->mcs_acquire_reader_lock(keys[0].get_key_lock(), &block, timeout);
    EXPECT_EQ(result, kErrorCodeOk);
    EXPECT_GT(block, 0);
    std::this_thread::sleep_for(std::chrono::nanoseconds(rnd.uniform_within(0, 100)));
    context->mcs_release_reader_lock(keys[0].get_key_lock(), block);
  }
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
#endif
  return foedus::kRetOk;
}

// Mix of readers and writers, acquire the same lock
ErrorStack read_write_task(const proc::ProcArguments& args) {
#ifdef MCS_RW_TIMEOUT_LOCK
  thread::Thread* context = args.context_;
  EXPECT_EQ(args.input_len_, sizeof(int));
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  const int kAcquires = 10;
  int id = *reinterpret_cast<const int*>(args.input_buffer_);
  assorted::UniformRandom rnd(id);
  for (int i = 0; i < kAcquires; ++i) {
    McsBlockIndex block = 0;
    uint32_t timeout = 0;//rnd.uniform_within(0, 1000000);  // 1 million cycles max
    if (id == 0) {
      auto result = context->mcs_acquire_reader_lock(keys[0].get_key_lock(), &block, timeout);
      // It is possible to fail, for the same reason as in read_only_task
      EXPECT_EQ(result, kErrorCodeOk);
      if (result == kErrorCodeOk) {
        EXPECT_EQ(result, kErrorCodeOk);
        EXPECT_GT(block, 0);
        std::this_thread::sleep_for(std::chrono::nanoseconds(rnd.uniform_within(0, 100)));
        context->mcs_release_reader_lock(keys[0].get_key_lock(), block);
      }
    } else {
       auto result = context->mcs_acquire_writer_lock(keys[0].get_key_lock(), &block, timeout);
      EXPECT_EQ(result, kErrorCodeOk);
      // It is possible to fail, for the same reason as in read_only_task
      if (result == kErrorCodeOk) {
        EXPECT_EQ(result, kErrorCodeOk);
        EXPECT_GT(block, 0);
        std::this_thread::sleep_for(std::chrono::nanoseconds(rnd.uniform_within(0, 100)));
        context->mcs_release_writer_lock(keys[0].get_key_lock(), block);
      }
    }
  }
  ++done_count;
  locked[id] = true;
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
#endif
  return foedus::kRetOk;
}
// Everyone is writer, and tries to acquire the same lock
ErrorStack write_only_task(const proc::ProcArguments& args) {
#ifdef MCS_RW_TIMEOUT_LOCK
  thread::Thread* context = args.context_;
  EXPECT_EQ(args.input_len_, sizeof(int));
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  const int kAcquires = 10;
  int id = *reinterpret_cast<const int*>(args.input_buffer_);
  assorted::UniformRandom rnd(id);
  for (int i = 0; i < kAcquires; ++i) {
    McsBlockIndex block = 0;
    uint32_t timeout = rnd.uniform_within(0, 1000000);  // 1 million cycles max
    auto result = context->mcs_acquire_writer_lock(keys[0].get_key_lock(), &block, timeout);
    // It is possible to fail, for the same reason as in read_only_task
    if (result == kErrorCodeOk) {
      EXPECT_EQ(result, kErrorCodeOk);
      EXPECT_GT(block, 0);
      std::this_thread::sleep_for(std::chrono::nanoseconds(rnd.uniform_within(0, 100)));
      context->mcs_release_writer_lock(keys[0].get_key_lock(), block);
    }
  }
  ++done_count;
  locked[id] = true;
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
#endif
  return foedus::kRetOk;
}
// A single write that acquires and releases the lock for 100 times
ErrorStack single_writer_task(const proc::ProcArguments& args) {
#ifdef MCS_RW_TIMEOUT_LOCK
  thread::Thread* context = args.context_;
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  const int kAcquires = 100;
  assorted::UniformRandom rnd(1234567);
  for (int i = 0; i < kAcquires; ++i) {
    McsBlockIndex block = 0;
    uint32_t timeout = rnd.uniform_within(0, 1000000);  // 1 million cycles max
    auto result = context->mcs_acquire_writer_lock(keys[0].get_key_lock(), &block, timeout);
    EXPECT_EQ(result, kErrorCodeOk);
    EXPECT_GT(block, 0);
    std::this_thread::sleep_for(std::chrono::nanoseconds(rnd.uniform_within(0, 100)));
    context->mcs_release_writer_lock(keys[0].get_key_lock(), block);
  }
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
#endif
  return foedus::kRetOk;
}
TEST(XctIdRwTimeoutLockTest, WriteOnly) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("write_only_task", write_only_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    init();
    std::vector<thread::ImpersonateSession> sessions;
    for (int i = 0; i < kThreads; ++i) {
      thread::ImpersonateSession session;
      bool ret = engine.get_thread_pool()->impersonate(
        "write_only_task",
        &i,
        sizeof(i),
        &session);
      EXPECT_TRUE(ret);
      EXPECT_TRUE(session.is_valid());
      ASSERT_ND(ret);
      ASSERT_ND(session.is_valid());
      sessions.emplace_back(std::move(session));
    }

    while (done_count < kThreads) {
      sleep_enough();
    }

    assorted::memory_fence_acquire();
    for (int i = 0; i < kKeys; ++i) {
      EXPECT_EQ(keys[i].get_key_lock()->nreaders(), 0);
      EXPECT_EQ(keys[i].get_key_lock()->tail_, 0);
    }
    for (int i = 0; i < kThreads; ++i) {
      COERCE_ERROR(sessions[i].get_result());
      sessions[i].release();
      EXPECT_TRUE(locked[i]);
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}
TEST(XctIdRwTimeoutLockTest, ReadOnly) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("read_only_task", read_only_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    init();
    std::vector<thread::ImpersonateSession> sessions;
    for (int i = 0; i < kThreads; ++i) {
      thread::ImpersonateSession session;
      bool ret = engine.get_thread_pool()->impersonate(
        "read_only_task",
        &i,
        sizeof(i),
        &session);
      EXPECT_TRUE(ret);
      EXPECT_TRUE(session.is_valid());
      ASSERT_ND(ret);
      ASSERT_ND(session.is_valid());
      sessions.emplace_back(std::move(session));
    }

    while (done_count < kThreads) {
      sleep_enough();
    }

    assorted::memory_fence_acquire();
    for (int i = 0; i < kKeys; ++i) {
      EXPECT_EQ(keys[i].get_key_lock()->nreaders(), 0);
    }
    for (int i = 0; i < kThreads; ++i) {
      COERCE_ERROR(sessions[i].get_result());
      sessions[i].release();
      EXPECT_TRUE(locked[i]);
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(XctIdRwTimeoutLockTest, SingleReader) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("single_reader_task", single_reader_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    init();
    std::vector<thread::ImpersonateSession> sessions;
    thread::ImpersonateSession session;
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("single_reader_task"));
    assorted::memory_fence_acquire();
    for (int i = 0; i < kKeys; ++i) {
      EXPECT_EQ(keys[i].get_key_lock()->nreaders(), 0);
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(XctIdRwTimeoutLockTest, SingleWriter) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("single_writer_task", single_writer_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    init();
    std::vector<thread::ImpersonateSession> sessions;
    thread::ImpersonateSession session;
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("single_writer_task"));
    assorted::memory_fence_acquire();
    for (int i = 0; i < kKeys; ++i) {
      EXPECT_EQ(keys[i].get_key_lock()->nreaders(), 0);
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}
TEST(XctIdRwTimeoutLockTest, ReadWrite) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = kThreads;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("read_write_task", read_write_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    init();
    std::vector<thread::ImpersonateSession> sessions;
    for (int i = 0; i < kThreads; ++i) {
      thread::ImpersonateSession session;
      bool ret = engine.get_thread_pool()->impersonate(
        "read_write_task",
        &i,
        sizeof(i),
        &session);
      EXPECT_TRUE(ret);
      EXPECT_TRUE(session.is_valid());
      ASSERT_ND(ret);
      ASSERT_ND(session.is_valid());
      sessions.emplace_back(std::move(session));
    }

    while (done_count < kThreads) {
      sleep_enough();
    }

    assorted::memory_fence_acquire();
    for (int i = 0; i < kKeys; ++i) {
      EXPECT_EQ(keys[i].get_key_lock()->nreaders(), 0);
    }
    for (int i = 0; i < kThreads; ++i) {
      COERCE_ERROR(sessions[i].get_result());
      sessions[i].release();
      EXPECT_TRUE(locked[i]);
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}
}  // namespace xct
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(XctIdRwTimeoutLockTest, foedus.xct);
