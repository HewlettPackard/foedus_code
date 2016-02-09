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

// adjust these when debugging
const int kMaxCores = 4;
const int kMaxNodes = 4;
const int kRounds = 1;

int core_count = 0;

RwLockableXctId key;
bool  signaled;
std::atomic<int> done_count;
assorted::UniformRandom spin_rnd;

void sleep_enough() {
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

void spin() {
  int32_t rounds = spin_rnd.uniform_within(1, 1000);
  while (--rounds) {}
}

void init() {
  key.reset();
  EXPECT_FALSE(key.xct_id_.is_valid());
  EXPECT_FALSE(key.is_deleted());
  EXPECT_FALSE(key.is_keylocked());
  EXPECT_FALSE(key.is_moved());
  done_count = 0;
  signaled = false;
}

bool timeout_reader_acquire(
  thread::Thread* context, McsRwLock* lock, McsBlockIndex block_index, int32_t timeout) {
  while (timeout--) {
    if (context->mcs_retry_async_rw_reader(lock, block_index)) {
      return true;
    }
  }
  return false;
}

bool timeout_writer_acquire(
  thread::Thread* context, McsRwLock* lock, McsBlockIndex block_index, int32_t timeout) {
  while (timeout--) {
    if (context->mcs_retry_async_rw_writer(lock, block_index)) {
      return true;
    }
  }
  return false;
}

// Everyone is reader, and tries to acquire the same lock
ErrorStack read_only_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  EXPECT_EQ(args.input_len_, sizeof(int));
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  const int kAcquires = 200;
  int id = *reinterpret_cast<const int*>(args.input_buffer_);
  assorted::UniformRandom rnd(id);
  for (int i = 0; i < kAcquires; ++i) {
    uint32_t timeout = rnd.uniform_within(0, 200000);
    McsBlockIndex block = 0;
    if (timeout % 1000 == 0) {
      block = context->mcs_acquire_reader_lock(key.get_key_lock());
      EXPECT_GT(block, 0);
      spin();
      context->mcs_release_reader_lock(key.get_key_lock(), block);
    } else {
      block = context->mcs_try_acquire_reader_lock(key.get_key_lock());
      EXPECT_GT(block, 0);
      if (timeout_reader_acquire(context, key.get_key_lock(), block, timeout)) {
        spin();
        context->mcs_release_reader_lock(key.get_key_lock(), block);
      } else {
        context->mcs_cancel_async_rw_reader(key.get_key_lock(), block);
      }
    }
  }
  ++done_count;
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
  return foedus::kRetOk;
}

// Everyone is writer, and tries to acquire the same lock
ErrorStack write_only_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  EXPECT_EQ(args.input_len_, sizeof(int));
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  const int kAcquires = 100;
  int id = *reinterpret_cast<const int*>(args.input_buffer_);
  assorted::UniformRandom rnd(id);
  for (int i = 0; i < kAcquires; ++i) {
    uint32_t timeout = rnd.uniform_within(0, 200000);
    McsBlockIndex block = 0;
    if (timeout % 1000 == 0) {
      block = context->mcs_acquire_writer_lock(key.get_key_lock());
      EXPECT_GT(block, 0);
      spin();
      context->mcs_release_writer_lock(key.get_key_lock(), block);
    } else {
      block = context->mcs_try_acquire_writer_lock(key.get_key_lock());
      EXPECT_GT(block, 0);
      if (timeout_writer_acquire(context, key.get_key_lock(), block, timeout)) {
        spin();
        context->mcs_release_writer_lock(key.get_key_lock(), block);
      } else {
        context->mcs_cancel_async_rw_writer(key.get_key_lock(), block);
      }
    }
  }
  ++done_count;
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
  return foedus::kRetOk;
}

// Mix of readers and writers, acquire the same lock
ErrorStack read_write_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  EXPECT_EQ(args.input_len_, sizeof(int));
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  const int kAcquires = 300;
  int id = *reinterpret_cast<const int*>(args.input_buffer_);
  assorted::UniformRandom rnd(id);
  for (int i = 0; i < kAcquires; ++i) {
    McsBlockIndex block = 0;
    uint32_t timeout = rnd.uniform_within(0, 200000);
    if (i % 2 == 0) {
      if (timeout % 1000 == 0) {
        block = context->mcs_acquire_reader_lock(key.get_key_lock());
        EXPECT_GT(block, 0);
        spin();
        context->mcs_release_reader_lock(key.get_key_lock(), block);
      } else {
        block = context->mcs_try_acquire_reader_lock(key.get_key_lock());
        EXPECT_GT(block, 0);
        if (timeout_reader_acquire(context, key.get_key_lock(), block, timeout)) {
          spin();
          context->mcs_release_reader_lock(key.get_key_lock(), block);
        } else {
          context->mcs_cancel_async_rw_reader(key.get_key_lock(), block);
        }
      }
    } else {
      if (timeout % 1000 == 0) {
        block = context->mcs_acquire_writer_lock(key.get_key_lock());
        EXPECT_GT(block, 0);
        spin();
        context->mcs_release_writer_lock(key.get_key_lock(), block);
      } else {
        block = context->mcs_try_acquire_writer_lock(key.get_key_lock());
        EXPECT_GT(block, 0);
        if (timeout_writer_acquire(context, key.get_key_lock(), block, timeout)) {
          spin();
          context->mcs_release_writer_lock(key.get_key_lock(), block);
        } else {
          context->mcs_cancel_async_rw_writer(key.get_key_lock(), block);
        }
      }
    }
  }
  ++done_count;
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
  return foedus::kRetOk;
}

// Only a single reader, acquire and release for 100 times
ErrorStack single_reader_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  const int kAcquires = 100;
  assorted::UniformRandom rnd(1234567);
  for (int i = 0; i < kAcquires; ++i) {
    McsBlockIndex block = context->mcs_acquire_reader_lock(key.get_key_lock());
    EXPECT_GT(block, 0);
    spin();
    context->mcs_release_reader_lock(key.get_key_lock(), block);
  }
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
  return foedus::kRetOk;
}

// A single write that acquires and releases the lock for 100 times
ErrorStack single_writer_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  const int kAcquires = 100;
  assorted::UniformRandom rnd(1234567);
  for (int i = 0; i < kAcquires; ++i) {
    McsBlockIndex block = context->mcs_acquire_writer_lock(key.get_key_lock());
    EXPECT_GT(block, 0);
    spin();
    context->mcs_release_writer_lock(key.get_key_lock(), block);
  }
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
  return foedus::kRetOk;
}

TEST(XctIdRwTimeoutLockTest, WriteOnly) {
  EngineOptions options = get_tiny_options();
  if (options.xct_.mcs_implementation_type_ != XctOptions::kMcsImplementationTypeExtended) {
    return;
  }
  options.thread_.group_count_ = std::min(
    kMaxNodes, static_cast<int>(options.thread_.group_count_));
  // assuming htt is on...
  options.thread_.thread_count_per_group_ =
    std::min(kMaxCores, std::max(options.thread_.thread_count_per_group_ / 2, 1));
  ASSERT_ND(options.thread_.group_count_ <= kMaxNodes);
  ASSERT_ND(options.thread_.thread_count_per_group_ <= kMaxCores);
  core_count = options.thread_.group_count_ * options.thread_.thread_count_per_group_; 

  for (int i = 0; i < kRounds; i++) {
    std::cout << "round " << i << std::endl;
    Engine engine(options);
    engine.get_proc_manager()->pre_register("write_only_task", write_only_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      init();
      std::vector<thread::ImpersonateSession> sessions;
      for (int i = 0; i < core_count; ++i) {
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

      while (done_count < core_count) {
        sleep_enough();
      }

      assorted::memory_fence_acquire();
      EXPECT_EQ(key.get_key_lock()->nreaders(), 0);
      EXPECT_EQ(key.get_key_lock()->tail_, 0);
      for (int i = 0; i < core_count; ++i) {
        COERCE_ERROR(sessions[i].get_result());
        sessions[i].release();
      }
      COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
  }
}
TEST(XctIdRwTimeoutLockTest, ReadOnly) {
  EngineOptions options = get_tiny_options();
  if (options.xct_.mcs_implementation_type_ != XctOptions::kMcsImplementationTypeExtended) {
    return;
  }
  options.thread_.group_count_ = std::min(
    kMaxNodes, static_cast<int>(options.thread_.group_count_));
  // assuming htt is on...
  options.thread_.thread_count_per_group_ =
    std::min(kMaxCores, std::max(options.thread_.thread_count_per_group_ / 2, 1));
  ASSERT_ND(options.thread_.group_count_ <= kMaxNodes);
  ASSERT_ND(options.thread_.thread_count_per_group_ <= kMaxCores);
  core_count = options.thread_.group_count_ * options.thread_.thread_count_per_group_; 

  for (int i = 0; i < kRounds; i++) {
    std::cout << "round " << i << std::endl;
    Engine engine(options);
    engine.get_proc_manager()->pre_register("read_only_task", read_only_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      init();
      std::vector<thread::ImpersonateSession> sessions;
      for (int i = 0; i < core_count; ++i) {
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

      while (done_count < core_count) {
        sleep_enough();
      }

      assorted::memory_fence_acquire();
      EXPECT_EQ(key.get_key_lock()->nreaders(), 0);
      for (int i = 0; i < core_count; ++i) {
        COERCE_ERROR(sessions[i].get_result());
        sessions[i].release();
      }
      COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
  }
}

TEST(XctIdRwTimeoutLockTest, SingleReader) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = 1;
  options.thread_.group_count_ = 1;
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
    EXPECT_EQ(key.get_key_lock()->nreaders(), 0);
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(XctIdRwTimeoutLockTest, SingleWriter) {
  EngineOptions options = get_tiny_options();
  options.thread_.thread_count_per_group_ = 1;
  options.thread_.group_count_ = 1;
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
    EXPECT_EQ(key.get_key_lock()->nreaders(), 0);
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}
TEST(XctIdRwTimeoutLockTest, ReadWrite) {
  EngineOptions options = get_tiny_options();
  if (options.xct_.mcs_implementation_type_ != XctOptions::kMcsImplementationTypeExtended) {
    return;
  }
  options.thread_.group_count_ = std::min(
    kMaxNodes, static_cast<int>(options.thread_.group_count_));
  // assuming htt is on...
  options.thread_.thread_count_per_group_ =
    std::min(kMaxCores, std::max(options.thread_.thread_count_per_group_ / 2, 1));
  ASSERT_ND(options.thread_.group_count_ <= kMaxNodes);
  ASSERT_ND(options.thread_.thread_count_per_group_ <= kMaxCores);
  core_count = options.thread_.group_count_ * options.thread_.thread_count_per_group_; 

  for (int i = 0; i < kRounds; i++) {
    std::cout << "round " << i << std::endl;
    Engine engine(options);
    engine.get_proc_manager()->pre_register("read_write_task", read_write_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      init();
      std::vector<thread::ImpersonateSession> sessions;
      for (int i = 0; i < core_count; ++i) {
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

      while (done_count < core_count) {
        sleep_enough();
      }

      assorted::memory_fence_acquire();
      EXPECT_EQ(key.get_key_lock()->nreaders(), 0);
      for (int i = 0; i < core_count; ++i) {
        COERCE_ERROR(sessions[i].get_result());
        sessions[i].release();
      }
      COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
  }
}

}  // namespace xct
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(XctIdRwTimeoutLockTest, foedus.xct);
