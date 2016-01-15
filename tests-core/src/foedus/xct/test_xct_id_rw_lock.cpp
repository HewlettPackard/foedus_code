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
DEFINE_TEST_CASE_PACKAGE(XctIdRwLockTest, foedus.xct);

// Even IDs are readers, odd ones are writers
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
ErrorStack no_conflict_task(const proc::ProcArguments& args) {
#ifdef MCS_RW_LOCK
  thread::Thread* context = args.context_;
  EXPECT_EQ(args.input_len_, sizeof(int));
  int id = *reinterpret_cast<const int*>(args.input_buffer_);
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  McsBlockIndex block = 0;
  if (id % 2 == 0) {
    block = context->mcs_acquire_reader_lock(keys[id].get_key_lock());
  } else {
    block = context->mcs_acquire_writer_lock(keys[id].get_key_lock());
  }
  locked[id] = true;
  ++locked_count;
  while (!signaled) {
    sleep_enough();
    assorted::memory_fence_acquire();
  }
  if (id % 2 == 0) {
    context->mcs_release_reader_lock(keys[id].get_key_lock(), block);
  } else {
    context->mcs_release_writer_lock(keys[id].get_key_lock(), block);
  }
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
  done[id] = true;
  ++done_count;
#endif
  return foedus::kRetOk;
}

ErrorStack conflict_task(const proc::ProcArguments& args) {
#ifdef MCS_RW_LOCK
  thread::Thread* context = args.context_;
  EXPECT_EQ(args.input_len_, sizeof(int));
  int id = *reinterpret_cast<const int*>(args.input_buffer_);
  int l = id < kThreads / 2 ? id : id - kThreads / 2;
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  McsBlockIndex block = 0;
  if (id % 2 == 0) {
    block = context->mcs_acquire_reader_lock(keys[l].get_key_lock());
  } else {
    block = context->mcs_acquire_writer_lock(keys[l].get_key_lock());
  }
  locked[id] = true;
  ++locked_count;
  while (!signaled) {
    sleep_enough();
    assorted::memory_fence_acquire();
  }
  if (id % 2 == 0) {
    context->mcs_release_reader_lock(keys[l].get_key_lock(), block);
  } else {
    context->mcs_release_writer_lock(keys[l].get_key_lock(), block);
  }
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
  done[id] = true;
  ++done_count;
#endif
  return foedus::kRetOk;
}


ErrorStack random_task(const proc::ProcArguments& args) {
#ifdef MCS_RW_LOCK
  thread::Thread* context = args.context_;
  EXPECT_EQ(args.input_len_, sizeof(int));
  int id = *reinterpret_cast<const int*>(args.input_buffer_);
  assorted::UniformRandom r(id);
  XctManager* xct_manager = context->get_engine()->get_xct_manager();
  WRAP_ERROR_CODE(xct_manager->begin_xct(context, kSerializable));
  for (uint32_t i = 0; i < 1000; ++i) {
    uint32_t k = r.uniform_within(0, kKeys - 1);
    McsBlockIndex block = 0;
    if (id % 2 == 0) {
      block = context->mcs_acquire_reader_lock(keys[k].get_key_lock());
      context->mcs_release_reader_lock(keys[k].get_key_lock(), block);
    } else {
      block = context->mcs_acquire_writer_lock(keys[k].get_key_lock());
      context->mcs_release_writer_lock(keys[k].get_key_lock(), block);
    }
  }
  WRAP_ERROR_CODE(xct_manager->abort_xct(context));
  ++done_count;
  done[id] = true;
#endif
  return foedus::kRetOk;
}

TEST(XctIdRwLockTest, NoConflict) {
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
      bool ret = engine.get_thread_pool()->impersonate(
        "no_conflict_task",
        &i,
        sizeof(i),
        &session);
      EXPECT_TRUE(ret);
      EXPECT_TRUE(session.is_valid());
      ASSERT_ND(ret);
      ASSERT_ND(session.is_valid());
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

TEST(XctIdRwLockTest, Conflict) {
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
      EXPECT_TRUE(session.is_valid());
      ASSERT_ND(session.is_valid());
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
      EXPECT_TRUE(session.is_valid());
      ASSERT_ND(session.is_valid());
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

TEST(XctIdRwLockTest, Random) {
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
      bool ret = engine.get_thread_pool()->impersonate(
        "random_task",
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

TEST_MAIN_CAPTURE_SIGNALS(XctIdRwLockTest, foedus.xct);
