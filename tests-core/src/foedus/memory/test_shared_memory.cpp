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
#include <spawn.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <string>

#include "foedus/test_common.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/memory/shared_memory.hpp"

/**
 * @file test_shared_memory.cpp
 * Testcases for SharedMemory.
 * If possible, use --trace-children=yes to check memory leak in child processes.
 */
namespace foedus {
namespace memory {

DEFINE_TEST_CASE_PACKAGE(SharedMemoryTest, foedus.memory);

std::string base_path() {
  std::string path = "/tmp/libfoedus_test_shared_memory_";
  path += std::to_string(::getpid());
  return path;
}

TEST(SharedMemoryTest, Alone) {
  SharedMemory memory;
  EXPECT_TRUE(memory.get_block() == nullptr);
  EXPECT_TRUE(memory.is_null());
  std::string meta_path = base_path() + std::string("_alone");
  COERCE_ERROR(memory.alloc(meta_path, 1ULL << 21, 0));
  EXPECT_TRUE(memory.get_block() != nullptr);
  EXPECT_EQ(1ULL << 21, memory.get_size());
  EXPECT_EQ(meta_path, memory.get_meta_path());
  EXPECT_NE(0, memory.get_shmid());
  EXPECT_NE(0, memory.get_shmkey());
  EXPECT_EQ(0, memory.get_numa_node());
  EXPECT_FALSE(memory.is_null());
  EXPECT_TRUE(memory.is_owned());
  EXPECT_EQ(::getpid(), memory.get_owner_pid());
  memory.release_block();
  EXPECT_TRUE(memory.get_block() == nullptr);
  EXPECT_TRUE(memory.is_null());
}

TEST(SharedMemoryTest, ShareFork) {
  bool was_child = false;
  {
    SharedMemory memory;
    EXPECT_TRUE(memory.get_block() == nullptr);
    EXPECT_TRUE(memory.is_null());
    std::string meta_path = base_path() + std::string("_share_fork");
    COERCE_ERROR(memory.alloc(meta_path, 1ULL << 21, 0));
    memory.get_block()[3] = 42;
    pid_t pid = ::fork();
    if (pid == -1) {
      memory.mark_for_release();
      ASSERT_ND(false);
      EXPECT_TRUE(false);
    } else if (pid == 0) {
      // child
      SharedMemory memory_child;
      memory_child.attach(meta_path);
      EXPECT_FALSE(memory_child.is_null());
      EXPECT_EQ(1ULL << 21, memory_child.get_size());
      EXPECT_EQ(meta_path, memory_child.get_meta_path());
      EXPECT_NE(0, memory_child.get_shmid());
      EXPECT_NE(0, memory_child.get_shmkey());
      EXPECT_EQ(0, memory_child.get_numa_node());
      EXPECT_EQ(42, memory_child.get_block()[3]);
      EXPECT_FALSE(memory.is_owned());
      was_child = true;
    } else {
      // parent
      int status;
      pid_t result = ::waitpid(pid, &status, 0);
      EXPECT_EQ(pid, result);
      EXPECT_EQ(0, status);
    }

    memory.release_block();
  }
  if (was_child) {
    ::_exit(0);  // otherwise, it goes on to execute the following tests again.
  }
}

TEST(SharedMemoryTest, ShareSpawn) {
  const char* env_meta_path = ::getenv("test_meta_path");
  if (env_meta_path) {
    // child process!
    {
      std::string meta_path(env_meta_path);
      std::cout << "I'm a child process(" << ::getpid()
        << "). meta_path=" << meta_path << std::endl;
      SharedMemory memory;
      memory.attach(meta_path);
      EXPECT_FALSE(memory.is_null());
      EXPECT_EQ(1ULL << 21, memory.get_size());
      EXPECT_EQ(meta_path, memory.get_meta_path());
      EXPECT_NE(0, memory.get_shmid());
      EXPECT_NE(0, memory.get_shmkey());
      EXPECT_EQ(0, memory.get_numa_node());
      EXPECT_EQ(42, memory.get_block()[3]);
      EXPECT_FALSE(memory.is_owned());
      memory.get_block()[5] = 67;
    }
    ::_exit(0);  // for the same reason, this should terminate now.
    return;
  } else {
    std::cout << "I'm a master process(" << ::getpid() << ")" << std::endl;
  }

  SharedMemory memory;
  EXPECT_TRUE(memory.get_block() == nullptr);
  EXPECT_TRUE(memory.is_null());
  std::string meta_path = base_path() + std::string("_share_spawn");
  COERCE_ERROR(memory.alloc(meta_path, 1ULL << 21, 0));
  memory.get_block()[3] = 42;


  posix_spawn_file_actions_t file_actions;
  posix_spawnattr_t attr;
  ::posix_spawn_file_actions_init(&file_actions);
  ::posix_spawnattr_init(&attr);

  std::string path = assorted::get_current_executable_path();
  // execute this test
  char* const new_argv[] = {
    const_cast<char*>(path.c_str()),
    const_cast<char*>("--gtest_filter=SharedMemoryTest.ShareSpawn"),
    nullptr};

  // with meta_path in environment variable
  std::string meta("test_meta_path=");
  meta += meta_path;
  char* const new_envp[] = {
    const_cast<char*>(meta.c_str()),
    nullptr};

  pid_t child_pid;
  std::cout << "spawning: " << path << std::endl;
  int ret = ::posix_spawn(&child_pid, path.c_str(), &file_actions, &attr, new_argv, new_envp);

  EXPECT_EQ(0, ret);
  if (ret == 0) {
    EXPECT_NE(0, child_pid);
    int status = 0;
    pid_t result = ::waitpid(child_pid, &status, 0);
    std::cout << "child process(" << child_pid << ") died. result=" << result << std::endl;
    EXPECT_EQ(child_pid, result);
    EXPECT_EQ(0, status);
  }
  assorted::memory_fence_acquire();
  EXPECT_EQ(67, memory.get_block()[5]);
  memory.release_block();
}

}  // namespace memory
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SharedMemoryTest, foedus.memory);
