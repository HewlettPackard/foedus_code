/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <unistd.h>
#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <string>

#include "foedus/test_common.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/memory/shared_memory.hpp"
#include "foedus/soc/shared_mutex.hpp"

namespace foedus {
namespace soc {

DEFINE_TEST_CASE_PACKAGE(SharedMutexTest, foedus.soc);

TEST(SharedMutexTest, Alone) {
  SharedMutex mtx;
  mtx.lock();
  mtx.unlock();
  mtx.lock();
  mtx.unlock();
}


std::string base_path() {
  std::string path = "/tmp/libfoedus_test_shared_mutex_";
  path += std::to_string(::getpid());
  return path;
}

TEST(SharedMutexTest, SharedMemoryAlone) {
  memory::SharedMemory memory;
  std::string meta_path = base_path() + std::string("_SharedMemoryAlone");
  memory.alloc(meta_path, 1ULL << 21, 0);
  memory.mark_for_release();
  EXPECT_NE(nullptr, memory.get_block());
  void* block = memory.get_block();
  SharedMutex *mtx = reinterpret_cast<SharedMutex*>(block);
  mtx->initialize();
  mtx->lock();
  mtx->unlock();
  {
    SharedMutexScope scope(mtx);
  }
  mtx->uninitialize();
}


// This must be the last test. otherwise gtest executes the following tests twice.
TEST(SharedMutexTest, SharedMemoryFork) {
  memory::SharedMemory memory;
  std::string meta_path = base_path() + std::string("_SharedMemoryFork");
  memory.alloc(meta_path, 1ULL << 21, 0);
  EXPECT_NE(nullptr, memory.get_block());
  char* block = reinterpret_cast<char*>(memory.get_block());

  SharedMutex *mtx = reinterpret_cast<SharedMutex*>(block);
  int *total = reinterpret_cast<int*>(block + sizeof(SharedMutex));
  mtx->initialize();
  *total = 0;
  assorted::memory_fence_release();

  pid_t pid = ::fork();
  const uint32_t kIterations = 3000;
  if (pid == -1) {
    memory.mark_for_release();
    ASSERT_ND(false);
    EXPECT_TRUE(false);
  } else if (pid == 0) {
    // child
    memory::SharedMemory memory_child;
    memory_child.attach(meta_path);
    char* child_block = reinterpret_cast<char*>(memory_child.get_block());
    SharedMutex *child_mtx = reinterpret_cast<SharedMutex*>(child_block);
    int *child_total = reinterpret_cast<int*>(child_block + sizeof(SharedMutex));
    EXPECT_TRUE(child_mtx->is_initialized());
    for (uint32_t i = 0; i < kIterations; ++i) {
      SharedMutexScope scope(child_mtx);
      *child_total = (*child_total) + 1;
    }
    return;
  } else {
    // parent
    EXPECT_TRUE(mtx->is_initialized());
    for (uint32_t i = 0; i < kIterations; ++i) {
      SharedMutexScope scope(mtx);
      *total = (*total) + 1;
    }

    int status;
    pid_t result = ::waitpid(pid, &status, 0);
    EXPECT_EQ(pid, result);
    EXPECT_EQ(0, status);
  }
  assorted::memory_fence_acquire();
  EXPECT_EQ(kIterations * 2, *total);
  mtx->uninitialize();
  memory.release_block();
}

}  // namespace soc
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SharedMutexTest, foedus.soc);
