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
#include <unistd.h>
#include <gtest/gtest.h>

#include <string>

#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"
#include "foedus/soc/shared_memory_repo.hpp"

/**
 * @file test_shared_memory_repo.cpp
 * Testcases for SharedMemoryRepo \b without fork or spawn.
 * Testcases with fork/spawn are separated to test_shared_memory_repo_multip.cpp.
 */
namespace foedus {
namespace soc {

DEFINE_TEST_CASE_PACKAGE(SharedMemoryRepoTest, foedus.soc);

TEST(SharedMemoryRepoTest, Alone) {
  SharedMemoryRepo repo;
  EXPECT_EQ(nullptr, repo.get_global_memory());
  EngineOptions options = get_tiny_options();
  Upid pid = ::getpid();
  Eid eid = 1234;
  EXPECT_FALSE(repo.allocate_shared_memories(pid, eid, options).is_error());
  EXPECT_NE(nullptr, repo.get_global_memory());
  EXPECT_NE(nullptr, repo.get_global_memory_anchors()->log_manager_memory_);
  EXPECT_NE(nullptr, repo.get_global_memory_anchors()->restart_manager_memory_);
  EXPECT_NE(nullptr, repo.get_global_memory_anchors()->savepoint_manager_memory_);
  EXPECT_NE(nullptr, repo.get_global_memory_anchors()->snapshot_manager_memory_);
  EXPECT_NE(nullptr, repo.get_global_memory_anchors()->storage_manager_memory_);
  EXPECT_NE(nullptr, repo.get_global_memory_anchors()->xct_manager_memory_);
  EXPECT_NE(nullptr, repo.get_global_memory_anchors()->master_status_memory_);
  EXPECT_NE(nullptr, repo.get_global_memory_anchors()->options_xml_);
  EXPECT_NE(0, repo.get_global_memory_anchors()->options_xml_length_);
  EXPECT_NE(nullptr, repo.get_global_memory_anchors()->storage_name_sort_memory_);
  EXPECT_NE(nullptr, repo.get_global_memory_anchors()->storage_memories_);

  EXPECT_NE(nullptr, repo.get_node_memory(0));
  NodeMemoryAnchors* node_anchors = repo.get_node_memory_anchors(0);
  EXPECT_NE(nullptr, node_anchors->child_status_memory_);
  EXPECT_NE(nullptr, node_anchors->logger_memories_);
  for (uint16_t i = 0; i < options.log_.loggers_per_node_; ++i) {
    EXPECT_NE(nullptr, node_anchors->logger_memories_[i]);
  }
  EXPECT_NE(nullptr, node_anchors->volatile_pool_status_);
  EXPECT_NE(nullptr, node_anchors->proc_manager_memory_);
  EXPECT_NE(nullptr, node_anchors->proc_memory_);
  EXPECT_NE(nullptr, node_anchors->proc_name_sort_memory_);
  for (uint16_t i = 0; i < options.thread_.thread_count_per_group_; ++i) {
    ThreadMemoryAnchors* thread_anchors_ = node_anchors->thread_anchors_ + i;
    EXPECT_NE(nullptr, thread_anchors_->thread_memory_);
    EXPECT_NE(nullptr, thread_anchors_->task_input_memory_);
    EXPECT_NE(nullptr, thread_anchors_->task_output_memory_);
    EXPECT_NE(nullptr, thread_anchors_->mcs_ww_lock_memories_);
  }

  repo.deallocate_shared_memories();
  EXPECT_EQ(nullptr, repo.get_global_memory());
  EXPECT_EQ(nullptr, repo.get_global_memory());
  EXPECT_EQ(nullptr, repo.get_global_memory_anchors()->log_manager_memory_);
  EXPECT_EQ(nullptr, repo.get_global_memory_anchors()->restart_manager_memory_);
  EXPECT_EQ(nullptr, repo.get_global_memory_anchors()->savepoint_manager_memory_);
  EXPECT_EQ(nullptr, repo.get_global_memory_anchors()->snapshot_manager_memory_);
  EXPECT_EQ(nullptr, repo.get_global_memory_anchors()->storage_manager_memory_);
  EXPECT_EQ(nullptr, repo.get_global_memory_anchors()->xct_manager_memory_);
  EXPECT_EQ(nullptr, repo.get_global_memory_anchors()->master_status_memory_);
  EXPECT_EQ(nullptr, repo.get_global_memory_anchors()->options_xml_);
  EXPECT_EQ(0, repo.get_global_memory_anchors()->options_xml_length_);
  EXPECT_EQ(nullptr, repo.get_global_memory_anchors()->storage_name_sort_memory_);
  EXPECT_EQ(nullptr, repo.get_global_memory_anchors()->storage_memories_);
}

TEST(SharedMemoryRepoTest, Attach) {
  SharedMemoryRepo repo;
  EngineOptions options = get_tiny_options();
  Upid pid = ::getpid();
  Eid eid = 1235;
  EXPECT_FALSE(repo.allocate_shared_memories(pid, eid, options).is_error());

  SharedMemoryRepo child;
  EngineOptions child_options;
  EXPECT_FALSE(child.attach_shared_memories(pid, eid, 0, &child_options).is_error());

  EXPECT_NE(nullptr, child.get_global_memory());
  EXPECT_NE(nullptr, child.get_global_memory_anchors()->log_manager_memory_);
  EXPECT_NE(nullptr, child.get_global_memory_anchors()->restart_manager_memory_);
  EXPECT_NE(nullptr, child.get_global_memory_anchors()->savepoint_manager_memory_);
  EXPECT_NE(nullptr, child.get_global_memory_anchors()->snapshot_manager_memory_);
  EXPECT_NE(nullptr, child.get_global_memory_anchors()->storage_manager_memory_);
  EXPECT_NE(nullptr, child.get_global_memory_anchors()->xct_manager_memory_);
  EXPECT_NE(nullptr, child.get_global_memory_anchors()->master_status_memory_);
  EXPECT_NE(nullptr, child.get_global_memory_anchors()->options_xml_);
  EXPECT_NE(0, child.get_global_memory_anchors()->options_xml_length_);
  EXPECT_NE(nullptr, child.get_global_memory_anchors()->storage_name_sort_memory_);
  EXPECT_NE(nullptr, child.get_global_memory_anchors()->storage_memories_);

  EXPECT_EQ(options.thread_.group_count_, child_options.thread_.group_count_);
  EXPECT_EQ(options.thread_.thread_count_per_group_, child_options.thread_.thread_count_per_group_);
  EXPECT_EQ(options.savepoint_.savepoint_path_, child_options.savepoint_.savepoint_path_);
  EXPECT_EQ(
    options.memory_.page_pool_size_mb_per_node_,
    child_options.memory_.page_pool_size_mb_per_node_);


  EXPECT_NE(nullptr, child.get_node_memory(0));
  NodeMemoryAnchors* node_anchors = child.get_node_memory_anchors(0);
  EXPECT_NE(nullptr, node_anchors->child_status_memory_);
  EXPECT_NE(nullptr, node_anchors->logger_memories_);
  for (uint16_t i = 0; i < options.log_.loggers_per_node_; ++i) {
    EXPECT_NE(nullptr, node_anchors->logger_memories_[i]);
  }
  EXPECT_NE(nullptr, node_anchors->volatile_pool_status_);
  EXPECT_NE(nullptr, node_anchors->proc_manager_memory_);
  EXPECT_NE(nullptr, node_anchors->proc_memory_);
  EXPECT_NE(nullptr, node_anchors->proc_name_sort_memory_);
  for (uint16_t i = 0; i < options.thread_.thread_count_per_group_; ++i) {
    ThreadMemoryAnchors* thread_anchors_ = node_anchors->thread_anchors_ + i;
    EXPECT_NE(nullptr, thread_anchors_->thread_memory_);
    EXPECT_NE(nullptr, thread_anchors_->task_input_memory_);
    EXPECT_NE(nullptr, thread_anchors_->task_output_memory_);
    EXPECT_NE(nullptr, thread_anchors_->mcs_ww_lock_memories_);
  }

  child.deallocate_shared_memories();
  repo.deallocate_shared_memories();
}


TEST(SharedMemoryRepoTest, Boundary) {
  SharedMemoryRepo repo;
  EngineOptions options = get_tiny_options();

  options.memory_.page_pool_size_mb_per_node_ = 32;
  options.snapshot_.log_reducer_buffer_mb_ = 16;
  Upid pid = ::getpid();
  Eid eid = 1236;
  EXPECT_FALSE(repo.allocate_shared_memories(pid, eid, options).is_error());

  void* pool_memory = repo.get_volatile_pool(0);
  std::memset(pool_memory, 0, 32 << 20);  // especially interesting with valgrind

  void* reducer_memory_0 = repo.get_node_memory_anchors(0)->log_reducer_buffers_[0];
  void* reducer_memory_1 = repo.get_node_memory_anchors(0)->log_reducer_buffers_[1];
  std::memset(reducer_memory_0, 0, 8 << 20);  // especially interesting with valgrind
  std::memset(reducer_memory_1, 0, 8 << 20);  // especially interesting with valgrind
  uintptr_t ad0 = reinterpret_cast<uintptr_t>(reducer_memory_0);
  uintptr_t ad1 = reinterpret_cast<uintptr_t>(reducer_memory_1);
  EXPECT_EQ(ad1, ad0 + (8ULL << 20));


  repo.deallocate_shared_memories();
}

}  // namespace soc
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SharedMemoryRepoTest, foedus.soc);
