/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
  EXPECT_FALSE(repo.allocate_shared_memories(options).is_error());
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
    EXPECT_NE(nullptr, thread_anchors_->mcs_lock_memories_);
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
  EXPECT_FALSE(repo.allocate_shared_memories(options).is_error());

  Upid pid = ::getpid();
  SharedMemoryRepo child;
  EngineOptions child_options;
  EXPECT_FALSE(child.attach_shared_memories(pid, 0, &child_options).is_error());

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
    EXPECT_NE(nullptr, thread_anchors_->mcs_lock_memories_);
  }

  child.deallocate_shared_memories();
  repo.deallocate_shared_memories();
}

}  // namespace soc
}  // namespace foedus
