/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"
#include "foedus/fs/path.hpp"

namespace foedus {
DEFINE_TEST_CASE_PACKAGE(EngineOptionsTest, foedus);

TEST(EngineOptionsTest, Instantiate) {
  EngineOptions options;
}

TEST(EngineOptionsTest, Copy) {
  EngineOptions options;
  EngineOptions options2;
  options2.savepoint_.savepoint_path_ = "aaa";
  EXPECT_NE("aaa", options.savepoint_.savepoint_path_);
  options = options2;
  EXPECT_EQ("aaa", options.savepoint_.savepoint_path_);
  EngineOptions options3(options2);
  EXPECT_EQ("aaa", options3.savepoint_.savepoint_path_);
}

TEST(EngineOptionsTest, Print) {
  EngineOptions options;
  options.log_.folder_path_pattern_ = "~/assd.log";

  std::cout << options << std::endl;
}

TEST(EngineOptionsTest, SaveLoad) {
  EngineOptions options;
  options.log_.folder_path_pattern_ = "~/assd.log";
  options.savepoint_.savepoint_path_ = "aaa";
  options.memory_.interleave_numa_alloc_ = false;
  options.memory_.page_pool_size_mb_per_node_ = 123;

  fs::Path random_path(get_random_name() + ".xml");
  COERCE_ERROR(options.save_to_file(random_path));

  EngineOptions options2;
  COERCE_ERROR(options2.load_from_file(random_path));

  EXPECT_EQ(options.log_.folder_path_pattern_, options2.log_.folder_path_pattern_);
  EXPECT_EQ(options.savepoint_.savepoint_path_, options2.savepoint_.savepoint_path_);
  EXPECT_EQ(options.memory_.interleave_numa_alloc_, options2.memory_.interleave_numa_alloc_);
  EXPECT_EQ(options.memory_.page_pool_size_mb_per_node_,
        options2.memory_.page_pool_size_mb_per_node_);
}

}  // namespace foedus
