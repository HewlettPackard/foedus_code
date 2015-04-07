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
  EXPECT_NE(fs::FixedPath("aaa"), options.savepoint_.savepoint_path_);
  options = options2;
  EXPECT_EQ(fs::FixedPath("aaa"), options.savepoint_.savepoint_path_);
  EngineOptions options3(options2);
  EXPECT_EQ(fs::FixedPath("aaa"), options3.savepoint_.savepoint_path_);
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

TEST_MAIN_CAPTURE_SIGNALS(EngineOptionsTest, foedus);
