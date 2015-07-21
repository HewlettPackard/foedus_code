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

#include <string>

#include "foedus/test_common.hpp"
#include "foedus/log/log_options.hpp"
/**
 * @file test_log_options.cpp
 * Testcases for LogOptions.
 */
namespace foedus {
namespace log {
DEFINE_TEST_CASE_PACKAGE(LogOptionsTest, foedus.log);

TEST(LogOptionsTest, NodePattern) {
  LogOptions options;
  options.folder_path_pattern_ = "/log/node_$NODE$/logger";
  EXPECT_EQ(std::string("/log/node_1/logger"), options.convert_folder_path_pattern(1, 0));
  EXPECT_EQ(std::string("/log/node_2/logger"), options.convert_folder_path_pattern(2, 1));
  EXPECT_EQ(std::string("/log/node_0/logger"), options.convert_folder_path_pattern(0, 3));
}

TEST(LogOptionsTest, LoggerPattern) {
  LogOptions options;
  options.folder_path_pattern_ = "/log/node/logger_$LOGGER$";
  EXPECT_EQ(std::string("/log/node/logger_0"), options.convert_folder_path_pattern(1, 0));
  EXPECT_EQ(std::string("/log/node/logger_1"), options.convert_folder_path_pattern(2, 1));
  EXPECT_EQ(std::string("/log/node/logger_3"), options.convert_folder_path_pattern(0, 3));
}

TEST(LogOptionsTest, BothPattern) {
  LogOptions options;
  options.folder_path_pattern_ = "/log/node_$NODE$/logger_$LOGGER$";
  EXPECT_EQ(std::string("/log/node_1/logger_0"), options.convert_folder_path_pattern(1, 0));
  EXPECT_EQ(std::string("/log/node_2/logger_1"), options.convert_folder_path_pattern(2, 1));
  EXPECT_EQ(std::string("/log/node_0/logger_3"), options.convert_folder_path_pattern(0, 3));
}

TEST(LogOptionsTest, NonePattern) {
  LogOptions options;
  options.folder_path_pattern_ = "/log/node/logger";
  EXPECT_EQ(std::string("/log/node/logger"), options.convert_folder_path_pattern(1, 0));
  EXPECT_EQ(std::string("/log/node/logger"), options.convert_folder_path_pattern(2, 1));
  EXPECT_EQ(std::string("/log/node/logger"), options.convert_folder_path_pattern(0, 3));
}

}  // namespace log
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(LogOptionsTest, foedus.log);
