/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/log/log_options.hpp>
#include <gtest/gtest.h>
#include <string>
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
