/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine_options.hpp>
#include <gtest/gtest.h>
#include <iostream>

TEST(EngineOptionsTest, Instantiate) {
    foedus::EngineOptions options;
}

TEST(EngineOptionsTest, Copy) {
    foedus::EngineOptions options;
    foedus::EngineOptions options2;
    options2.savepoint_.savepoint_path_ = "aaa";
    EXPECT_NE("aaa", options.savepoint_.savepoint_path_);
    options = options2;
    EXPECT_EQ("aaa", options.savepoint_.savepoint_path_);
    foedus::EngineOptions options3(options2);
    EXPECT_EQ("aaa", options3.savepoint_.savepoint_path_);
}

TEST(EngineOptionsTest, Print) {
    foedus::EngineOptions options;
    options.log_.log_paths_.clear();
    options.log_.log_paths_.push_back("~/assd.log");
    options.log_.log_paths_.push_back("/home/sss/ggg/assd.log");

    std::cout << options << std::endl;
}
