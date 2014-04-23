/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_TEST_COMMON_HPP_
#define FOEDUS_TEST_COMMON_HPP_
#include <foedus/engine_options.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/path.hpp>
#include <sstream>
#include <string>

namespace foedus {
    /**
     * Constructs an EngineOption so that all file paths are unique random.
     * This makes it possible to run an arbitrary number of tests in parallel.
     */
    EngineOptions get_randomized_paths(int logger_count, int snapshot_folder_count) {
        EngineOptions options;
        std::string uniquefier = fs::unique_path("%%%%_%%%%_%%%%_%%%%").string();
        options.log_.log_paths_.clear();
        for (int i = 0; i < logger_count; ++i) {
            std::stringstream str;
            str << "test_logs/" << uniquefier << "_" << i << ".log";
            options.log_.log_paths_.push_back(str.str());
        }

        options.snapshot_.folder_paths_.clear();
        for (int i = 0; i < snapshot_folder_count; ++i) {
            std::stringstream str;
            str << "test_snapshots/" << uniquefier << "_" << i;
            options.snapshot_.folder_paths_.push_back(str.str());
        }

        options.savepoint_.savepoint_path_ = std::string("test_savepoints/") + uniquefier + ".xml";

        return options;
    }

    /**
     * Use this for most testcases to reduce test execution time.
     */
    EngineOptions get_tiny_options() {
        EngineOptions options = get_randomized_paths(1, 1);
        options.memory_.page_pool_size_mb_ = 2;
        options.memory_.private_page_pool_initial_grab_ = 32;
        options.thread_.group_count_ = 1;
        options.thread_.thread_count_per_group_ = 2;
        return options;
    }
}  // namespace foedus

#endif  // FOEDUS_TEST_COMMON_HPP_
