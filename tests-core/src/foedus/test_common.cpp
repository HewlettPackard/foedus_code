/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/path.hpp>
#include <sstream>
#include <iostream>
#include <string>
#include <vector>

namespace foedus {
    std::string get_random_name() {
        return fs::unique_name("%%%%_%%%%_%%%%_%%%%");
    }

    EngineOptions get_randomized_paths(int logger_count, int snapshot_folder_count) {
        EngineOptions options;
        std::string uniquefier = get_random_name();
        std::cout << "test uniquefier=" << uniquefier << std::endl;
        options.log_.log_paths_.clear();
        for (int i = 0; i < logger_count; ++i) {
            std::stringstream str;
            str << "tmp_logs/" << uniquefier << "_" << i << ".log";
            options.log_.log_paths_.push_back(str.str());
        }

        options.snapshot_.folder_paths_.clear();
        for (int i = 0; i < snapshot_folder_count; ++i) {
            std::stringstream str;
            str << "tmp_snapshots/" << uniquefier << "_" << i;
            options.snapshot_.folder_paths_.push_back(str.str());
        }

        options.savepoint_.savepoint_path_ = std::string("tmp_savepoints/") + uniquefier + ".xml";

        return options;
    }

    EngineOptions get_tiny_options() {
        EngineOptions options = get_randomized_paths(1, 1);
        options.debugging_.debug_log_min_threshold_ = debugging::DebuggingOptions::DEBUG_LOG_INFO;
        options.debugging_.debug_log_stderr_threshold_
            = debugging::DebuggingOptions::DEBUG_LOG_INFO;
        options.debugging_.verbose_log_level_ = 1;
        options.debugging_.verbose_modules_ = "*";

        options.log_.log_buffer_kb_ = 1 << 8;
        options.memory_.page_pool_size_mb_ = 2;
        options.memory_.private_page_pool_initial_grab_ = 32;
        options.thread_.group_count_ = 1;
        options.thread_.thread_count_per_group_ = 2;
        return options;
    }

    void remove_files_start_with(const fs::Path &folder, const fs::Path &prefix) {
        if (fs::exists(folder)) {
            std::vector< fs::Path > child_paths(folder.child_paths());
            for (fs::Path child : child_paths) {
                if (child.string().find(prefix.string()) == 0) {
                    fs::remove(child);
                }
            }
        }
    }
    void cleanup_test(const EngineOptions& options) {
        fs::remove(fs::Path(options.savepoint_.savepoint_path_));
        for (std::string log_path : options.log_.log_paths_) {
            fs::Path folder = fs::Path(log_path).parent_path();
            remove_files_start_with(folder, fs::Path(log_path));
        }
        /*
        TODO(Hideaki) When snapshot is implemented
        for (std::string snapshot_folder : options.snapshot_.folder_paths_) {
            fs::Path folder(snapshot_folder);
            fs::remove_all(folder);
        }
        */
    }

}  // namespace foedus
