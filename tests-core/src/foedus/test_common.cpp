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

    EngineOptions get_randomized_paths() {
        EngineOptions options;
        std::string uniquefier = get_random_name();
        std::cout << "test uniquefier=" << uniquefier << std::endl;
        {
            std::stringstream str;
            str << "tmp_logs/" << uniquefier << "/node_$NODE$/logger_$LOGGER$";
            options.log_.folder_path_pattern_ = str.str();
        }

        {
            std::stringstream str;
            str << "tmp_snapshots/" << uniquefier << "/node_$NODE$/partition_$PARTITION$";
            options.snapshot_.folder_path_pattern_ = str.str();
        }

        options.savepoint_.savepoint_path_ = std::string("tmp_savepoints/") + uniquefier + ".xml";

        return options;
    }

    EngineOptions get_tiny_options() {
        EngineOptions options = get_randomized_paths();
        options.debugging_.debug_log_min_threshold_ = debugging::DebuggingOptions::DEBUG_LOG_INFO;
        options.debugging_.debug_log_stderr_threshold_
            = debugging::DebuggingOptions::DEBUG_LOG_INFO;
        options.debugging_.verbose_log_level_ = 1;
        options.debugging_.verbose_modules_ = "*";

        options.log_.log_buffer_kb_ = 1 << 8;
        options.memory_.page_pool_size_mb_per_node_ = 2;
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
        fs::remove(fs::Path("tmp_logs"));
        fs::remove(fs::Path("tmp_snapshots"));
    }

}  // namespace foedus
