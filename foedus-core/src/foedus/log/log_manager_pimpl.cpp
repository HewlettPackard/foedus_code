/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/log/log_id.hpp>
#include <foedus/log/log_manager_pimpl.hpp>
#include <foedus/log/log_options.hpp>
#include <foedus/log/logger_impl.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/engine_options.hpp>
#include <glog/logging.h>
#include <cassert>
#include <string>
#include <vector>
namespace foedus {
namespace log {
ErrorStack LogManagerPimpl::initialize_once() {
    groups_ = engine_->get_options().thread_.group_count_;
    const std::vector< std::string > &log_paths = engine_->get_options().log_.log_paths_;
    const LoggerId total_loggers = log_paths.size();
    const uint16_t total_threads = engine_->get_options().thread_.thread_count_per_group_ * groups_;
    LOG(INFO) << "Initializing LogManager. #loggers=" << total_loggers
        << ", #NUMA-nodes=" << static_cast<int>(groups_) << ", #total_threads=" << total_threads;
    if (!engine_->get_thread_pool().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    // see comments in LogOptions#log_paths_
    if (total_loggers % groups_ != 0 || total_threads % total_loggers != 0
        || total_loggers > total_threads) {
        return ERROR_STACK(ERROR_CODE_LOG_INVALID_LOGGER_COUNT);
    }

    // evenly distribute loggers to NUMA nodes, then to cores.
    const uint16_t loggers_per_group = total_loggers / groups_;
    const uint16_t cores_per_logger = total_threads / total_loggers;
    LoggerId current_logger_id = 0;
    for (thread::ThreadGroupId group = 0; group < groups_; ++group) {
        thread::ThreadLocalOrdinal current_ordinal = 0;
        for (auto j = 0; j < loggers_per_group; ++j) {
            std::vector< thread::ThreadId > assigned_thread_ids;
            for (auto k = 0; k < cores_per_logger; ++k) {
                assigned_thread_ids.push_back(thread::compose_thread_id(group, current_ordinal));
                current_ordinal++;
            }
            Logger* logger = new Logger(engine_, current_logger_id,
                fs::Path(log_paths[current_logger_id]), assigned_thread_ids);
            CHECK_OUTOFMEMORY(logger);
            loggers_.push_back(logger);
            CHECK_ERROR(logger->initialize());
            ++current_logger_id;
        }
        assert(current_ordinal == engine_->get_options().thread_.thread_count_per_group_);
    }
    assert(current_logger_id == total_loggers);
    return RET_OK;
}

ErrorStack LogManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing LogManager..";
    ErrorStackBatch batch;
    if (!engine_->get_thread_pool().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }
    batch.uninitialize_and_delete_all(&loggers_);
    return RET_OK;
}

}  // namespace log
}  // namespace foedus
