/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/log/logger_impl.hpp>
#include <foedus/fs/direct_io_file.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/savepoint/savepoint.hpp>
#include <foedus/savepoint/savepoint_manager.hpp>
#include <glog/logging.h>
#include <numa.h>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <string>
#include <sstream>
#include <thread>
#include <vector>
namespace foedus {
namespace log {

Logger::Logger(Engine* engine, LoggerId id,
               const fs::Path &log_path, const std::vector< thread::ThreadId > &assigned_thread_ids)
    : engine_(engine), id_(id), log_path_(log_path), assigned_thread_ids_(assigned_thread_ids) {
    logger_stop_requested_ = false;
    logger_stopped_ = false;
    current_file_ = nullptr;
    oldest_ordinal_ = 0;
    current_ordinal_ = 0;
    logger_buffer_ = nullptr;
    logger_buffer_size_ = 0;
}

fs::Path Logger::construct_suffixed_log_path(LogFileOrdinal ordinal) const {
    std::stringstream path_str;
    path_str << log_path_.string() << "." << ordinal;
    return fs::Path(path_str.str());
}


ErrorStack Logger::initialize_once() {
    numa_node_ = static_cast<int>(thread::decompose_numa_node(assigned_thread_ids_[0]));
    LOG(INFO) << "Initializing Logger-" << id_ << ". assigned " << assigned_thread_ids_.size()
        << " threads, starting from " << assigned_thread_ids_[0] << ", numa_node_=" << numa_node_;

    // this is during initialization. no race.
    const savepoint::Savepoint &savepoint = engine_->get_savepoint_manager().get_savepoint_fast();
    current_file_path_ = construct_suffixed_log_path(savepoint.current_log_files_[id_]);
    current_file_ = new fs::DirectIoFile(current_file_path_,
                                         engine_->get_options().log_.emulation_);
    CHECK_ERROR(current_file_->open(false, true, true, savepoint.empty()));
    logger_thread_ = std::thread(&Logger::handle_logger, this);
    return RET_OK;
}

ErrorStack Logger::uninitialize_once() {
    LOG(INFO) << "Uninitializing Logger-" << id_ << ".";
    ErrorStackBatch batch;
    LOG(INFO) << "Stopping logger_thread...";
    if (!logger_stopped_ && logger_thread_.joinable()) {
        logger_stop_requested_ = true;
        logger_stop_condition_.notify_one();
        logger_thread_.join();
        assert(logger_stopped_);
    }
    if (current_file_) {
        current_file_->close();
        delete current_file_;
        current_file_ = nullptr;
    }
    return RET_OK;
}
void Logger::handle_logger() {
    LOG(INFO) << "Logger-" << id_ << " started. pin on NUMA node-" << numa_node_;
    ::numa_run_on_node(numa_node_);
    while (!logger_stop_requested_) {
        sleep_logger();
    }
    logger_stopped_ = true;
    std::atomic_thread_fence(std::memory_order_release);
    LOG(INFO) << "Logger-" << id_ << " ended.";
}

void Logger::sleep_logger() {
    std::unique_lock<std::mutex> the_lock(logger_mutex_);
    logger_stop_condition_.wait(the_lock);
    if (logger_stop_requested_) {
        return;
    }

    VLOG(1) << "Logger-" << id_ << " woke up.";
}


}  // namespace log
}  // namespace foedus
