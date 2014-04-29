/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/log/logger_impl.hpp>
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/direct_io_file.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/savepoint/savepoint.hpp>
#include <foedus/savepoint/savepoint_manager.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread_pool_pimpl.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/memory/numa_node_memory.hpp>
#include <foedus/assert_nd.hpp>
#include <glog/logging.h>
#include <numa.h>
#include <atomic>
#include <string>
#include <sstream>
#include <thread>
#include <vector>
namespace foedus {
namespace log {
fs::Path Logger::construct_suffixed_log_path(LogFileOrdinal ordinal) const {
    std::stringstream path_str;
    path_str << log_path_.string() << "." << ordinal;
    return fs::Path(path_str.str());
}

ErrorStack Logger::initialize_once() {
    // clear all variables
    current_file_ = nullptr;
    oldest_ordinal_ = 0;
    current_ordinal_ = 0;
    node_memory_ = nullptr;
    logger_buffer_ = nullptr;
    logger_buffer_size_ = 0;
    numa_node_ = static_cast<int>(thread::decompose_numa_node(assigned_thread_ids_[0]));
    LOG(INFO) << "Initializing Logger-" << id_ << ". assigned " << assigned_thread_ids_.size()
        << " threads, starting from " << assigned_thread_ids_[0] << ", numa_node_="
        << static_cast<int>(numa_node_);

    // this is during initialization. no race.
    const savepoint::Savepoint &savepoint = engine_->get_savepoint_manager().get_savepoint_fast();
    current_file_path_ = construct_suffixed_log_path(savepoint.current_log_files_[id_]);
    // open the log file
    current_file_ = new fs::DirectIoFile(current_file_path_,
                                         engine_->get_options().log_.emulation_);
    CHECK_ERROR(current_file_->open(false, true, true, savepoint.empty()));
    uint64_t desired_length = savepoint.current_log_files_offset_durable_[id_];
    uint64_t current_length = fs::file_size(current_file_path_);
    if (desired_length < fs::file_size(current_file_path_)) {
        // there are non-durable regions as an incomplete remnant of previous execution.
        // probably there was a crash. in this case, we discard the non-durable regions.
        LOG(ERROR) << "Logger-" << id_ << "'s log file has a non-durable region. Probably there"
            << " was a crash. Will truncate it to " << desired_length << " from " << current_length;
        CHECK_ERROR(current_file_->truncate(desired_length, true));  // also sync right now
    }

    // which threads are assigned to me?
    for (auto thread_id : assigned_thread_ids_) {
        assigned_threads_.push_back(
            engine_->get_thread_pool().get_pimpl()->get_thread(thread_id));
    }

    // grab a buffer to do file I/O
    node_memory_ = engine_->get_memory_manager().get_node_memory(numa_node_);
    logger_buffer_ = node_memory_->get_logger_buffer_memory_piece(id_);
    logger_buffer_size_ = node_memory_->get_logger_buffer_memory_size_per_core();
    LOG(INFO) << "Logger-" << id_ << " grabbed a I/O buffer. size=" << logger_buffer_size_;

    // log file and buffer prepared. let's launch the logger thread
    logger_thread_.initialize("Logger-", id_,
                    std::thread(&Logger::handle_logger, this), std::chrono::milliseconds(50));
    return RET_OK;
}

ErrorStack Logger::uninitialize_once() {
    LOG(INFO) << "Uninitializing Logger-" << id_ << ".";
    ErrorStackBatch batch;
    logger_thread_.stop();
    if (current_file_) {
        current_file_->close();
        delete current_file_;
        current_file_ = nullptr;
    }
    return RET_OK;
}
void Logger::handle_logger() {
    LOG(INFO) << "Logger-" << id_ << " started. pin on NUMA node-" << static_cast<int>(numa_node_);
    ::numa_run_on_node(numa_node_);
    while (!logger_thread_.sleep()) {
        VLOG(1) << "Logger-" << id_ << " doing the job..";
        std::atomic_thread_fence(std::memory_order_acquire);
        for (thread::Thread* the_thread : assigned_threads_) {
            ThreadLogBuffer& buffer = the_thread->get_thread_log_buffer();
            if (buffer.get_offset_head() == buffer.get_offset_durable()) {
                VLOG(1) << "Thread-" << the_thread->get_thread_id() << " has no log to flush.";
                continue;
            }
        }
    }
    LOG(INFO) << "Logger-" << id_ << " ended.";
}

}  // namespace log
}  // namespace foedus
