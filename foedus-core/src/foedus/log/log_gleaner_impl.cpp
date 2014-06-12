/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
4 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <foedus/engine.hpp>
#include <foedus/epoch.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/log/log_gleaner_impl.hpp>
#include <foedus/log/log_type.hpp>
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/log/log_manager.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/direct_io_file.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <glog/logging.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <ostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
namespace foedus {
namespace log {

ErrorStack LogGleaner::initialize_once() {
    LOG(INFO) << "Initializing Log Gleaner for Logger-" << logger_id_
        << ", numa_node_=" << static_cast<int>(numa_node_);


    // log file and buffer prepared. let's launch the logger thread
    gleaner_thread_.initialize("LogGleaner-", logger_id_,
                    std::thread(&LogGleaner::handle_gleaner, this), std::chrono::milliseconds(10));

    return RET_OK;
}

ErrorStack LogGleaner::uninitialize_once() {
    LOG(INFO) << "Uninitializing Log Gleaner for Logger-" << logger_id_;
    ErrorStackBatch batch;
    gleaner_thread_.stop();
    return SUMMARIZE_ERROR_BATCH(batch);
}

void LogGleaner::handle_gleaner() {
}


std::string LogGleaner::to_string() const {
    std::stringstream stream;
    stream << *this;
    return stream.str();
}
std::ostream& operator<<(std::ostream& o, const LogGleaner& v) {
    o << "<LogGleaner>"
        << "<logger_id_>" << v.logger_id_ << "</logger_id_>"
        << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
        << "</LogGleaner>";
    return o;
}


}  // namespace log
}  // namespace foedus
