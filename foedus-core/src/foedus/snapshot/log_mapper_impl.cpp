/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
4 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <foedus/engine.hpp>
#include <foedus/epoch.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/snapshot/log_mapper_impl.hpp>
#include <glog/logging.h>
#include <chrono>
#include <ostream>
#include <sstream>
#include <string>
namespace foedus {
namespace snapshot {

ErrorStack LogMapper::initialize_once() {
    LOG(INFO) << "Initializing LogMapper-" << id_;
    mapper_thread_.initialize("LogMapper-", id_,
                    std::thread(&LogMapper::handle_mapper, this), std::chrono::milliseconds(10));

    return kRetOk;
}

ErrorStack LogMapper::uninitialize_once() {
    LOG(INFO) << "Uninitializing LogMapper-" << id_;
    ErrorStackBatch batch;
    mapper_thread_.stop();
    return SUMMARIZE_ERROR_BATCH(batch);
}

void LogMapper::handle_mapper() {
}


std::string LogMapper::to_string() const {
    std::stringstream stream;
    stream << *this;
    return stream.str();
}
std::ostream& operator<<(std::ostream& o, const LogMapper& v) {
    o << "<LogMapper>"
        << "<id_>" << v.id_ << "</id_>"
        << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
        << "<mapper_thread_>" << v.mapper_thread_ << "</mapper_thread_>"
        << "</LogMapper>";
    return o;
}


}  // namespace snapshot
}  // namespace foedus
