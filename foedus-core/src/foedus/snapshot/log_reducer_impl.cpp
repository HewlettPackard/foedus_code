/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
4 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <foedus/engine.hpp>
#include <foedus/epoch.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/snapshot/log_reducer_impl.hpp>
#include <glog/logging.h>
#include <chrono>
#include <ostream>
#include <sstream>
#include <string>
namespace foedus {
namespace snapshot {

ErrorStack LogReducer::initialize_once() {
    LOG(INFO) << "Initializing LogReducer-" << id_;
    reducer_thread_.initialize("LogReducer-", id_,
                    std::thread(&LogReducer::handle_reducer, this), std::chrono::milliseconds(10));

    return RET_OK;
}

ErrorStack LogReducer::uninitialize_once() {
    LOG(INFO) << "Uninitializing LogReducer-" << id_;
    ErrorStackBatch batch;
    reducer_thread_.stop();
    return SUMMARIZE_ERROR_BATCH(batch);
}

void LogReducer::handle_reducer() {
}


std::string LogReducer::to_string() const {
    std::stringstream stream;
    stream << *this;
    return stream.str();
}
std::ostream& operator<<(std::ostream& o, const LogReducer& v) {
    o << "<LogReducer>"
        << "<id_>" << v.id_ << "</id_>"
        << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
        << "<reducer_thread_>" << v.reducer_thread_ << "</reducer_thread_>"
        << "</LogReducer>";
    return o;
}


}  // namespace snapshot
}  // namespace foedus
