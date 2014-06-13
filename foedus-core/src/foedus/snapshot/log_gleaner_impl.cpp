/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
4 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/error_stack_batch.hpp>
#include <foedus/snapshot/log_gleaner_impl.hpp>
#include <glog/logging.h>
#include <chrono>
#include <ostream>
#include <sstream>
#include <string>
namespace foedus {
namespace snapshot {

ErrorStack LogGleaner::initialize_once() {
    LOG(INFO) << "Initializing Log Gleaner";
    gleaner_thread_.initialize("LogGleaner",
                    std::thread(&LogGleaner::handle_gleaner, this), std::chrono::milliseconds(10));

    return kRetOk;
}

ErrorStack LogGleaner::uninitialize_once() {
    LOG(INFO) << "Uninitializing Log Gleaner";
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
        << "<gleaner_thread_>" << v.gleaner_thread_ << "</gleaner_thread_>"
        << "<Mappers>"
        << "</Mappers>"
        << "<Reducers>"
        << "</Reducers>"
        << "</LogGleaner>";
    return o;
}


}  // namespace snapshot
}  // namespace foedus
