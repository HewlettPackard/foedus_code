/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
4 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/snapshot/log_gleaner_impl.hpp>
#include <foedus/snapshot/log_mapper_impl.hpp>
#include <foedus/snapshot/log_reducer_impl.hpp>
#include <foedus/snapshot/snapshot.hpp>
#include <foedus/thread/stoppable_thread_impl.hpp>
#include <glog/logging.h>
#include <chrono>
#include <ostream>
#include <sstream>
#include <string>
namespace foedus {
namespace snapshot {

ErrorStack LogGleaner::initialize_once() {
    LOG(INFO) << "Initializing Log Gleaner";

    const EngineOptions& options = engine_->get_options();
    const thread::ThreadGroupId numa_nodes = options.thread_.group_count_;
    for (thread::ThreadGroupId node = 0; node < numa_nodes; ++node) {
        for (uint16_t ordinal = 0; ordinal < options.log_.loggers_per_node_; ++ordinal) {
            log::LoggerId logger_id = options.log_.loggers_per_node_ * node + ordinal;
            mappers_.push_back(new LogMapper(engine_, this, logger_id, node));
        }

        for (uint16_t ordinal = 0; ordinal < options.snapshot_.partitions_per_node_; ++ordinal) {
            PartitionId partition_id = options.snapshot_.partitions_per_node_ * node + ordinal;
            reducers_.push_back(new LogReducer(engine_, this, partition_id, node));
        }
    }

    return kRetOk;
}

ErrorStack LogGleaner::uninitialize_once() {
    LOG(INFO) << "Uninitializing Log Gleaner";
    ErrorStackBatch batch;
    // note: at this point, mappers_/reducers_ are most likely already stopped (unless there were
    // unexpected errors).
    batch.uninitialize_and_delete_all(&mappers_);
    batch.uninitialize_and_delete_all(&reducers_);
    return SUMMARIZE_ERROR_BATCH(batch);
}

void LogGleaner::cancel_reducers_mappers() {
}


ErrorStack LogGleaner::execute(thread::StoppableThread* /*snapshot_thread*/) {
    LOG(INFO) << "gleaner_thread_ starts running: " << *this;

    // first, initialize mappers and reducers
    for (LogMapper* mapper : mappers_) {
        CHECK_ERROR(mapper->initialize());
    }
    for (LogReducer* reducer : reducers_) {
        CHECK_ERROR(reducer->initialize());
    }
    LOG(INFO) << "Initialized mappers and reducers: " << *this;

    // while (!snapshot_thread->sleep()) {
    // }

    LOG(INFO) << "gleaner_thread_ stopping.. cancelling reducers and mappers: " << *this;
    cancel_reducers_mappers();
    LOG(INFO) << "gleaner_thread_ ends: " << *this;

    return kRetOk;
}

std::string LogGleaner::to_string() const {
    std::stringstream stream;
    stream << *this;
    return stream.str();
}
std::ostream& operator<<(std::ostream& o, const LogGleaner& v) {
    o << "<LogGleaner>"
        << *v.snapshot_
        << "<Mappers>";
    for (auto mapper : v.mappers_) {
        o << *mapper;
    }
    o << "</Mappers>"
        << "<Reducers>";
    for (auto reducer : v.reducers_) {
        o << *reducer;
    }
    o << "</Reducers>"
        << "</LogGleaner>";
    return o;
}


}  // namespace snapshot
}  // namespace foedus
