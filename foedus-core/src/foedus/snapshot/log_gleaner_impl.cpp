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
    completed_count_.store(0U);
    error_count_.store(0U);
    exit_count_.store(0U);
    processing_epoch_.store(Epoch::kEpochInvalid);

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
    // unexpected errors). We do it again to make sure.
    batch.uninitialize_and_delete_all(&mappers_);
    batch.uninitialize_and_delete_all(&reducers_);
    return SUMMARIZE_ERROR_BATCH(batch);
}

bool LogGleaner::is_stop_requested() const {
    return gleaner_thread_->is_stop_requested();
}
void LogGleaner::wakeup() {
    gleaner_thread_->wakeup();
}


void LogGleaner::cancel_reducers_mappers() {
    // first, request to stop all of them before waiting for them.
    LOG(INFO) << "Requesting mappers/reducers to stop.. " << *this;
    for (LogMapper* mapper : mappers_) {
        if (mapper->is_initialized()) {
            mapper->request_stop();
        } else {
            LOG(WARNING) << "This mapper is not initilized.. During error handling?" << *mapper;
        }
    }
    for (LogReducer* reducer : reducers_) {
        if (reducer->is_initialized()) {
            reducer->request_stop();
        } else {
            LOG(WARNING) << "This reducer is not initilized.. During error handling?" << *reducer;
        }
    }

    LOG(INFO) << "Requested mappers/reducers to stop. Now blocking.." << *this;
    for (LogMapper* mapper : mappers_) {
        if (mapper->is_initialized()) {
            mapper->wait_for_stop();
        }
    }
    for (LogReducer* reducer : reducers_) {
        if (reducer->is_initialized()) {
            reducer->wait_for_stop();
        }
    }
    LOG(INFO) << "All mappers/reducers stopped." << *this;
}


ErrorStack LogGleaner::execute() {
    LOG(INFO) << "gleaner_thread_ starts running: " << *this;
    completed_count_.store(0U);
    error_count_.store(0U);
    exit_count_.store(0U);
    processing_epoch_.store(snapshot_->base_epoch_.value());

    // initialize mappers and reducers. This launches the threads.
    for (LogMapper* mapper : mappers_) {
        CHECK_ERROR(mapper->initialize());
    }
    for (LogReducer* reducer : reducers_) {
        CHECK_ERROR(reducer->initialize());
    }
    // Wait for completion of mapper/reducer initialization.
    LOG(INFO) << "Waiting for completion of mappers and reducers init.. " << *this;
    while (!gleaner_thread_->sleep()) {
        ASSERT_ND(completed_count_ <= mappers_.size() + reducers_.size());
        if (completed_count_ == mappers_.size() + reducers_.size()) {
            break;
        }
    }

    LOG(INFO) << "Initialized mappers and reducers: " << *this;

    // let mappers/reducers work for each
    while (!is_stop_requested() && error_count_ == 0) {
        // advance the processing epoch and wake up all mappers/reducers
        Epoch next_epoch = get_next_processing_epoch();
        if (next_epoch > snapshot_->valid_until_epoch_) {
            // okay, we already processed all logs
            break;
        }
        LOG(INFO) << "Starting a new map/reduce phase for epoch " << next_epoch << ": " << *this;
        completed_count_.store(0U);
        processing_epoch_.store(next_epoch.value());
        processing_epoch_cond_for(next_epoch).notify_all();

        // then, wait until all mappers/reducers are done for this epoch
        while (!gleaner_thread_->sleep() && error_count_ == 0) {
            if (is_stop_requested()) {
                break;
            }
            ASSERT_ND(get_processing_epoch() == snapshot_->valid_until_epoch_ || exit_count_ == 0U);
            ASSERT_ND(completed_count_ <= mappers_.size() + reducers_.size());
            if (completed_count_ == mappers_.size() + reducers_.size()) {
                break;
            }
        }
    }

    if (get_processing_epoch().is_valid()
            && get_processing_epoch() < snapshot_->valid_until_epoch_) {
        LOG(WARNING) << "gleaner_thread_ stopped without completion. cancelled? " << *this;
    }

    if (error_count_ > 0) {
        LOG(ERROR) << "Some mapper/reducer got an error. " << *this;
    }

    LOG(INFO) << "gleaner_thread_ stopping.. cancelling reducers and mappers: " << *this;
    cancel_reducers_mappers();
    ASSERT_ND(exit_count_.load() == mappers_.size() + reducers_.size());
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
        << *v.snapshot_ << *v.gleaner_thread_
        << "<completed_count_>" << v.completed_count_ << "</completed_count_>"
        << "<completed_mapper_count_>" << v.completed_mapper_count_ << "</completed_mapper_count_>"
        << "<error_count_>" << v.error_count_ << "</error_count_>"
        << "<exit_count_>" << v.exit_count_ << "</exit_count_>"
        << "<processing_epoch_>" << v.get_processing_epoch() << "</processing_epoch_>";
    o << "<Mappers>";
    for (auto mapper : v.mappers_) {
        o << *mapper;
    }
    o << "</Mappers>";
    o << "<Reducers>";
    for (auto reducer : v.reducers_) {
        o << *reducer;
    }
    o << "</Reducers>";
    o << "</LogGleaner>";
    return o;
}


}  // namespace snapshot
}  // namespace foedus
