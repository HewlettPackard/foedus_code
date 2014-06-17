/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
4 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <foedus/engine.hpp>
#include <foedus/epoch.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/snapshot/log_gleaner_impl.hpp>
#include <foedus/snapshot/log_reducer_impl.hpp>
#include <glog/logging.h>
#include <numa.h>
#include <chrono>
#include <ostream>
#include <sstream>
#include <string>
namespace foedus {
namespace snapshot {

ErrorStack LogReducer::initialize_once() {
    LOG(INFO) << "Initializing LogReducer-" << id_;
    // most of the initialization happens on its own thread (handle)
    reducer_thread_.initialize("LogReducer-", id_,
                    std::thread(&LogReducer::handle, this), std::chrono::milliseconds(10));
    return kRetOk;
}

ErrorStack LogReducer::uninitialize_once() {
    LOG(INFO) << "Uninitializing LogReducer-" << id_;
    ErrorStackBatch batch;
    // most of the uninitialization happens on its own thread (handle), but we do it again
    // here in case there was some error.
    reducer_thread_.stop();
    return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack LogReducer::handle_initialize() {
    LOG(INFO) << "handle_initialize LogReducer-" << id_;
    return kRetOk;
}

ErrorStack LogReducer::handle_uninitialize() {
    LOG(INFO) << "handle_uninitialize LogReducer-" << id_;
    ErrorStackBatch batch;
    return SUMMARIZE_ERROR_BATCH(batch);
}


void LogReducer::handle() {
    LOG(INFO) << "Reducer started running: LogReducer-" << id_;
    ::numa_run_on_node(numa_node_);

    ErrorStack init_error = handle_initialize();
    if (init_error.is_error()) {
        LOG(ERROR) << "LogReducer-" << id_ << " failed to initialize:" << init_error;
        ++parent_->error_count_;
        parent_->gleaner_thread_->wakeup();
    } else {
        LOG(INFO) << "LogReducer-" << id_ << " initialization done";
        if (parent_->wait_for_next_epoch()) {
            while (!parent_->is_stop_requested()) {
                DVLOG(0) << "LogReducer-" << id_ << " processing epoch-"
                    << parent_->get_processing_epoch();
                ErrorStack exec_error = handle_epoch();
                if (exec_error.is_error()) {
                    LOG(ERROR) << "LogReducer-" << id_
                        << " got an error while processing:" << exec_error;
                    ++parent_->error_count_;
                    parent_->gleaner_thread_->wakeup();
                    break;  // exit now
                }

                DVLOG(0) << "LogReducer-" << id_ << " processed epoch-"
                    << parent_->get_processing_epoch();
                if (!parent_->wait_for_next_epoch()) {
                    break;
                }
            }
        }
    }

    ErrorStack uninit_error = handle_uninitialize();
    if (uninit_error.is_error()) {
        // error while uninitialize doesn't change what's happening. anyway the gleaner is dying.
        LOG(ERROR) << "LogReducer-" << id_ << " failed to uninitialize:" << uninit_error;
        ++parent_->error_count_;
    }

    ++parent_->exit_count_;
    LOG(INFO) << "Reducer stopped running: LogReducer-" << id_;
}

ErrorStack LogReducer::handle_epoch() {
    // Epoch epoch = parent_->get_processing_epoch();
    return kRetOk;
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
