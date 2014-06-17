/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
4 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <foedus/engine.hpp>
#include <foedus/epoch.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/snapshot/log_gleaner_impl.hpp>
#include <foedus/snapshot/mapreduce_base_impl.hpp>
#include <glog/logging.h>
#include <numa.h>
#include <chrono>
#include <ostream>
#include <sstream>
#include <string>
namespace foedus {
namespace snapshot {

ErrorStack MapReduceBase::initialize_once() {
    LOG(INFO) << "Initializing " << to_string();
    // most of the initialization happens on its own thread (handle)
    thread_.initialize(to_string(),
                    std::thread(&MapReduceBase::handle, this), std::chrono::milliseconds(10));
    return kRetOk;
}

ErrorStack MapReduceBase::uninitialize_once() {
    LOG(INFO) << "Uninitializing " << to_string();
    ErrorStackBatch batch;
    // most of the uninitialization happens on its own thread (handle), but we do it again
    // here in case there was some error.
    LOG(INFO) << "Calling handle_uninitialize at uninitialize_once: " << to_string() << "...";
    batch.emprace_back(handle_uninitialize());
    thread_.stop();
    return SUMMARIZE_ERROR_BATCH(batch);
}

void MapReduceBase::handle() {
    LOG(INFO) << "Reducer started running: " << to_string();
    ::numa_run_on_node(numa_node_);

    LOG(INFO) << "Calling handle_initialize at handle(): " << to_string() << "...";
    ErrorStack init_error = handle_initialize();
    if (init_error.is_error()) {
        LOG(ERROR) << to_string() << " failed to initialize:" << init_error;
        parent_->increment_error_count();
        parent_->wakeup();
    } else {
        LOG(INFO) << to_string() << " initialization done";
        if (parent_->wait_for_next_epoch()) {
            while (!parent_->is_stop_requested()) {
                DVLOG(0) << to_string() << " processing epoch-"
                    << parent_->get_processing_epoch();
                ErrorStack exec_error = handle_epoch();
                if (exec_error.is_error()) {
                    LOG(ERROR) << to_string() << " got an error while processing:" << exec_error;
                    parent_->increment_error_count();
                    parent_->wakeup();
                    break;  // exit now
                }

                DVLOG(0) << to_string() << " processed epoch-"
                    << parent_->get_processing_epoch();
                if (!parent_->wait_for_next_epoch()) {
                    break;
                }
            }
        }
    }

    LOG(INFO) << "Calling handle_uninitialize at handle(): " << to_string() << "...";
    ErrorStack uninit_error = handle_uninitialize();
    if (uninit_error.is_error()) {
        // error while uninitialize doesn't change what's happening. anyway the gleaner is dying.
        LOG(ERROR) << to_string() << " failed to uninitialize:" << uninit_error;
        parent_->increment_error_count();
    }

    parent_->increment_exit_count();
    LOG(INFO) << "Reducer stopped running: " << to_string();
}


}  // namespace snapshot
}  // namespace foedus
