/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/xct/xct_manager_pimpl.hpp>
#include <foedus/xct/xct_options.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/engine_options.hpp>
#include <glog/logging.h>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <thread>
namespace foedus {
namespace xct {
XctManagerPimpl::XctManagerPimpl(Engine* engine) : engine_(engine) {
    epoch_advance_stop_requested_ = false;
    epoch_advance_stopped_ = false;
}

ErrorStack XctManagerPimpl::initialize_once() {
    LOG(INFO) << "Initializing XctManager..";
    if (!engine_->get_storage_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    epoch_advance_thread_ = std::thread(&XctManagerPimpl::handle_epoch_advance, this);
    return RET_OK;
}

ErrorStack XctManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing XctManager..";
    ErrorStackBatch batch;
    if (!engine_->get_storage_manager().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }
    LOG(INFO) << "Stopping epoch_advance_thread...";
    if (!epoch_advance_stopped_ && epoch_advance_thread_.joinable()) {
        epoch_advance_stop_requested_ = true;
        epoch_advance_stop_condition_.notify_one();
        epoch_advance_thread_.join();
        assert(epoch_advance_stopped_);
    }
    return RET_OK;
}

ErrorStack XctManagerPimpl::begin_xct(thread::Thread* context) {
    if (context->is_running_xct()) {
        return ERROR_STACK(ERROR_CODE_XCT_ALREADY_RUNNING);
    }
    context->activate_xct();
    return RET_OK;
}
ErrorStack XctManagerPimpl::commit_xct(thread::Thread* context) {
    if (!context->is_running_xct()) {
        return ERROR_STACK(ERROR_CODE_XCT_NO_XCT);
    }
    // TODO(Hideaki) Implement
    context->deactivate_xct();
    return RET_OK;
}
ErrorStack XctManagerPimpl::abort_xct(thread::Thread* context) {
    if (!context->is_running_xct()) {
        return ERROR_STACK(ERROR_CODE_XCT_NO_XCT);
    }
    // TODO(Hideaki) Implement
    context->deactivate_xct();
    return RET_OK;
}

void XctManagerPimpl::handle_epoch_advance() {
    const uint32_t interval_ms = engine_->get_options().xct_.epoch_advance_interval_ms_;
    LOG(INFO) << "epoch_advance_thread started. interval_ms=" << interval_ms;
    while (!epoch_advance_stop_requested_) {
        std::unique_lock<std::mutex> the_lock(epoch_advance_mutex_);
        epoch_advance_stop_condition_.wait_for(the_lock, std::chrono::milliseconds(interval_ms));
        if (epoch_advance_stop_requested_) {
            break;
        }

        VLOG(1) << "epoch_advance_thread woke up. current global_epoch_=" << global_epoch_;
        // TODO(Hideaki) Must check long-running transactions
        global_epoch_.increment();
    }
    epoch_advance_stopped_ = true;
    std::atomic_thread_fence(std::memory_order_release);
    LOG(INFO) << "epoch_advance_thread ended.";
}

}  // namespace xct
}  // namespace foedus
