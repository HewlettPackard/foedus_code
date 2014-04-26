/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/xct/xct_access.hpp>
#include <foedus/xct/xct_manager_pimpl.hpp>
#include <foedus/xct/xct_options.hpp>
#include <foedus/xct/xct.hpp>
#include <foedus/log/log_type_invoke.hpp>
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/storage/record.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/assert_nd.hpp>
#include <glog/logging.h>
#include <algorithm>
#include <atomic>
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
        ASSERT_ND(epoch_advance_stopped_);
    }
    return RET_OK;
}

ErrorStack XctManagerPimpl::begin_xct(thread::Thread* context) {
    if (context->is_running_xct()) {
        return ERROR_STACK(ERROR_CODE_XCT_ALREADY_RUNNING);
    }
    LOG(INFO) << "Stopping epoch_advance_thread...";
    context->activate_xct();
    return RET_OK;
}

ErrorStack XctManagerPimpl::prepare_commit_xct(thread::Thread* context, Epoch *commit_epoch) {
    if (!context->is_running_xct()) {
        return ERROR_STACK(ERROR_CODE_XCT_NO_XCT);
    }

    *commit_epoch = Epoch();
    bool read_only = context->get_current_xct().get_write_set_size() == 0;
    DLOG(INFO) << "Committing Thread-" << context->get_thread_id() << ", read_only=" << read_only;
    if (!read_only) {
        prepare_commit_xct_lock_phase(context);
    }

    std::atomic_thread_fence(std::memory_order_acq_rel);

    *commit_epoch = global_epoch_;  // serialization point!
    DLOG(INFO) << "Acquired commit epoch " << *commit_epoch;

    std::atomic_thread_fence(std::memory_order_acq_rel);

    bool verified = prepare_commit_xct_verify_phase(context);
    if (verified) {
        if (!read_only) {
            prepare_commit_xct_apply_phase(context, *commit_epoch);  // this also unlocks
            context->get_thread_log_buffer().publish_current_xct_log();
        }
    } else if (!read_only) {
        prepare_commit_xct_unlock(context);
    }

    context->deactivate_xct();
    if (verified) {
        return RET_OK;
    } else {
        DLOG(WARNING) << "Aborting because of contention";
        return ERROR_STACK(ERROR_CODE_XCT_RACE_ABORT);
    }
}

void XctManagerPimpl::prepare_commit_xct_lock_phase(thread::Thread* context) {
    WriteXctAccess* write_set = context->get_current_xct().get_write_set();
    uint32_t        write_set_size = context->get_current_xct().get_write_set_size();
    DLOG(INFO) << "write_set_size=" << write_set_size;
    std::sort(write_set, write_set + write_set_size, WriteXctAccess::compare);
    DLOG(INFO) << "sorted write set";

    // lock them unconditionally. there is no risk of deadlock thanks to the sort.
    // lock bit is the highest bit of ordinal_and_status_.
    for (uint32_t i = 0; i < write_set_size; ++i) {
        write_set[i].record_->owner_id_.lock_unconditional<15>();
    }
    DLOG(INFO) << "locked write set";
}

bool XctManagerPimpl::prepare_commit_xct_verify_phase(thread::Thread* context) {
    const WriteXctAccess*   write_set = context->get_current_xct().get_write_set();
    const uint32_t          write_set_size = context->get_current_xct().get_write_set_size();
    const XctAccess*        read_set = context->get_current_xct().get_read_set();
    const uint32_t          read_set_size = context->get_current_xct().get_read_set_size();
    for (uint32_t i = 0; i < read_set_size; ++i) {
        // The owning transaction has changed.
        // We don't check ordinal here because there is no change we are racing with ourselves.
        if (!read_set[i].observed_owner_id_.compare_epoch_and_thread(
                read_set[i].record_->owner_id_)) {
            DLOG(WARNING) << "read set changed by other transaction. will abort";
            return false;
        }
        // TODO(Hideaki) For data structures that have previous links, we need to check if
        // it's latest. Array doesn't have it.

        if (read_set[i].record_->owner_id_.is_locked<15>()) {
            DLOG(INFO) << "read set contained a locked record. was it myself who locked it?";
            // write set is sorted. so we can do binary search.
            WriteXctAccess dummy;
            dummy.record_ = read_set[i].record_;
            bool found = std::binary_search(write_set, write_set + write_set_size, dummy,
                               WriteXctAccess::compare);
            if (!found) {
                DLOG(WARNING) << "no, not me. will abort";
                return false;
            }
        }
    }

    // TODO(Hideaki) Node set check. Now that we have persistent storages too, we need to also
    // check the latest-ness of pages if we followed a snapshot pointer.
    return true;
}

void XctManagerPimpl::prepare_commit_xct_apply_phase(thread::Thread* context,
                                                     const Epoch &commit_epoch) {
    WriteXctAccess* write_set = context->get_current_xct().get_write_set();
    uint32_t        write_set_size = context->get_current_xct().get_write_set_size();
    DLOG(INFO) << "applying.. write_set_size=" << write_set_size;

    context->get_current_xct().issue_next_id(commit_epoch);

    DLOG(INFO) << "generated new xct id=" << context->get_current_xct().get_id();
    for (uint32_t i = 0; i < write_set_size; ++i) {
        log::invoke_apply_record(
            write_set[i].log_entry_, write_set[i].storage_, write_set[i].record_);
        write_set[i].record_->owner_id_ = context->get_current_xct().get_id();  // this also unlocks
    }
    DLOG(INFO) << "aplied and unlocked write set";
}

void XctManagerPimpl::prepare_commit_xct_unlock(thread::Thread* context) {
    WriteXctAccess* write_set = context->get_current_xct().get_write_set();
    uint32_t        write_set_size = context->get_current_xct().get_write_set_size();
    DLOG(INFO) << "unlocking.. write_set_size=" << write_set_size;
    for (uint32_t i = 0; i < write_set_size; ++i) {
        write_set[i].record_->owner_id_.unlock<15>();
    }
    std::atomic_thread_fence(std::memory_order_release);
    DLOG(INFO) << "unlocked write set";
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
