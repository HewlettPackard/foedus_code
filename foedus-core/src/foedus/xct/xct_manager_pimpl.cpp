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
#include <chrono>
#include <thread>
namespace foedus {
namespace xct {
ErrorStack XctManagerPimpl::initialize_once() {
    LOG(INFO) << "Initializing XctManager..";
    if (!engine_->get_storage_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    current_global_epoch_ = Epoch(1);
    durable_global_epoch_ = Epoch(0);
    epoch_advance_thread_.initialize("epoch_advance_thread",
        std::thread(&XctManagerPimpl::handle_epoch_advance, this),
        std::chrono::milliseconds(engine_->get_options().xct_.epoch_advance_interval_ms_));
    return RET_OK;
}

ErrorStack XctManagerPimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing XctManager..";
    ErrorStackBatch batch;
    if (!engine_->get_storage_manager().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }
    epoch_advance_thread_.stop();
    return RET_OK;
}

void XctManagerPimpl::handle_epoch_advance() {
    LOG(INFO) << "epoch_advance_thread started.";
    while (!epoch_advance_thread_.sleep()) {
        VLOG(1) << "epoch_advance_thread. current_global_epoch_=" << current_global_epoch_
            << ", durable_global_epoch_=" << durable_global_epoch_;
        // TODO(Hideaki) Must check long-running transactions
        ++current_global_epoch_;
        current_global_epoch_advanced_.notify_all();
        // TODO(Hideaki) Must check loggers, and update savepoint
        ++durable_global_epoch_;
        durable_global_epoch_advanced_.notify_all();
    }
    LOG(INFO) << "epoch_advance_thread ended.";
}

void XctManagerPimpl::advance_current_global_epoch() {
    Epoch now = current_global_epoch_;
    LOG(INFO) << "Requesting to immediately advance epoch. current_global_epoch_=" << now << "...";
    while (now <= current_global_epoch_) {
        epoch_advance_thread_.wakeup();  // hurrrrry up!
        std::unique_lock<std::mutex> the_lock(current_global_epoch_advanced_mutex_);
        current_global_epoch_advanced_.wait(the_lock);
    }

    LOG(INFO) << "epoch advanced. current_global_epoch_=" << current_global_epoch_;
}

ErrorCode XctManagerPimpl::wait_for_commit(const Epoch& commit_epoch, int64_t wait_microseconds) {
    std::atomic_thread_fence(std::memory_order_acquire);
    if (wait_microseconds == 0) {
        DVLOG(1) << "was it committed? commit_epoch=" << commit_epoch << ", durable_global_epoch_="
            << durable_global_epoch_;
        if (commit_epoch < durable_global_epoch_) {
            epoch_advance_thread_.wakeup();  // we anyway exit, but let's leave a demand
            return ERROR_CODE_TIMEOUT;
        } else {
            return ERROR_CODE_OK;
        }
    }

    std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now();
    std::chrono::high_resolution_clock::time_point until
        = now + std::chrono::microseconds(wait_microseconds);
    while (commit_epoch < durable_global_epoch_) {
        epoch_advance_thread_.wakeup();  // hurrrrry up!
        std::unique_lock<std::mutex> the_lock(durable_global_epoch_advanced_mutex_);
        if (wait_microseconds > 0) {
            LOG(INFO) << "Synchronously waiting for commit_epoch " << commit_epoch;
            if (durable_global_epoch_advanced_.wait_until(the_lock, until)
                    == std::cv_status::timeout && commit_epoch < durable_global_epoch_) {
                LOG(WARNING) << "Timeout occurs. wait_microseconds=" << wait_microseconds;
                return ERROR_CODE_TIMEOUT;
            }
        } else {
            durable_global_epoch_advanced_.wait(the_lock);
        }
    }

    LOG(INFO) << "durable epoch advanced. durable_global_epoch_=" << durable_global_epoch_;
    return ERROR_CODE_OK;
}


ErrorStack XctManagerPimpl::begin_xct(thread::Thread* context, IsolationLevel isolation_level) {
    Xct& current_xct = context->get_current_xct();
    if (current_xct.is_active()) {
        return ERROR_STACK(ERROR_CODE_XCT_ALREADY_RUNNING);
    }
    DLOG(INFO) << "Began new transaction in thread-" << context->get_thread_id();
    current_xct.activate(isolation_level);
    ASSERT_ND(context->get_thread_log_buffer().get_offset_tail()
        == context->get_thread_log_buffer().get_offset_current_xct_begin());
    return RET_OK;
}

ErrorStack XctManagerPimpl::precommit_xct(thread::Thread* context, Epoch *commit_epoch) {
    Xct& current_xct = context->get_current_xct();
    if (!current_xct.is_active()) {
        return ERROR_STACK(ERROR_CODE_XCT_NO_XCT);
    }

    bool read_only = context->get_current_xct().get_write_set_size() == 0;
    bool success;
    if (read_only) {
        success = precommit_xct_readonly(context, commit_epoch);
    } else {
        success = precommit_xct_readwrite(context, commit_epoch);
    }

    current_xct.deactivate();
    if (success) {
        return RET_OK;
    } else {
        DLOG(WARNING) << "Aborting because of contention";
        context->get_thread_log_buffer().discard_current_xct_log();
        return ERROR_STACK(ERROR_CODE_XCT_RACE_ABORT);
    }
}
bool XctManagerPimpl::precommit_xct_readonly(thread::Thread* context, Epoch *commit_epoch) {
    DLOG(INFO) << "Committing Thread-" << context->get_thread_id() << ", read_only";
    *commit_epoch = Epoch();
    std::atomic_thread_fence(std::memory_order_acquire);  // this is enough for read-only case
    return precommit_xct_verify_readonly(context, commit_epoch);
}

bool XctManagerPimpl::precommit_xct_readwrite(thread::Thread* context, Epoch *commit_epoch) {
    DLOG(INFO) << "Committing Thread-" << context->get_thread_id() << ", read-write";
    precommit_xct_lock(context);  // Phase 1
    std::atomic_thread_fence(std::memory_order_acq_rel);

    *commit_epoch = current_global_epoch_;  // serialization point!
    DLOG(INFO) << "Acquired read-write commit epoch " << *commit_epoch;

    std::atomic_thread_fence(std::memory_order_acq_rel);
    bool verified = precommit_xct_verify_readwrite(context);  // phase 2
    if (verified) {
        precommit_xct_apply(context, *commit_epoch);  // phase 3. this also unlocks
        context->get_thread_log_buffer().publish_current_xct_log(*commit_epoch);  // announce log
    } else {
        precommit_xct_unlock(context);  // just unlock in this case
    }

    return verified;
}

void XctManagerPimpl::precommit_xct_lock(thread::Thread* context) {
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

bool XctManagerPimpl::precommit_xct_verify_readonly(thread::Thread* context, Epoch *commit_epoch) {
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
            DLOG(INFO) << "read set contained a locked record. abort";
            return false;
        }

        // Remembers the highest epoch observed.
        if (*commit_epoch < read_set[i].observed_owner_id_.epoch_) {
            *commit_epoch = read_set[i].observed_owner_id_.epoch_;
        }
    }

    DLOG(INFO) << "Read-only higest epoch observed: " << *commit_epoch;

    // TODO(Hideaki) Node set check. Now that we have persistent storages too, we need to also
    // check the latest-ness of pages if we followed a snapshot pointer.
    return true;
}

bool XctManagerPimpl::precommit_xct_verify_readwrite(thread::Thread* context) {
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

void XctManagerPimpl::precommit_xct_apply(thread::Thread* context,
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

void XctManagerPimpl::precommit_xct_unlock(thread::Thread* context) {
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
    Xct& current_xct = context->get_current_xct();
    if (!current_xct.is_active()) {
        return ERROR_STACK(ERROR_CODE_XCT_NO_XCT);
    }
    current_xct.deactivate();
    context->get_thread_log_buffer().discard_current_xct_log();
    return RET_OK;
}

}  // namespace xct
}  // namespace foedus
