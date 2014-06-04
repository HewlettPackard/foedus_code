/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/assert_nd.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/xct/xct_access.hpp>
#include <foedus/xct/xct_manager_pimpl.hpp>
#include <foedus/xct/xct_options.hpp>
#include <foedus/xct/xct.hpp>
#include <foedus/log/log_type_invoke.hpp>
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/log/log_manager.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/storage/record.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/savepoint/savepoint_manager.hpp>
#include <foedus/savepoint/savepoint.hpp>
#include <glog/logging.h>
#include <algorithm>
#include <chrono>
#ifndef NDEBUG
#include <set>  // only for debugging
#endif  // NDEBUG
#include <thread>
#include <vector>
namespace foedus {
namespace xct {
ErrorStack XctManagerPimpl::initialize_once() {
    LOG(INFO) << "Initializing XctManager..";
    if (!engine_->get_storage_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    const savepoint::Savepoint &savepoint = engine_->get_savepoint_manager().get_savepoint_fast();
    current_global_epoch_ = savepoint.get_current_epoch();
    ASSERT_ND(current_global_epoch_.is_valid());
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
    // Wait until all the other initializations are done.
    SPINLOCK_WHILE(!epoch_advance_thread_.is_stop_requested() && !is_initialized()) {
        assorted::memory_fence_acquire();
    }
    LOG(INFO) << "epoch_advance_thread now starts processing.";
    while (!epoch_advance_thread_.sleep()) {
        VLOG(1) << "epoch_advance_thread. current_global_epoch_=" << current_global_epoch_;
        ASSERT_ND(current_global_epoch_.is_valid());
        {
            std::unique_lock<std::mutex> guard(current_global_epoch_advanced_mutex_);
            ++current_global_epoch_;
            ASSERT_ND(current_global_epoch_.is_valid());
            current_global_epoch_advanced_.notify_broadcast(guard);
        }
        engine_->get_log_manager().wakeup_loggers();
    }
    LOG(INFO) << "epoch_advance_thread ended.";
}

void XctManagerPimpl::advance_current_global_epoch() {
    Epoch now = current_global_epoch_;
    LOG(INFO) << "Requesting to immediately advance epoch. current_global_epoch_=" << now << "...";
    std::unique_lock<std::mutex> the_lock(current_global_epoch_advanced_mutex_);
    while (now == current_global_epoch_) {
        epoch_advance_thread_.wakeup();  // hurrrrry up!
        current_global_epoch_advanced_.wait(the_lock);
    }

    LOG(INFO) << "epoch advanced. current_global_epoch_=" << current_global_epoch_;
}

ErrorStack XctManagerPimpl::wait_for_commit(Epoch commit_epoch, int64_t wait_microseconds) {
    assorted::memory_fence_acquire();
    if (commit_epoch < current_global_epoch_) {
        epoch_advance_thread_.wakeup();
    }

    CHECK_ERROR(engine_->get_log_manager().wait_until_durable(commit_epoch, wait_microseconds));
    return RET_OK;
}


ErrorStack XctManagerPimpl::begin_xct(thread::Thread* context, IsolationLevel isolation_level) {
    Xct& current_xct = context->get_current_xct();
    if (current_xct.is_active()) {
        return ERROR_STACK(ERROR_CODE_XCT_ALREADY_RUNNING);
    }
    DLOG(INFO) << *context << " Began new transaction";
    current_xct.activate(isolation_level);
    ASSERT_ND(context->get_thread_log_buffer().get_offset_tail()
        == context->get_thread_log_buffer().get_offset_committed());
    ASSERT_ND(current_xct.get_read_set_size() == 0);
    ASSERT_ND(current_xct.get_write_set_size() == 0);
    return RET_OK;
}
ErrorStack XctManagerPimpl::begin_schema_xct(thread::Thread* context) {
    Xct& current_xct = context->get_current_xct();
    if (current_xct.is_active()) {
        return ERROR_STACK(ERROR_CODE_XCT_ALREADY_RUNNING);
    }
    LOG(INFO) << *context << " Began new schema transaction";
    current_xct.activate(SERIALIZABLE, true);
    ASSERT_ND(context->get_thread_log_buffer().get_offset_tail()
        == context->get_thread_log_buffer().get_offset_committed());
    ASSERT_ND(current_xct.get_read_set_size() == 0);
    ASSERT_ND(current_xct.get_write_set_size() == 0);
    return RET_OK;
}


ErrorStack XctManagerPimpl::precommit_xct(thread::Thread* context, Epoch *commit_epoch) {
    Xct& current_xct = context->get_current_xct();
    if (!current_xct.is_active()) {
        return ERROR_STACK(ERROR_CODE_XCT_NO_XCT);
    }

    bool success;
    if (current_xct.is_schema_xct()) {
        success = precommit_xct_schema(context, commit_epoch);
    } else {
        bool read_only = context->get_current_xct().get_write_set_size() == 0;
        if (read_only) {
            success = precommit_xct_readonly(context, commit_epoch);
        } else {
            success = precommit_xct_readwrite(context, commit_epoch);
        }
    }

    current_xct.deactivate();
    if (success) {
        return RET_OK;
    } else {
        DLOG(WARNING) << *context << " Aborting because of contention";
        context->get_thread_log_buffer().discard_current_xct_log();
        return ERROR_STACK(ERROR_CODE_XCT_RACE_ABORT);
    }
}
bool XctManagerPimpl::precommit_xct_readonly(thread::Thread* context, Epoch *commit_epoch) {
    DVLOG(1) << *context << " Committing read_only";
    *commit_epoch = Epoch();
    assorted::memory_fence_acquire();  // this is enough for read-only case
    return precommit_xct_verify_readonly(context, commit_epoch);
}

bool XctManagerPimpl::precommit_xct_readwrite(thread::Thread* context, Epoch *commit_epoch) {
    DVLOG(1) << *context << " Committing read-write";
    precommit_xct_lock(context);  // Phase 1

    // BEFORE the first fence, update the in_commit_log_epoch_ for logger
    Xct::InCommitLogEpochGuard guard(&context->get_current_xct(), current_global_epoch_);

    assorted::memory_fence_acq_rel();

    *commit_epoch = current_global_epoch_;  // serialization point!
    DVLOG(1) << *context << " Acquired read-write commit epoch " << *commit_epoch;

    assorted::memory_fence_acq_rel();
    bool verified = precommit_xct_verify_readwrite(context);  // phase 2
    if (verified) {
        precommit_xct_apply(context, commit_epoch);  // phase 3. this also unlocks
        // announce log AFTER (with fence) apply, because apply sets xct_order in the logs.
        assorted::memory_fence_release();
        context->get_thread_log_buffer().publish_committed_log(*commit_epoch);
    } else {
        precommit_xct_unlock(context);  // just unlock in this case
    }

    return verified;
}
bool XctManagerPimpl::precommit_xct_schema(thread::Thread* context, Epoch* commit_epoch) {
    LOG(INFO) << *context << " committing a schema transaction";

    Xct::InCommitLogEpochGuard guard(&context->get_current_xct(), current_global_epoch_);
    assorted::memory_fence_acq_rel();
    *commit_epoch = current_global_epoch_;  // serialization point!
    LOG(INFO) << *context << " Acquired schema commit epoch " << *commit_epoch;
    assorted::memory_fence_acq_rel();

    Xct& current_xct = context->get_current_xct();
    current_xct.issue_next_id(commit_epoch);
    XctId new_xct_id = current_xct.get_id();

    LOG(INFO) << *context << " schema xct generated new xct id=" << new_xct_id;
    // Unlike usual transactions, schema xcts don't have read/write sets. Just iterate over logs.
    std::vector<char*> logs;
    context->get_thread_log_buffer().list_uncommitted_logs(&logs);
    for (char* entry : logs) {
        log::LogHeader* header = reinterpret_cast<log::LogHeader*>(entry);
        log::LogCode code = header->get_type();
        ASSERT_ND(code != log::LOG_TYPE_INVALID);
        log::LogCodeKind kind = log::get_log_code_kind(code);
        LOG(INFO) << *context << " Applying schema log " << log::get_log_type_name(code)
            << ". kind=" << kind << ", log length=" << header->log_length_;
        if (kind == log::MARKER_LOGS) {
            LOG(INFO) << *context << " Ignored marker log in schema xct's apply";
        } else if (kind == log::ENGINE_LOGS) {
            // engine type log, such as XXX.
            log::invoke_apply_engine(new_xct_id, entry, context);
        } else if (kind == log::STORAGE_LOGS) {
            // storage type log, such as CREATE/DROP STORAGE.
            storage::StorageId storage_id = header->storage_id_;
            LOG(INFO) << *context << " schema xct applying storage-" << storage_id;
            storage::Storage* storage = engine_->get_storage_manager().get_storage(storage_id);
            log::invoke_apply_storage(new_xct_id, entry, context, storage);
        } else {
            // schema xct must not have individual data modification operations.
            LOG(FATAL) << "Unexpected log type for schema xct:" << code;
        }
    }
    LOG(INFO) << *context << " schema xct applied all logs";

    // schema xct doesn't have apply phase because it is separately applied.
    context->get_thread_log_buffer().publish_committed_log(*commit_epoch);

    return true;  // so far scheme xct can always commit
}

void XctManagerPimpl::precommit_xct_lock(thread::Thread* context) {
    Xct& current_xct = context->get_current_xct();
    WriteXctAccess* write_set = current_xct.get_write_set();
    uint32_t        write_set_size = current_xct.get_write_set_size();
    DVLOG(1) << *context << " #write_sets=" << write_set_size << ", addr=" << write_set;

#ifndef NDEBUG
    // DEBUG: check equivalence of records/logs before/after sort
    std::set< storage::Record* > dbg_records;
    std::set< void* > dbg_logs;
    for (uint32_t i = 0; i < write_set_size; ++i) {
        dbg_records.insert(write_set[i].record_);
        dbg_logs.insert(write_set[i].log_entry_);
    }
    ASSERT_ND(dbg_records.size() == write_set_size);
    ASSERT_ND(dbg_logs.size() == write_set_size);
#endif  // NDEBUG

    std::sort(write_set, write_set + write_set_size, WriteXctAccess::compare);
    DVLOG(1) << *context << " sorted write set";

    // lock them unconditionally. there is no risk of deadlock thanks to the sort.
    // lock bit is the highest bit of ordinal_and_status_.
    for (uint32_t i = 0; i < write_set_size; ++i) {
        DVLOG(2) << *context << " Locking " << write_set[i].storage_->get_name()
            << ":" << write_set[i].record_;
        write_set[i].record_->owner_id_.keylock_unconditional();
        ASSERT_ND(write_set[i].record_->owner_id_.is_keylocked());
    }
    DVLOG(1) << *context << " locked write set";

#ifndef NDEBUG
    for (uint32_t i = 0; i < write_set_size; ++i) {
        ASSERT_ND(dbg_records.find(write_set[i].record_) != dbg_records.end());
        ASSERT_ND(dbg_logs.find(write_set[i].log_entry_) != dbg_logs.end());
        ASSERT_ND(write_set[i].record_->owner_id_.is_keylocked());
    }
#endif  // NDEBUG
}

bool XctManagerPimpl::precommit_xct_verify_readonly(thread::Thread* context, Epoch *commit_epoch) {
    Xct& current_xct = context->get_current_xct();
    const XctAccess*        read_set = current_xct.get_read_set();
    const uint32_t          read_set_size = current_xct.get_read_set_size();
    for (uint32_t i = 0; i < read_set_size; ++i) {
        // The owning transaction has changed.
        // We don't check ordinal here because there is no change we are racing with ourselves.
        const XctAccess& access = read_set[i];
        DVLOG(2) << *context << "Verifying " << access.storage_->get_name()
            << ":" << access.record_ << ". observed_xid=" << access.observed_owner_id_
                << ", now_xid=" << access.record_->owner_id_;
        ASSERT_ND(!access.observed_owner_id_.is_keylocked());  // we made it sure when we read.
        if (access.observed_owner_id_.data_ != access.record_->owner_id_.data_) {
            DLOG(WARNING) << *context << " read set changed by other transaction. will abort";
            return false;
        }
        // TODO(Hideaki) For data structures that have previous links, we need to check if
        // it's latest. Array doesn't have it.

        // Remembers the highest epoch observed.
        commit_epoch->store_max(access.observed_owner_id_.get_epoch());
    }

    DVLOG(1) << *context << "Read-only higest epoch observed: " << *commit_epoch;
    if (!commit_epoch->is_valid()) {
        DLOG(INFO) << *context
            << " Read-only higest epoch was empty. The transaction has no read set??";
        // In this case, set already-durable epoch. We don't have to use atomic version because
        // it's just conservatively telling how long it should wait.
        *commit_epoch = Epoch(engine_->get_log_manager().get_durable_global_epoch_nonatomic());
    }

    // TODO(Hideaki) Node set check. Now that we have persistent storages too, we need to also
    // check the latest-ness of pages if we followed a snapshot pointer.
    return true;
}

bool XctManagerPimpl::precommit_xct_verify_readwrite(thread::Thread* context) {
    Xct& current_xct = context->get_current_xct();
    const WriteXctAccess*   write_set = current_xct.get_write_set();
    const uint32_t          write_set_size = current_xct.get_write_set_size();
    const XctAccess*        read_set = current_xct.get_read_set();
    const uint32_t          read_set_size = current_xct.get_read_set_size();
    for (uint32_t i = 0; i < read_set_size; ++i) {
        // The owning transaction has changed.
        // We don't check ordinal here because there is no change we are racing with ourselves.
        const XctAccess& access = read_set[i];
        DVLOG(2) << *context << " Verifying " << access.storage_->get_name()
            << ":" << access.record_ << ". observed_xid=" << access.observed_owner_id_
                << ", now_xid=" << access.record_->owner_id_;
        ASSERT_ND(!access.observed_owner_id_.is_keylocked());  // we made it sure when we read.
        if (!access.observed_owner_id_.equals_serial_order(access.record_->owner_id_)) {
            DLOG(WARNING) << *context << " read set changed by other transaction. will abort";
            return false;
        }
        // TODO(Hideaki) For data structures that have previous links, we need to check if
        // it's latest. Array doesn't have it. So, we don't have the check so far.
        if (access.record_->owner_id_.is_keylocked()) {
            DVLOG(2) << *context
                << " read set contained a locked record. was it myself who locked it?";
            // write set is sorted. so we can do binary search.
            WriteXctAccess dummy;
            dummy.record_ = access.record_;
            bool found = std::binary_search(write_set, write_set + write_set_size, dummy,
                               WriteXctAccess::compare);
            if (!found) {
                DLOG(WARNING) << *context << " no, not me. will abort";
                return false;
            } else {
                DVLOG(2) << *context << " okay, myself. go on.";
            }
        }
    }

    // TODO(Hideaki) Node set check. Now that we have persistent storages too, we need to also
    // check the latest-ness of pages if we followed a snapshot pointer.
    return true;
}

void XctManagerPimpl::precommit_xct_apply(thread::Thread* context,
                                                     Epoch *commit_epoch) {
    Xct& current_xct = context->get_current_xct();
    WriteXctAccess* write_set = current_xct.get_write_set();
    uint32_t        write_set_size = current_xct.get_write_set_size();
    DVLOG(1) << *context << " applying and unlocking.. write_set_size=" << write_set_size;

    current_xct.issue_next_id(commit_epoch);
    XctId new_xct_id = current_xct.get_id();
    ASSERT_ND(new_xct_id.get_thread_id() == context->get_thread_id());
    ASSERT_ND(new_xct_id.get_epoch() == *commit_epoch);
    ASSERT_ND(new_xct_id.get_ordinal() > 0);
    ASSERT_ND(new_xct_id.is_status_bits_off());

    DVLOG(1) << *context << " generated new xct id=" << new_xct_id;
    for (uint32_t i = 0; i < write_set_size; ++i) {
        WriteXctAccess& write = write_set[i];
        DVLOG(2) << *context << " Applying/Unlocking " << write.storage_->get_name()
            << ":" << write.record_;
        log::invoke_apply_record(
            new_xct_id, write.log_entry_, context, write.storage_, write.record_);
    }
    DVLOG(1) << *context << " applied and unlocked write set";
}

void XctManagerPimpl::precommit_xct_unlock(thread::Thread* context) {
    WriteXctAccess* write_set = context->get_current_xct().get_write_set();
    uint32_t        write_set_size = context->get_current_xct().get_write_set_size();
    DVLOG(1) << *context << " unlocking without applying.. write_set_size=" << write_set_size;
    assorted::memory_fence_release();
    for (uint32_t i = 0; i < write_set_size; ++i) {
        WriteXctAccess& write = write_set[i];
        DVLOG(2) << *context << " Unlocking " << write.storage_->get_name() << ":" << write.record_;
        write.record_->owner_id_.release_keylock();
    }
    assorted::memory_fence_release();
    DLOG(INFO) << *context << " unlocked write set without applying";
}

ErrorStack XctManagerPimpl::abort_xct(thread::Thread* context) {
    Xct& current_xct = context->get_current_xct();
    if (!current_xct.is_active()) {
        return ERROR_STACK(ERROR_CODE_XCT_NO_XCT);
    }
    DLOG(INFO) << *context << " Aborted transaction in thread-" << context->get_thread_id();
    current_xct.deactivate();
    context->get_thread_log_buffer().discard_current_xct_log();
    return RET_OK;
}

}  // namespace xct
}  // namespace foedus
