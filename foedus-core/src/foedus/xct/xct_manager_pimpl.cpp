/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/xct/xct_manager_pimpl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <thread>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/log_type_invoke.hpp"
#include "foedus/log/thread_log_buffer_impl.hpp"
#include "foedus/savepoint/savepoint.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_access.hpp"
#include "foedus/xct/xct_inl.hpp"
#include "foedus/xct/xct_manager.hpp"
#include "foedus/xct/xct_options.hpp"

namespace foedus {
namespace xct {
// XctManager methods defined here to enable inlining
Epoch       XctManager::get_current_global_epoch() const {
  return pimpl_->get_current_global_epoch();
}
Epoch       XctManager::get_current_global_epoch_weak() const {
  return pimpl_->get_current_global_epoch_weak();
}
void        XctManager::advance_current_global_epoch() { pimpl_->advance_current_global_epoch(); }
ErrorCode   XctManager::wait_for_commit(Epoch commit_epoch, int64_t wait_microseconds) {
  return pimpl_->wait_for_commit(commit_epoch, wait_microseconds);
}

ErrorCode   XctManager::begin_xct(thread::Thread* context, IsolationLevel isolation_level) {
  return pimpl_->begin_xct(context, isolation_level);
}
ErrorCode   XctManager::begin_schema_xct(thread::Thread* context) {
  return pimpl_->begin_schema_xct(context);
}

ErrorCode   XctManager::precommit_xct(thread::Thread* context, Epoch *commit_epoch) {
  return pimpl_->precommit_xct(context, commit_epoch);
}
ErrorCode   XctManager::abort_xct(thread::Thread* context)  { return pimpl_->abort_xct(context); }


ErrorStack XctManagerPimpl::initialize_once() {
  LOG(INFO) << "Initializing XctManager..";
  if (!engine_->get_storage_manager().is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  }
  const savepoint::Savepoint &savepoint = engine_->get_savepoint_manager().get_savepoint_fast();
  current_global_epoch_ = savepoint.get_current_epoch().value();
  ASSERT_ND(get_current_global_epoch().is_valid());
  epoch_advance_thread_.initialize("epoch_advance_thread",
    std::thread(&XctManagerPimpl::handle_epoch_advance, this),
    std::chrono::milliseconds(engine_->get_options().xct_.epoch_advance_interval_ms_));
  return kRetOk;
}

ErrorStack XctManagerPimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing XctManager..";
  ErrorStackBatch batch;
  if (!engine_->get_storage_manager().is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  epoch_advance_thread_.stop();
  return SUMMARIZE_ERROR_BATCH(batch);
}

void XctManagerPimpl::handle_epoch_advance() {
  LOG(INFO) << "epoch_advance_thread started.";
  // Wait until all the other initializations are done.
  SPINLOCK_WHILE(!epoch_advance_thread_.is_stop_requested() && !is_initialized()) {
    assorted::memory_fence_acquire();
  }
  LOG(INFO) << "epoch_advance_thread now starts processing.";
  while (!epoch_advance_thread_.sleep()) {
    VLOG(1) << "epoch_advance_thread. current_global_epoch_=" << get_current_global_epoch();
    ASSERT_ND(get_current_global_epoch().is_valid());
    current_global_epoch_advanced_.notify_all([this]{
      current_global_epoch_ = get_current_global_epoch().one_more().value();
    });
    engine_->get_log_manager().wakeup_loggers();
  }
  LOG(INFO) << "epoch_advance_thread ended.";
}

void XctManagerPimpl::advance_current_global_epoch() {
  Epoch now = get_current_global_epoch();
  LOG(INFO) << "Requesting to immediately advance epoch. current_global_epoch_=" << now << "...";
  if (now == get_current_global_epoch()) {
    epoch_advance_thread_.wakeup();  // hurrrrry up!
    if (now != get_current_global_epoch()) {
      current_global_epoch_advanced_.wait([this, now]{
        return now != get_current_global_epoch();
      });
    }
  }

  LOG(INFO) << "epoch advanced. current_global_epoch_=" << get_current_global_epoch();
}

ErrorCode XctManagerPimpl::wait_for_commit(Epoch commit_epoch, int64_t wait_microseconds) {
  assorted::memory_fence_acquire();
  if (commit_epoch < get_current_global_epoch()) {
    epoch_advance_thread_.wakeup();
  }

  return engine_->get_log_manager().wait_until_durable(commit_epoch, wait_microseconds);
}


ErrorCode XctManagerPimpl::begin_xct(thread::Thread* context, IsolationLevel isolation_level) {
  Xct& current_xct = context->get_current_xct();
  if (current_xct.is_active()) {
    return kErrorCodeXctAlreadyRunning;
  }
  DLOG(INFO) << *context << " Began new transaction";
  current_xct.activate(isolation_level);
  ASSERT_ND(context->get_thread_log_buffer().get_offset_tail()
    == context->get_thread_log_buffer().get_offset_committed());
  ASSERT_ND(current_xct.get_read_set_size() == 0);
  ASSERT_ND(current_xct.get_write_set_size() == 0);
  ASSERT_ND(current_xct.get_lock_free_write_set_size() == 0);
  return kErrorCodeOk;
}
ErrorCode XctManagerPimpl::begin_schema_xct(thread::Thread* context) {
  Xct& current_xct = context->get_current_xct();
  if (current_xct.is_active()) {
    return kErrorCodeXctAlreadyRunning;
  }
  LOG(INFO) << *context << " Began new schema transaction";
  current_xct.activate(kSerializable, true);
  ASSERT_ND(context->get_thread_log_buffer().get_offset_tail()
    == context->get_thread_log_buffer().get_offset_committed());
  ASSERT_ND(current_xct.get_read_set_size() == 0);
  ASSERT_ND(current_xct.get_write_set_size() == 0);
  ASSERT_ND(current_xct.get_lock_free_write_set_size() == 0);
  return kErrorCodeOk;
}


ErrorCode XctManagerPimpl::precommit_xct(thread::Thread* context, Epoch *commit_epoch) {
  Xct& current_xct = context->get_current_xct();
  if (!current_xct.is_active()) {
    return kErrorCodeXctNoXct;
  }

  bool success;
  if (current_xct.is_schema_xct()) {
    success = precommit_xct_schema(context, commit_epoch);
  } else {
    bool read_only = context->get_current_xct().is_read_only();
    if (read_only) {
      success = precommit_xct_readonly(context, commit_epoch);
    } else {
      success = precommit_xct_readwrite(context, commit_epoch);
    }
  }

  current_xct.deactivate();
  if (success) {
    return kErrorCodeOk;
  } else {
    DLOG(WARNING) << *context << " Aborting because of contention";
    context->get_thread_log_buffer().discard_current_xct_log();
    return kErrorCodeXctRaceAbort;
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

  Xct& current_xct = context->get_current_xct();
  uint64_t write_set_size = current_xct.get_write_set_size();
  WriteXctAccess  write_set_copy[write_set_size];
  for (int x = 0; x < write_set_size; x++) write_set_copy[x] = *(current_xct.get_write_set() + x);

  precommit_xct_lock(context);  // Phase 1

  // BEFORE the first fence, update the in_commit_log_epoch_ for logger
  Xct::InCommitLogEpochGuard guard(&context->get_current_xct(), get_current_global_epoch_weak());

  assorted::memory_fence_acq_rel();

  *commit_epoch = get_current_global_epoch_weak();  // serialization point!
  DVLOG(1) << *context << " Acquired read-write commit epoch " << *commit_epoch;

  assorted::memory_fence_acq_rel();
  bool verified = precommit_xct_verify_readwrite(context);  // phase 2
  if (verified) {
    precommit_xct_apply(context, commit_epoch, &(write_set_copy[0]));  // phase 3. this also unlocks
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

  Xct::InCommitLogEpochGuard guard(&context->get_current_xct(), get_current_global_epoch_weak());
  assorted::memory_fence_acq_rel();
  *commit_epoch = get_current_global_epoch_weak();  // serialization point!
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
    ASSERT_ND(code != log::kLogCodeInvalid);
    log::LogCodeKind kind = log::get_log_code_kind(code);
    LOG(INFO) << *context << " Applying schema log " << log::get_log_type_name(code)
      << ". kind=" << kind << ", log length=" << header->log_length_;
    if (header->log_length_ >= 16) {
      header->xct_id_ = new_xct_id;
    } else {
      // So far only log type that omits xct_id is FillerLogType.
      ASSERT_ND(code == log::kLogCodeFiller);
    }
    if (kind == log::kMarkerLogs) {
      LOG(INFO) << *context << " Ignored marker log in schema xct's apply";
    } else if (kind == log::kEngineLogs) {
      // engine type log, such as XXX.
      log::invoke_apply_engine(entry, context);
    } else if (kind == log::kStorageLogs) {
      // storage type log, such as CREATE/DROP STORAGE.
      storage::StorageId storage_id = header->storage_id_;
      LOG(INFO) << *context << " schema xct applying storage-" << storage_id;
      storage::Storage* storage = engine_->get_storage_manager().get_storage(storage_id);
      log::invoke_apply_storage(entry, context, storage);
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
  WriteXctAccess*  write_set = current_xct.get_write_set();

  uint32_t        write_set_size = current_xct.get_write_set_size();
  DVLOG(1) << *context << " #write_sets=" << write_set_size << ", addr=" << write_set;
  std::sort(write_set, write_set + write_set_size, WriteXctAccess::compare);

#ifndef NDEBUG
  // check that write sets are now sorted
  for (uint32_t i = 1; i < write_set_size; ++i) {
    ASSERT_ND(
      write_set[i].record_ == write_set[i - 1].record_ ||
      WriteXctAccess::compare(write_set[i - 1], write_set[i]));
  }
#endif  // NDEBUG

  // One differences from original SILO protocol.
  // As there might be multiple write sets on one record, we check equality of next
  // write set and 1) lock only at the first write-set of the record, 2) unlock at the last

  // lock them unconditionally. there is no risk of deadlock thanks to the sort.
  // lock bit is the highest bit of ordinal_and_status_.
  for (uint32_t i = 0; i < write_set_size; ++i) {
    DVLOG(2) << *context << " Locking " << write_set[i].storage_->get_name()
      << ":" << write_set[i].record_;
    if (i > 0 && write_set[i].record_ == write_set[i - 1].record_) {
      DVLOG(0) << *context << " Multiple write sets on record " << write_set[i].storage_->get_name()
        << ":" << write_set[i].record_ << ". Will lock the first one and unlock the last one";
    } else {
      write_set[i].record_->owner_id_.keylock_unconditional();
    }
    ASSERT_ND(write_set[i].record_->owner_id_.is_keylocked());
  }
  DVLOG(1) << *context << " locked write set";

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

    // Remembers the highest epoch observed.
    commit_epoch->store_max(access.observed_owner_id_.get_epoch());
  }

  DVLOG(1) << *context << "Read-only higest epoch observed: " << *commit_epoch;
  if (!commit_epoch->is_valid()) {
    DLOG(INFO) << *context
      << " Read-only higest epoch was empty. The transaction has no read set??";
    // In this case, set already-durable epoch. We don't have to use atomic version because
    // it's just conservatively telling how long it should wait.
    *commit_epoch = Epoch(engine_->get_log_manager().get_durable_global_epoch_weak());
  }

  // Node set check.
  const NodeAccess*       node_set = current_xct.get_node_set();
  const uint32_t          node_set_size = current_xct.get_node_set_size();
  for (uint32_t i = 0; i < node_set_size; ++i) {
    const NodeAccess& access = node_set[i];
    if (access.observed_.word != access.address_->word) {
      DLOG(WARNING) << *context << " node set changed by other transaction. will abort";
      return false;
    }
  }
  return true;
}

bool XctManagerPimpl::precommit_xct_verify_readwrite(thread::Thread* context) {
  Xct& current_xct = context->get_current_xct();
  WriteXctAccess*         write_set = current_xct.get_write_set();
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
      bool found = std::binary_search(
        write_set,
        write_set + write_set_size,
        dummy,
        WriteXctAccess::compare);
      if (!found) {
        DLOG(WARNING) << *context << " no, not me. will abort";
        return false;
      } else {
        DVLOG(2) << *context << " okay, myself. go on.";
      }
    }
  }

  // Node set check.
  const NodeAccess*       node_set = current_xct.get_node_set();
  const uint32_t          node_set_size = current_xct.get_node_set_size();
  for (uint32_t i = 0; i < node_set_size; ++i) {
    const NodeAccess& access = node_set[i];
    if (access.observed_.word != access.address_->word) {
      DLOG(WARNING) << *context << " node set changed by other transaction. will abort";
      return false;
    }
  }
  return true;
}

void XctManagerPimpl::precommit_xct_apply(thread::Thread* context, Epoch *commit_epoch,
                                          WriteXctAccess* write_set_original) {
  Xct& current_xct = context->get_current_xct();
  WriteXctAccess* write_set = write_set_original; //unsorted
  uint32_t        write_set_size = current_xct.get_write_set_size();
  LockFreeWriteXctAccess* lock_free_write_set = current_xct.get_lock_free_write_set();
  uint32_t                lock_free_write_set_size = current_xct.get_lock_free_write_set_size();
  DVLOG(1) << *context << " applying and unlocking.. write_set_size=" << write_set_size
    << ", lock_free_write_set_size=" << lock_free_write_set_size;

  current_xct.issue_next_id(commit_epoch);
  XctId new_xct_id = current_xct.get_id();
  ASSERT_ND(new_xct_id.get_thread_id() == context->get_thread_id());
  ASSERT_ND(new_xct_id.get_epoch() == *commit_epoch);
  ASSERT_ND(new_xct_id.get_ordinal() > 0);
  ASSERT_ND(new_xct_id.is_status_bits_off());
//  XctId locked_new_xct_id = new_xct_id;
//  locked_new_xct_id.keylock_unconditional();
  DVLOG(1) << *context << " generated new xct id=" << new_xct_id;
  for (uint32_t i = 0; i < write_set_size; ++i) {
    WriteXctAccess& write = write_set[i];
    DVLOG(2) << *context << " Applying/Unlocking " << write.storage_->get_name()
      << ":" << write.record_;

    // We must be careful on the memory order of unlock and data write.
    // We must write data first (invoke_apply), then unlock.
    // Otherwise the correctness is not guaranteed.
    // Also because we want to write records in order
    log::invoke_apply_record(write.log_entry_, context, write.storage_, write.record_);
    // For this reason, we put memory_fence_release() between data and owner_id writes.
    assorted::memory_fence_release();
    ASSERT_ND(!write.record_->owner_id_.get_epoch().is_valid() ||
      write.record_->owner_id_.before(new_xct_id));  // ordered correctly?
    // Since we're applying in order, not in sorted order, it's easiest to do unlocks at once after
//     // we can't just check if next edited record is same record
//     // TODO: Possibly make the next few lines faster
//     bool is_final = true;
//     for (uint32_t x = i + 1; x < write_set_size; x++) {
//       if (write_set[x].record_ == write_set[i].record_) is_final = false;
//     }
//     if (!is_final) {
//       DVLOG(0) << *context << " Multiple write sets on record " << write_set[i].storage_->get_name()
//         << ":" << write_set[i].record_ << ". Unlock at the last one of the write sets";
//       // keep the lock for the next write set
//     } else {
//       write.record_->owner_id_ = new_xct_id;  // this also unlocks
//     }
  }
  // Unlock records all at once // Be careful to only overwrite each record's ID once
  for (uint32_t i = 0; i < write_set_size; ++i) {
    WriteXctAccess& write = (context->get_current_xct().get_write_set())[i]; //Use sorted list
    if (i < write_set_size - 1 && write_set[i].record_ == write_set[i + 1].record_) {
      DVLOG(0) << *context << " Multiple write sets on record " << write_set[i].storage_->get_name()
        << ":" << write_set[i].record_ << ". Unlock at the last one of the write sets";
      // keep the lock for the next write set
    } else {
      ASSERT_ND(write.record_->owner_id_.is_keylocked());
      write.record_->owner_id_ = new_xct_id;  // this also unlocks
    }
  }
  // lock-free write-set doesn't have to worry about lock or ordering.
  for (uint32_t i = 0; i < lock_free_write_set_size; ++i) {
    LockFreeWriteXctAccess& write = lock_free_write_set[i];
    DVLOG(2) << *context << " Applying Lock-Free write " << write.storage_->get_name();
    write.log_entry_->header_.xct_id_ = new_xct_id;
    log::invoke_apply_record(write.log_entry_, context, write.storage_, nullptr);
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

ErrorCode XctManagerPimpl::abort_xct(thread::Thread* context) {
  Xct& current_xct = context->get_current_xct();
  if (!current_xct.is_active()) {
    return kErrorCodeXctNoXct;
  }
  DLOG(INFO) << *context << " Aborted transaction in thread-" << context->get_thread_id();
  current_xct.deactivate();
  context->get_thread_log_buffer().discard_current_xct_log();
  return kErrorCodeOk;
}

}  // namespace xct
}  // namespace foedus
