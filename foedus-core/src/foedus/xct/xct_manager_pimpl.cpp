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
#include "foedus/assorted/cacheline.hpp"
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
  ASSERT_ND(current_xct.get_mcs_block_current() == 0);
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
  ASSERT_ND(current_xct.get_mcs_block_current() == 0);
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
  ASSERT_ND(context->get_thread_log_buffer().get_offset_committed() ==
      context->get_thread_log_buffer().get_offset_tail());
  *commit_epoch = Epoch();
  assorted::memory_fence_acquire();  // this is enough for read-only case
  return precommit_xct_verify_readonly(context, commit_epoch);
}

bool XctManagerPimpl::precommit_xct_readwrite(thread::Thread* context, Epoch *commit_epoch) {
  DVLOG(1) << *context << " Committing read-write";
  bool success = precommit_xct_lock(context);  // Phase 1
  // lock can fail only when physical records went too far away
  if (!success) {
    DLOG(INFO) << *context << " Interesting. failed due to records moved too far away";
    return false;
  }

  // BEFORE the first fence, update the in_commit_log_epoch_ for logger
  Xct::InCommitLogEpochGuard guard(&context->get_current_xct(), get_current_global_epoch_weak());

  assorted::memory_fence_acq_rel();

  *commit_epoch = get_current_global_epoch_weak();  // serialization point!
  DVLOG(1) << *context << " Acquired read-write commit epoch " << *commit_epoch;

  assorted::memory_fence_acq_rel();
  bool verified = precommit_xct_verify_readwrite(context);  // phase 2
#ifndef NDEBUG
  {
    WriteXctAccess* write_set = context->get_current_xct().get_write_set();
    uint32_t        write_set_size = context->get_current_xct().get_write_set_size();
    for (uint32_t i = 0; i < write_set_size; ++i) {
      ASSERT_ND(write_set[i].owner_id_address_->is_keylocked());
    }
  }
#endif  // NDEBUG
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
    header->set_xct_id(new_xct_id);
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

bool XctManagerPimpl::precommit_xct_lock(thread::Thread* context) {
  Xct& current_xct = context->get_current_xct();
  WriteXctAccess* write_set = current_xct.get_write_set();
  uint32_t        write_set_size = current_xct.get_write_set_size();
  DVLOG(1) << *context << " #write_sets=" << write_set_size << ", addr=" << write_set;

  // we have to access the owner_id's pointed address. let's prefetch them in parallel
  for (uint32_t i = 0; i < write_set_size; ++i) {
    assorted::prefetch_cacheline(write_set[i].owner_id_address_);
  }

  while (true) {  // while loop for retrying in case of moved-bit error
    // first, check for moved-bit and track where the corresponding physical record went.
    // we do this before locking, so it is possible that later we find it moved again.
    // if that happens, we retry.
    // we must not do lock-then-track to avoid deadlocks.
    for (uint32_t i = 0; i < write_set_size; ++i) {
      // TODO(Hideaki) if this happens often, this might be too frequent virtual method call.
      // maybe a batched version of this? I'm not sure if this is that often, though.
      if (UNLIKELY(write_set[i].owner_id_address_->is_moved())) {
        bool success = write_set[i].storage_->track_moved_record(write_set + i);
        if (!success) {
          // this happens when the record went too far away (eg another layer in masstree).
          // in that case, retry the whole transaction. This is rare.
          return false;
        }
      }
    }

    std::sort(write_set, write_set + write_set_size, WriteXctAccess::compare);

#ifndef NDEBUG
    // check that write sets are now sorted
    for (uint32_t i = 1; i < write_set_size; ++i) {
      ASSERT_ND(
        write_set[i].owner_id_address_ == write_set[i - 1].owner_id_address_ ||
        WriteXctAccess::compare(write_set[i - 1], write_set[i]));
    }
#endif  // NDEBUG

    // One differences from original SILO protocol.
    // As there might be multiple write sets on one record, we check equality of next
    // write set and lock/unlock only at the last write-set of the record.

    // lock them unconditionally. there is no risk of deadlock thanks to the sort.
    bool needs_retry = false;
    for (uint32_t i = 0; i < write_set_size; ++i) {
      ASSERT_ND(write_set[i].mcs_block_ == 0);
      DVLOG(2) << *context << " Locking " << write_set[i].storage_->get_name()
        << ":" << write_set[i].owner_id_address_;
      if (i < write_set_size - 1 &&
        write_set[i].owner_id_address_ == write_set[i + 1].owner_id_address_) {
        DVLOG(0) << *context << " Multiple write sets on record "
          << write_set[i].storage_->get_name()
          << ":" << write_set[i].owner_id_address_
          << ". Will lock/unlock at the last one";
      } else {
        write_set[i].mcs_block_ = context->mcs_acquire_lock(
          write_set[i].owner_id_address_->get_key_lock());
        if (UNLIKELY(write_set[i].owner_id_address_->is_moved())) {
          VLOG(0) << *context << " Interesting. moved-bit conflict in "
            << write_set[i].storage_->get_name()
            << ":" << write_set[i].owner_id_address_
            << ". This occasionally happens.";
          // release all locks acquired so far, retry
          precommit_xct_unlock(context);
          needs_retry = true;
          break;
        }
        ASSERT_ND(!write_set[i].owner_id_address_->is_moved());
        ASSERT_ND(write_set[i].owner_id_address_->is_keylocked());
      }
    }

    if (!needs_retry) {
      break;
    }
  }
  DVLOG(1) << *context << " locked write set";
#ifndef NDEBUG
  for (uint32_t i = 0; i < write_set_size; ++i) {
    ASSERT_ND(write_set[i].owner_id_address_->lock_.is_keylocked());
  }
#endif  // NDEBUG
  return true;
}

const uint16_t kReadsetPrefetchBatch = 16;

bool XctManagerPimpl::precommit_xct_verify_readonly(thread::Thread* context, Epoch *commit_epoch) {
  Xct& current_xct = context->get_current_xct();
  XctAccess*        read_set = current_xct.get_read_set();
  const uint32_t    read_set_size = current_xct.get_read_set_size();
  for (uint32_t i = 0; i < read_set_size; ++i) {
    // let's prefetch owner_id in parallel
    if (i % kReadsetPrefetchBatch == 0) {
      for (uint32_t j = i; j < i + kReadsetPrefetchBatch && j < read_set_size; ++j) {
        assorted::prefetch_cacheline(read_set[j].owner_id_address_);
      }
    }

    // The owning transaction has changed.
    // We don't check ordinal here because there is no change we are racing with ourselves.
    XctAccess& access = read_set[i];
    DVLOG(2) << *context << "Verifying " << access.storage_->get_name()
      << ":" << access.owner_id_address_ << ". observed_xid=" << access.observed_owner_id_
        << ", now_xid=" << access.owner_id_address_->xct_id_;
    if (UNLIKELY(access.owner_id_address_->is_moved())) {
      access.owner_id_address_ = access.storage_->track_moved_record(access.owner_id_address_);
    }
    if (access.observed_owner_id_ != access.owner_id_address_->xct_id_) {
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

  // Check Page Pointer/Version
  if (!precommit_xct_verify_pointer_set(context)) {
    return false;
  } else if (!precommit_xct_verify_page_version_set(context)) {
    return false;
  } else {
    return true;
  }
}

bool XctManagerPimpl::precommit_xct_verify_readwrite(thread::Thread* context) {
  Xct& current_xct = context->get_current_xct();
  const WriteXctAccess*   write_set = current_xct.get_write_set();
  const uint32_t          write_set_size = current_xct.get_write_set_size();
  XctAccess*              read_set = current_xct.get_read_set();
  const uint32_t          read_set_size = current_xct.get_read_set_size();
  for (uint32_t i = 0; i < read_set_size; ++i) {
    // let's prefetch owner_id in parallel
    if (i % kReadsetPrefetchBatch == 0) {
      for (uint32_t j = i; j < i + kReadsetPrefetchBatch && j < read_set_size; ++j) {
        assorted::prefetch_cacheline(read_set[j].owner_id_address_);
      }
    }

    // The owning transaction has changed.
    // We don't check ordinal here because there is no change we are racing with ourselves.
    XctAccess& access = read_set[i];
    DVLOG(2) << *context << " Verifying " << access.storage_->get_name()
      << ":" << access.owner_id_address_ << ". observed_xid=" << access.observed_owner_id_
        << ", now_xid=" << access.owner_id_address_->xct_id_;

    // read-set has to also track moved records.
    // however, unlike write-set locks, we don't have to do retry-loop.
    // if the rare event (yet another concurrent split) happens, we just abort the transaction.
    if (UNLIKELY(access.owner_id_address_->is_moved())) {
      access.owner_id_address_ = access.storage_->track_moved_record(access.owner_id_address_);
    }

    if (access.observed_owner_id_ != access.owner_id_address_->xct_id_) {
      DLOG(WARNING) << *context << " read set changed by other transaction. will abort";
      return false;
    }
    if (access.owner_id_address_->is_keylocked()) {
      DVLOG(2) << *context
        << " read set contained a locked record. was it myself who locked it?";
      // write set is sorted. so we can do binary search.
      WriteXctAccess dummy;
      dummy.owner_id_address_ = access.owner_id_address_;
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

  // Check Page Pointer/Version
  if (!precommit_xct_verify_pointer_set(context)) {
    return false;
  } else if (!precommit_xct_verify_page_version_set(context)) {
    return false;
  } else {
    return true;
  }
}

bool XctManagerPimpl::precommit_xct_verify_pointer_set(thread::Thread* context) {
  const Xct& current_xct = context->get_current_xct();
  const PointerAccess*    pointer_set = current_xct.get_pointer_set();
  const uint32_t          pointer_set_size = current_xct.get_pointer_set_size();
  for (uint32_t i = 0; i < pointer_set_size; ++i) {
    // let's prefetch address_ in parallel
    if (i % kReadsetPrefetchBatch == 0) {
      for (uint32_t j = i; j < i + kReadsetPrefetchBatch && j < pointer_set_size; ++j) {
        assorted::prefetch_cacheline(pointer_set[j].address_);
      }
    }
    const PointerAccess& access = pointer_set[i];
    if (access.address_->word !=  access.observed_.word) {
      DLOG(WARNING) << *context << " volatile ptr is changed by other transaction. will abort";
      return false;
    }
  }
  return true;
}
bool XctManagerPimpl::precommit_xct_verify_page_version_set(thread::Thread* context) {
  const Xct& current_xct = context->get_current_xct();
  const PageVersionAccess*  page_version_set = current_xct.get_page_version_set();
  const uint32_t            page_version_set_size = current_xct.get_page_version_set_size();
  for (uint32_t i = 0; i < page_version_set_size; ++i) {
    // let's prefetch address_ in parallel
    if (i % kReadsetPrefetchBatch == 0) {
      for (uint32_t j = i; j < i + kReadsetPrefetchBatch && j < page_version_set_size; ++j) {
        assorted::prefetch_cacheline(page_version_set[j].address_);
      }
    }
    const PageVersionAccess& access = page_version_set[i];
    if (access.address_->status_ != access.observed_) {
      DLOG(WARNING) << *context << " page version is changed by other transaction. will abort"
        " observed=" << access.observed_ << ", now=" << access.address_->status_;
      return false;
    }
  }
  return true;
}

void XctManagerPimpl::precommit_xct_apply(thread::Thread* context, Epoch *commit_epoch) {
  Xct& current_xct = context->get_current_xct();
  WriteXctAccess* write_set = current_xct.get_write_set();
  uint32_t        write_set_size = current_xct.get_write_set_size();
  LockFreeWriteXctAccess* lock_free_write_set = current_xct.get_lock_free_write_set();
  uint32_t                lock_free_write_set_size = current_xct.get_lock_free_write_set_size();
  DVLOG(1) << *context << " applying and unlocking.. write_set_size=" << write_set_size
    << ", lock_free_write_set_size=" << lock_free_write_set_size;

  current_xct.issue_next_id(commit_epoch);
  XctId new_xct_id = current_xct.get_id();
  ASSERT_ND(new_xct_id.get_epoch() == *commit_epoch);
  ASSERT_ND(new_xct_id.get_ordinal() > 0);
  new_xct_id.clear_status_bits();
  XctId new_deleted_xct_id = new_xct_id;
  new_deleted_xct_id.set_deleted();  // used if the record after apply is in deleted state.

  DVLOG(1) << *context << " generated new xct id=" << new_xct_id;
  for (uint32_t i = 0; i < write_set_size; ++i) {
    WriteXctAccess& write = write_set[i];
    DVLOG(2) << *context << " Applying/Unlocking " << write.storage_->get_name()
      << ":" << write.owner_id_address_;
    ASSERT_ND(write.owner_id_address_->is_keylocked());
    ASSERT_ND(write.mcs_block_ != 0 ||
      write.owner_id_address_ == write_set[i + 1].owner_id_address_);

    // We must be careful on the memory order of unlock and data write.
    // We must write data first (invoke_apply), then unlock.
    // Otherwise the correctness is not guaranteed.
    write.log_entry_->header_.set_xct_id(new_xct_id);
    if (i > 0 && write.owner_id_address_ == write_set[i - 1].owner_id_address_) {
      // the previous one has already set being_written and kept the lock
      ASSERT_ND(write.owner_id_address_->xct_id_.is_being_written());
    } else {
      ASSERT_ND(!write.owner_id_address_->xct_id_.is_being_written());
      write.owner_id_address_->xct_id_.set_being_written();
      assorted::memory_fence_release();
    }
    log::invoke_apply_record(
      write.log_entry_,
      context,
      write.storage_,
      write.owner_id_address_,
      write.payload_address_);
    ASSERT_ND(!write.owner_id_address_->xct_id_.get_epoch().is_valid() ||
      write.owner_id_address_->xct_id_.before(new_xct_id));  // ordered correctly?
    if (i < write_set_size - 1 &&
      write.owner_id_address_ == write_set[i + 1].owner_id_address_) {
      DVLOG(0) << *context << " Multiple write sets on record " << write_set[i].storage_->get_name()
        << ":" << write_set[i].owner_id_address_ << ". Unlock at the last one of the write sets";
      // keep the lock for the next write set
      ASSERT_ND(write.mcs_block_ == 0);
    } else {
      ASSERT_ND(write.mcs_block_ != 0);
      // For this reason, we put memory_fence_release() between data and owner_id writes.
      assorted::memory_fence_release();
      if (write.owner_id_address_->xct_id_.is_deleted()) {
        // preserve delete-flag set by delete operations (so, the operation should be delete)
        ASSERT_ND(
          write.log_entry_->header_.get_type() == log::kLogCodeHashDelete ||
          write.log_entry_->header_.get_type() == log::kLogCodeMasstreeDelete);
        write.owner_id_address_->xct_id_ = new_deleted_xct_id;
      } else {
        ASSERT_ND(
          write.log_entry_->header_.get_type() != log::kLogCodeHashDelete &&
          write.log_entry_->header_.get_type() != log::kLogCodeMasstreeDelete);
        write.owner_id_address_->xct_id_ = new_xct_id;
      }
      // also unlocks
      context->mcs_release_lock(write.owner_id_address_->get_key_lock(), write.mcs_block_);
      write.mcs_block_ = 0;
    }
  }
  // lock-free write-set doesn't have to worry about lock or ordering.
  for (uint32_t i = 0; i < lock_free_write_set_size; ++i) {
    LockFreeWriteXctAccess& write = lock_free_write_set[i];
    DVLOG(2) << *context << " Applying Lock-Free write " << write.storage_->get_name();
    write.log_entry_->header_.set_xct_id(new_xct_id);
    log::invoke_apply_record(write.log_entry_, context, write.storage_, nullptr, nullptr);
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
    // this might be called from precommit_xct_lock(), so some of them might not be locked yet.
    if (write.mcs_block_ != 0) {
      DVLOG(2) << *context << " Unlocking " << write.storage_->get_name() << ":"
        << write.owner_id_address_;
      ASSERT_ND(write.owner_id_address_->is_keylocked());
      context->mcs_release_lock(write.owner_id_address_->get_key_lock(), write.mcs_block_);
      write.mcs_block_ = 0;
    }
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
