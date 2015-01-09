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
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/log/log_type_invoke.hpp"
#include "foedus/log/thread_log_buffer.hpp"
#include "foedus/savepoint/savepoint.hpp"
#include "foedus/savepoint/savepoint_manager.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/thread/thread_ref.hpp"
#include "foedus/xct/in_commit_epoch_guard.hpp"
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

ErrorCode   XctManager::precommit_xct(thread::Thread* context, Epoch *commit_epoch) {
  return pimpl_->precommit_xct(context, commit_epoch);
}
ErrorCode   XctManager::abort_xct(thread::Thread* context)  { return pimpl_->abort_xct(context); }

ErrorStack XctManagerPimpl::initialize_once() {
  LOG(INFO) << "Initializing XctManager..";
  if (!engine_->get_storage_manager()->is_initialized()) {
    return ERROR_STACK(kErrorCodeDepedentModuleUnavailableInit);
  }
  soc::SharedMemoryRepo* memory_repo = engine_->get_soc_manager()->get_shared_memory_repo();
  control_block_ = memory_repo->get_global_memory_anchors()->xct_manager_memory_;

  if (engine_->is_master()) {
    control_block_->initialize();
    control_block_->current_global_epoch_
      = engine_->get_savepoint_manager()->get_initial_current_epoch().value();
    ASSERT_ND(get_current_global_epoch().is_valid());
    control_block_->requested_global_epoch_ = control_block_->current_global_epoch_.load();
    control_block_->epoch_chime_terminate_requested_ = false;
    epoch_chime_thread_ = std::move(std::thread(&XctManagerPimpl::handle_epoch_chime, this));
  }
  return kRetOk;
}

ErrorStack XctManagerPimpl::uninitialize_once() {
  LOG(INFO) << "Uninitializing XctManager..";
  ErrorStackBatch batch;
  if (!engine_->get_storage_manager()->is_initialized()) {
    batch.emprace_back(ERROR_STACK(kErrorCodeDepedentModuleUnavailableUninit));
  }
  if (engine_->is_master()) {
    if (epoch_chime_thread_.joinable()) {
      {
        soc::SharedMutexScope scope(control_block_->epoch_chime_wakeup_.get_mutex());
        control_block_->epoch_chime_terminate_requested_ = true;
        control_block_->epoch_chime_wakeup_.signal(&scope);
      }
      epoch_chime_thread_.join();
    }
    control_block_->uninitialize();
  }
  return SUMMARIZE_ERROR_BATCH(batch);
}

bool XctManagerPimpl::is_stop_requested() const {
  ASSERT_ND(engine_->is_master());
  return control_block_->epoch_chime_terminate_requested_;
}

////////////////////////////////////////////////////////////////////////////////////////////
///
///       Epoch Chime related methods
///
////////////////////////////////////////////////////////////////////////////////////////////
void XctManagerPimpl::handle_epoch_chime() {
  LOG(INFO) << "epoch_chime_thread started.";
  ASSERT_ND(engine_->is_master());
  // Wait until all the other initializations are done.
  SPINLOCK_WHILE(!is_stop_requested() && !is_initialized()) {
    assorted::memory_fence_acquire();
  }
  uint64_t interval_nanosec = engine_->get_options().xct_.epoch_advance_interval_ms_ * 1000000ULL;
  LOG(INFO) << "epoch_chime_thread now starts processing. interval_nanosec=" << interval_nanosec;
  while (!is_stop_requested()) {
    {
      soc::SharedMutexScope scope(control_block_->epoch_chime_wakeup_.get_mutex());
      if (is_stop_requested()) {
        break;
      }
      if (get_requested_global_epoch() <= get_current_global_epoch())  {  // otherwise no sleep
        bool signaled = control_block_->epoch_chime_wakeup_.timedwait(&scope, interval_nanosec);
        VLOG(1) << "epoch_chime_thread. wokeup with " << (signaled ? "signal" : "timeout");
      }
    }
    if (is_stop_requested()) {
      break;
    }
    VLOG(1) << "epoch_chime_thread. current_global_epoch_=" << get_current_global_epoch();
    ASSERT_ND(get_current_global_epoch().is_valid());

    // Before advanding the epoch, we have to make sure there is no thread that might commit
    // with previous epoch.
    Epoch grace_epoch = get_current_global_epoch().one_less();
    handle_epoch_chime_wait_grace_period(grace_epoch);
    if (is_stop_requested()) {
      break;
    }

    {
      soc::SharedMutexScope scope(control_block_->current_global_epoch_advanced_.get_mutex());
      control_block_->current_global_epoch_ = get_current_global_epoch().one_more().value();
      control_block_->current_global_epoch_advanced_.broadcast(&scope);
    }
    engine_->get_log_manager()->wakeup_loggers();
  }
  LOG(INFO) << "epoch_chime_thread ended.";
}

void XctManagerPimpl::handle_epoch_chime_wait_grace_period(Epoch grace_epoch) {
  ASSERT_ND(engine_->is_master());
  ASSERT_ND(grace_epoch.one_more() == get_current_global_epoch());

  VLOG(1) << "XctManager waiting until all worker threads exit grace_epoch:" << grace_epoch;
  debugging::StopWatch watch;

  // In most cases, all workers have already switched to the current epoch long before.
  // Quickly check it, then really wait if there is suspicious one.
  thread::ThreadPool* pool = engine_->get_thread_pool();
  uint16_t nodes = engine_->get_soc_count();
  for (uint16_t node = 0; node < nodes; ++node) {
    // Check all threads' in-commit epoch now.
    // We might add a background thread in each SOC to avoid checking too many threads
    // in the master engine, but anyway this happens only once in tens of milliseconds.
    thread::ThreadGroupRef* group = pool->get_group_ref(node);

    // @spinlock until the long-running transaction exits.
    const uint32_t kSpins = 1 << 12;  // some number of spins first, then sleep.
    uint32_t spins = 0;
    SPINLOCK_WHILE(!is_stop_requested()) {
      Epoch min_epoch = group->get_min_in_commit_epoch();
      if (!min_epoch.is_valid() || min_epoch > grace_epoch) {
        break;
      }
      ++spins;
      if (spins == 1U) {
        // first spin.
        VLOG(0) << "Interesting, node-" << node << " has some thread that is still running "
          << "epoch-" << grace_epoch << ". we have to wait before advancing epoch. min_epoch="
          << min_epoch;
      } else if (spins >= kSpins) {
        if (spins == kSpins) {
          LOG(INFO) << "node-" << node << " has some thread that is running "
            << "epoch-" << grace_epoch << " for long time. we are still waiting before advancing"
            << " epoch. min_epoch=" << min_epoch;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
  }

  watch.stop();
  VLOG(1) << "grace_epoch-" << grace_epoch << " guaranteed. took " << watch.elapsed_ns() << "ns";
  if (watch.elapsed_ms() > 10) {
    LOG(INFO) << "Very interesting, grace_epoch-" << grace_epoch << " took "
      << watch.elapsed_ms() << "ms to be guaranteed. Most likely there was a long-running xct";
  }
}


void XctManagerPimpl::wakeup_epoch_chime_thread() {
  soc::SharedMutexScope scope(control_block_->epoch_chime_wakeup_.get_mutex());
  control_block_->epoch_chime_wakeup_.signal(&scope);  // hurrrrry up!
}

void XctManagerPimpl::set_requested_global_epoch(Epoch request) {
  // set request value, atomically
  while (true) {
    Epoch already_requested = get_requested_global_epoch();
    if (already_requested >= request) {
      break;
    }
    Epoch::EpochInteger cmp = already_requested.value();
    if (control_block_->requested_global_epoch_.compare_exchange_strong(cmp, request.value())) {
      break;
    }
  }
}

void XctManagerPimpl::advance_current_global_epoch() {
  Epoch now = get_current_global_epoch();
  Epoch request = now.one_more();
  LOG(INFO) << "Requesting to immediately advance epoch. request=" << request << "...";
  set_requested_global_epoch(request);
  while (get_current_global_epoch() < request) {
    wakeup_epoch_chime_thread();
    {
      soc::SharedMutexScope scope(control_block_->current_global_epoch_advanced_.get_mutex());
      if (get_current_global_epoch() < request) {
        control_block_->current_global_epoch_advanced_.wait(&scope);
      }
    }
  }

  LOG(INFO) << "epoch advanced. current_global_epoch_=" << get_current_global_epoch();
}

void XctManagerPimpl::wait_for_current_global_epoch(Epoch target_epoch) {
  // this method doesn't aggressively wake up the epoch-advance thread. it just waits.
  set_requested_global_epoch(target_epoch);
  while (get_current_global_epoch() < target_epoch) {
    soc::SharedMutexScope scope(control_block_->current_global_epoch_advanced_.get_mutex());
    if (get_current_global_epoch() < target_epoch) {
      control_block_->current_global_epoch_advanced_.wait(&scope);
    }
  }
}


ErrorCode XctManagerPimpl::wait_for_commit(Epoch commit_epoch, int64_t wait_microseconds) {
  // to durably commit transactions in commit_epoch, the current global epoch should be
  // commit_epoch + 2 or more. (current-1 is grace epoch. current-2 is the the latest loggable ep)
  Epoch target_epoch = commit_epoch.one_more().one_more();
  if (target_epoch > get_current_global_epoch()) {
    set_requested_global_epoch(target_epoch);
    wakeup_epoch_chime_thread();
  }

  return engine_->get_log_manager()->wait_until_durable(commit_epoch, wait_microseconds);
}

////////////////////////////////////////////////////////////////////////////////////////////
///
///       User transactions related methods
///
////////////////////////////////////////////////////////////////////////////////////////////
ErrorCode XctManagerPimpl::begin_xct(thread::Thread* context, IsolationLevel isolation_level) {
  Xct& current_xct = context->get_current_xct();
  if (current_xct.is_active()) {
    return kErrorCodeXctAlreadyRunning;
  }
  if (UNLIKELY(control_block_->new_transaction_paused_.load())) {
    wait_until_resume_accepting_xct(context);
  }
  DVLOG(1) << *context << " Began new transaction";
  current_xct.activate(isolation_level);
  ASSERT_ND(current_xct.get_mcs_block_current() == 0);
  ASSERT_ND(context->get_thread_log_buffer().get_offset_tail()
    == context->get_thread_log_buffer().get_offset_committed());
  ASSERT_ND(current_xct.get_read_set_size() == 0);
  ASSERT_ND(current_xct.get_write_set_size() == 0);
  ASSERT_ND(current_xct.get_lock_free_write_set_size() == 0);
  return kErrorCodeOk;
}

void XctManagerPimpl::pause_accepting_xct() {
  control_block_->new_transaction_paused_.store(true);
}
void XctManagerPimpl::resume_accepting_xct() {
  control_block_->new_transaction_paused_.store(false);
}

void XctManagerPimpl::wait_until_resume_accepting_xct(thread::Thread* context) {
  LOG(INFO) << *context << " realized that new transactions are not accepted now."
    << " waits until it is allowed to start a new transaction";
  // no complex thing here. just sleep-check. this happens VERY infrequently.
  while (control_block_->new_transaction_paused_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
}

ErrorCode XctManagerPimpl::precommit_xct(thread::Thread* context, Epoch *commit_epoch) {
  Xct& current_xct = context->get_current_xct();
  if (!current_xct.is_active()) {
    return kErrorCodeXctNoXct;
  }
  ASSERT_ND(current_xct.assert_related_read_write());

  bool success;
  bool read_only = context->get_current_xct().is_read_only();
  if (read_only) {
    success = precommit_xct_readonly(context, commit_epoch);
  } else {
    success = precommit_xct_readwrite(context, commit_epoch);
  }

  ASSERT_ND(current_xct.assert_related_read_write());
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
  XctId max_xct_id;
  max_xct_id.set(Epoch::kEpochInitialDurable, 1);  // TODO(Hideaki) not quite..
  bool success = precommit_xct_lock(context, &max_xct_id);  // Phase 1
  // lock can fail only when physical records went too far away
  if (!success) {
    DLOG(INFO) << *context << " Interesting. failed due to records moved too far away";
    return false;
  }

  // BEFORE the first fence, update the in commit epoch for epoch chime.
  // see InCommitEpochGuard class comments for why we need to do this.
  Epoch conservative_epoch = get_current_global_epoch_weak();
  InCommitEpochGuard guard(context->get_in_commit_epoch_address(), conservative_epoch);

  assorted::memory_fence_acq_rel();

  *commit_epoch = get_current_global_epoch_weak();  // serialization point!
  DVLOG(1) << *context << " Acquired read-write commit epoch " << *commit_epoch;

  assorted::memory_fence_acq_rel();
  bool verified = precommit_xct_verify_readwrite(context, &max_xct_id);  // phase 2
#ifndef NDEBUG
  {
    WriteXctAccess* write_set = context->get_current_xct().get_write_set();
    uint32_t        write_set_size = context->get_current_xct().get_write_set_size();
    for (uint32_t i = 0; i < write_set_size; ++i) {
      ASSERT_ND(write_set[i].owner_id_address_->is_keylocked());
      ASSERT_ND(!write_set[i].owner_id_address_->needs_track_moved());
    }
  }
#endif  // NDEBUG
  if (verified) {
    precommit_xct_apply(context, max_xct_id, commit_epoch);  // phase 3. this also unlocks
    // announce log AFTER (with fence) apply, because apply sets xct_order in the logs.
    assorted::memory_fence_release();
    if (engine_->get_options().log_.emulation_.null_device_) {
      context->get_thread_log_buffer().discard_current_xct_log();
    } else {
      context->get_thread_log_buffer().publish_committed_log(*commit_epoch);
    }
  } else {
    precommit_xct_unlock(context);  // just unlock in this case
  }

  return verified;
}


bool XctManagerPimpl::precommit_xct_lock_track_write(WriteXctAccess* entry) {
  ASSERT_ND(entry->owner_id_address_->needs_track_moved());
  storage::StorageManager* st = engine_->get_storage_manager();
  TrackMovedRecordResult result = st->track_moved_record(
    entry->storage_id_,
    entry->owner_id_address_,
    entry);
  if (result.new_owner_address_ == nullptr) {
    // failed to track down even with the write set. this is a quite rare event.
    // in that case, retry the whole transaction.
    DLOG(INFO) << "Failed to track moved record even with write set";
    return false;
  }
  if (entry->related_read_) {
    // also apply the result to related read access so that we don't have to track twice.
    ASSERT_ND(entry->related_read_->related_write_ == entry);
    ASSERT_ND(entry->related_read_->owner_id_address_ == entry->owner_id_address_);
    entry->related_read_->owner_id_address_ = result.new_owner_address_;
  }
  entry->owner_id_address_ = result.new_owner_address_;
  entry->payload_address_ = result.new_payload_address_;
  return true;
}
bool XctManagerPimpl::precommit_xct_verify_track_read(ReadXctAccess* entry) {
  ASSERT_ND(entry->owner_id_address_->needs_track_moved());
  ASSERT_ND(entry->related_write_ == nullptr);  // if there is, lock() should have updated it.
  storage::StorageManager* st = engine_->get_storage_manager();
  TrackMovedRecordResult result = st->track_moved_record(
    entry->storage_id_,
    entry->owner_id_address_,
    entry->related_write_);
  if (result.new_owner_address_ == nullptr) {
    // failed to track down. if entry->related_write_ is null, this always happens when
    // the record is now in next layer. in this case, retry the whole transaction.
    return false;
  }
  entry->owner_id_address_ = result.new_owner_address_;
  return true;
}

bool XctManagerPimpl::precommit_xct_lock(thread::Thread* context, XctId* max_xct_id) {
  Xct& current_xct = context->get_current_xct();
  WriteXctAccess* write_set = current_xct.get_write_set();
  uint32_t        write_set_size = current_xct.get_write_set_size();
  DVLOG(1) << *context << " #write_sets=" << write_set_size << ", addr=" << write_set;

  // we have to access the owner_id's pointed address. let's prefetch them in parallel
  for (uint32_t i = 0; i < write_set_size; ++i) {
    assorted::prefetch_cacheline(write_set[i].owner_id_address_);
  }

  storage::StorageManager* st = engine_->get_storage_manager();
  while (true) {  // while loop for retrying in case of moved-bit error
    ASSERT_ND(current_xct.assert_related_read_write());
    // first, check for moved-bit and track where the corresponding physical record went.
    // we do this before locking, so it is possible that later we find it moved again.
    // if that happens, we retry.
    // we must not do lock-then-track to avoid deadlocks.
    for (uint32_t i = 0; i < write_set_size; ++i) {
      // TODO(Hideaki) if this happens often, this might be too frequent virtual method call.
      // maybe a batched version of this? I'm not sure if this is that often, though.
      WriteXctAccess* entry = write_set + i;
      if (UNLIKELY(entry->owner_id_address_->needs_track_moved())) {
        if (!precommit_xct_lock_track_write(entry)) {
          return false;
        }
      }
    }

    ASSERT_ND(current_xct.assert_related_read_write());
    std::sort(write_set, write_set + write_set_size, WriteXctAccess::compare);
    // after the sorting, the related-link from read-set to write-set is now broken.
    // we fix it by following the back-link from write-set to read-set.
    for (uint32_t i = 0; i < write_set_size; ++i) {
      WriteXctAccess* entry = write_set + i;
      if (entry->related_read_) {
        ASSERT_ND(entry->owner_id_address_ == entry->related_read_->owner_id_address_);
        ASSERT_ND(entry->related_read_->related_write_);
        entry->related_read_->related_write_ = entry;
      }
    }
    ASSERT_ND(current_xct.assert_related_read_write());

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
      DVLOG(2) << *context << " Locking " << st->get_name(write_set[i].storage_id_)
        << ":" << write_set[i].owner_id_address_;
      if (i < write_set_size - 1 &&
        write_set[i].owner_id_address_ == write_set[i + 1].owner_id_address_) {
        DVLOG(0) << *context << " Multiple write sets on record "
          << st->get_name(write_set[i].storage_id_)
          << ":" << write_set[i].owner_id_address_
          << ". Will lock/unlock at the last one";
      } else {
        write_set[i].mcs_block_ = context->mcs_acquire_lock(
          write_set[i].owner_id_address_->get_key_lock());
        if (UNLIKELY(write_set[i].owner_id_address_->needs_track_moved())) {
          VLOG(0) << *context << " Interesting. moved-bit conflict in "
            << st->get_name(write_set[i].storage_id_)
            << ":" << write_set[i].owner_id_address_
            << ". This occasionally happens.";
          // release all locks acquired so far, retry
          precommit_xct_unlock(context);
          needs_retry = true;
          break;
        }
        ASSERT_ND(!write_set[i].owner_id_address_->is_moved());
        ASSERT_ND(!write_set[i].owner_id_address_->is_next_layer());
        ASSERT_ND(write_set[i].owner_id_address_->is_keylocked());
        max_xct_id->store_max(write_set[i].owner_id_address_->xct_id_);
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
  ReadXctAccess*    read_set = current_xct.get_read_set();
  const uint32_t    read_set_size = current_xct.get_read_set_size();
  storage::StorageManager* st = engine_->get_storage_manager();
  for (uint32_t i = 0; i < read_set_size; ++i) {
    ASSERT_ND(read_set[i].related_write_ == nullptr);
    // let's prefetch owner_id in parallel
    if (i % kReadsetPrefetchBatch == 0) {
      for (uint32_t j = i; j < i + kReadsetPrefetchBatch && j < read_set_size; ++j) {
        assorted::prefetch_cacheline(read_set[j].owner_id_address_);
      }
    }

    // The owning transaction has changed.
    // We don't check ordinal here because there is no change we are racing with ourselves.
    ReadXctAccess& access = read_set[i];
    DVLOG(2) << *context << "Verifying " << st->get_name(access.storage_id_)
      << ":" << access.owner_id_address_ << ". observed_xid=" << access.observed_owner_id_
        << ", now_xid=" << access.owner_id_address_->xct_id_;
    if (UNLIKELY(access.owner_id_address_->needs_track_moved())) {
      if (!precommit_xct_verify_track_read(&access)) {
        return false;
      }
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
    *commit_epoch = Epoch(engine_->get_log_manager()->get_durable_global_epoch_weak());
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

bool XctManagerPimpl::precommit_xct_verify_readwrite(thread::Thread* context, XctId* max_xct_id) {
  Xct& current_xct = context->get_current_xct();
  ReadXctAccess*          read_set = current_xct.get_read_set();
  const uint32_t          read_set_size = current_xct.get_read_set_size();
  storage::StorageManager* st = engine_->get_storage_manager();
  for (uint32_t i = 0; i < read_set_size; ++i) {
    // let's prefetch owner_id in parallel
    if (i % kReadsetPrefetchBatch == 0) {
      for (uint32_t j = i; j < i + kReadsetPrefetchBatch && j < read_set_size; ++j) {
        assorted::prefetch_cacheline(read_set[j].owner_id_address_);
      }
    }

    // The owning transaction has changed.
    // We don't check ordinal here because there is no change we are racing with ourselves.
    ReadXctAccess& access = read_set[i];
    DVLOG(2) << *context << " Verifying " << st->get_name(access.storage_id_)
      << ":" << access.owner_id_address_ << ". observed_xid=" << access.observed_owner_id_
        << ", now_xid=" << access.owner_id_address_->xct_id_;

    // read-set has to also track moved records.
    // however, unlike write-set locks, we don't have to do retry-loop.
    // if the rare event (yet another concurrent split) happens, we just abort the transaction.
    if (UNLIKELY(access.owner_id_address_->needs_track_moved())) {
      if (!precommit_xct_verify_track_read(&access)) {
        return false;
      }
    }

    if (access.observed_owner_id_ != access.owner_id_address_->xct_id_) {
      DLOG(WARNING) << *context << " read set changed by other transaction. will abort";
      return false;
    }
    max_xct_id->store_max(access.observed_owner_id_);
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

void XctManagerPimpl::precommit_xct_apply(
  thread::Thread* context,
  XctId max_xct_id,
  Epoch *commit_epoch) {
  Xct& current_xct = context->get_current_xct();
  WriteXctAccess* write_set = current_xct.get_write_set();
  uint32_t        write_set_size = current_xct.get_write_set_size();
  LockFreeWriteXctAccess* lock_free_write_set = current_xct.get_lock_free_write_set();
  uint32_t                lock_free_write_set_size = current_xct.get_lock_free_write_set_size();
  DVLOG(1) << *context << " applying and unlocking.. write_set_size=" << write_set_size
    << ", lock_free_write_set_size=" << lock_free_write_set_size;

  current_xct.issue_next_id(max_xct_id, commit_epoch);
  XctId new_xct_id = current_xct.get_id();
  ASSERT_ND(new_xct_id.get_epoch() == *commit_epoch);
  ASSERT_ND(new_xct_id.get_ordinal() > 0);
  new_xct_id.clear_status_bits();
  XctId new_deleted_xct_id = new_xct_id;
  new_deleted_xct_id.set_deleted();  // used if the record after apply is in deleted state.

  DVLOG(1) << *context << " generated new xct id=" << new_xct_id;
  for (uint32_t i = 0; i < write_set_size; ++i) {
    WriteXctAccess& write = write_set[i];
    DVLOG(2) << *context << " Applying/Unlocking "
      << engine_->get_storage_manager()->get_name(write.storage_id_)
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
      write.storage_id_,
      write.owner_id_address_,
      write.payload_address_);
    ASSERT_ND(!write.owner_id_address_->xct_id_.get_epoch().is_valid() ||
      write.owner_id_address_->xct_id_.before(new_xct_id));  // ordered correctly?
    if (i < write_set_size - 1 &&
      write.owner_id_address_ == write_set[i + 1].owner_id_address_) {
      DVLOG(0) << *context << " Multiple write sets on record "
        << engine_->get_storage_manager()->get_name(write_set[i].storage_id_)
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
    DVLOG(2) << *context << " Applying Lock-Free write "
      << engine_->get_storage_manager()->get_name(write.storage_id_);
    write.log_entry_->header_.set_xct_id(new_xct_id);
    log::invoke_apply_record(write.log_entry_, context, write.storage_id_, nullptr, nullptr);
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
      DVLOG(2) << *context << " Unlocking "
        << engine_->get_storage_manager()->get_name(write.storage_id_) << ":"
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
  DVLOG(1) << *context << " Aborted transaction in thread-" << context->get_thread_id();
  current_xct.deactivate();
  context->get_thread_log_buffer().discard_current_xct_log();
  return kErrorCodeOk;
}

}  // namespace xct
}  // namespace foedus
