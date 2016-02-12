/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
#include "foedus/cache/cache_manager.hpp"
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
#include "foedus/xct/retrospective_lock_list.hpp"
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
Epoch       XctManager::get_current_grace_epoch() const {
  return pimpl_->get_current_global_epoch().one_less();
}
Epoch       XctManager::get_current_grace_epoch_weak() const {
  return pimpl_->get_current_global_epoch_weak().one_less();
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
  // See CacheManager's comments for why we have to stop the cleaner here
  CHECK_ERROR(engine_->get_cache_manager()->stop_cleaner());
  if (engine_->is_master()) {
    if (epoch_chime_thread_.joinable()) {
      {
        control_block_->epoch_chime_terminate_requested_ = true;
        control_block_->epoch_chime_wakeup_.signal();
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
  uint64_t interval_microsec = engine_->get_options().xct_.epoch_advance_interval_ms_ * 1000ULL;
  LOG(INFO) << "epoch_chime_thread now starts processing. interval_microsec=" << interval_microsec;
  while (!is_stop_requested()) {
    {
      uint64_t demand = control_block_->epoch_chime_wakeup_.acquire_ticket();
      if (is_stop_requested()) {
        break;
      }
      if (get_requested_global_epoch() <= get_current_global_epoch())  {  // otherwise no sleep
        bool signaled = control_block_->epoch_chime_wakeup_.timedwait(
          demand,
          interval_microsec,
          soc::kDefaultPollingSpins,
          interval_microsec);
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
      // soc::SharedMutexScope scope(control_block_->current_global_epoch_advanced_.get_mutex());
      // There is only one thread (this) that might update current_global_epoch_, so
      // no mutex needed. just set it and put fence.
      control_block_->current_global_epoch_ = get_current_global_epoch().one_more().value();
      assorted::memory_fence_release();
      control_block_->current_global_epoch_advanced_.signal();
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
  control_block_->epoch_chime_wakeup_.signal();  // hurrrrry up!
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
      uint64_t demand = control_block_->current_global_epoch_advanced_.acquire_ticket();
      if (get_current_global_epoch() < request) {
        control_block_->current_global_epoch_advanced_.wait(demand);
      }
    }
  }

  LOG(INFO) << "epoch advanced. current_global_epoch_=" << get_current_global_epoch();
}

void XctManagerPimpl::wait_for_current_global_epoch(Epoch target_epoch, int64_t wait_microseconds) {
  // this method doesn't aggressively wake up the epoch-advance thread. it just waits.
  set_requested_global_epoch(target_epoch);
  while (get_current_global_epoch() < target_epoch) {
    uint64_t demand = control_block_->current_global_epoch_advanced_.acquire_ticket();
    if (get_current_global_epoch() < target_epoch) {
      if (wait_microseconds < 0) {
        control_block_->current_global_epoch_advanced_.wait(demand);
      } else {
        control_block_->current_global_epoch_advanced_.timedwait(demand, wait_microseconds);
        return;
      }
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
  DVLOG(1) << *context << " Began new transaction."
    << " RLL size=" << current_xct.get_retrospective_lock_list()->get_last_active_entry();
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

  ErrorCode result;
  bool read_only = context->get_current_xct().is_read_only();
  if (read_only) {
    result = precommit_xct_readonly(context, commit_epoch);
  } else {
    result = precommit_xct_readwrite(context, commit_epoch);
  }

  ASSERT_ND(current_xct.assert_related_read_write());
  if (result != kErrorCodeOk) {
    ErrorCode abort_ret = abort_xct(context);
    ASSERT_ND(abort_ret == kErrorCodeOk);
    DVLOG(1) << *context << " Aborting because of contention";
  } else {
    current_xct.get_retrospective_lock_list()->clear_entries();
    release_and_clear_all_current_locks(context);
    current_xct.deactivate();
  }
  ASSERT_ND(current_xct.get_current_lock_list()->is_empty());
  return result;
}
ErrorCode XctManagerPimpl::precommit_xct_readonly(thread::Thread* context, Epoch *commit_epoch) {
  DVLOG(1) << *context << " Committing read_only";
  ASSERT_ND(context->get_thread_log_buffer().get_offset_committed() ==
      context->get_thread_log_buffer().get_offset_tail());
  *commit_epoch = Epoch();
  assorted::memory_fence_acquire();  // this is enough for read-only case
  if (precommit_xct_verify_readonly(context, commit_epoch)) {
    return kErrorCodeOk;
  } else {
    return kErrorCodeXctRaceAbort;
  }
}

ErrorCode XctManagerPimpl::precommit_xct_readwrite(thread::Thread* context, Epoch *commit_epoch) {
  DVLOG(1) << *context << " Committing read-write";
  XctId max_xct_id;
  max_xct_id.set(Epoch::kEpochInitialDurable, 1);  // TODO(Hideaki) not quite..
  auto lock_ret = precommit_xct_lock(context, &max_xct_id);  // Phase 1
  if (lock_ret != kErrorCodeOk) {
    return lock_ret;
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
      ASSERT_ND(write_set[i].log_entry_);
      ASSERT_ND(write_set[i].payload_address_);
    }
  }
#endif  // NDEBUG
  if (verified) {
    precommit_xct_apply(context, max_xct_id, commit_epoch);  // phase 3. this does NOT unlock
    // announce log AFTER (with fence) apply, because apply sets xct_order in the logs.
    assorted::memory_fence_release();
    if (engine_->get_options().log_.emulation_.null_device_) {
      context->get_thread_log_buffer().discard_current_xct_log();
    } else {
      context->get_thread_log_buffer().publish_committed_log(*commit_epoch);
    }
    return kErrorCodeOk;
  }
  return kErrorCodeXctRaceAbort;
}


bool XctManagerPimpl::precommit_xct_lock_track_write(
  thread::Thread* /*context*/, WriteXctAccess* entry) {
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

bool XctManagerPimpl::precommit_xct_verify_track_read(
  thread::Thread* /*context*/, ReadXctAccess* entry) {
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

void XctManagerPimpl::precommit_xct_sort_access(thread::Thread* context) {
  Xct& current_xct = context->get_current_xct();
  WriteXctAccess* write_set = current_xct.get_write_set();
  uint32_t        write_set_size = current_xct.get_write_set_size();

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
    entry->write_set_ordinal_ = i;
  }
  ASSERT_ND(current_xct.assert_related_read_write());

#ifndef NDEBUG
  ASSERT_ND(current_xct.assert_related_read_write());
  // check that write sets are now sorted.
  // when address is the same, they must be ordered by the order they are created.
  // eg: repeated overwrites to one record should be applied in the order the user issued.
  for (uint32_t i = 1; i < write_set_size; ++i) {
    ASSERT_ND(write_set[i - 1].write_set_ordinal_ != write_set[i].write_set_ordinal_);
    if (write_set[i].owner_id_address_ == write_set[i - 1].owner_id_address_) {
      ASSERT_ND(write_set[i - 1].write_set_ordinal_ < write_set[i].write_set_ordinal_);
    } else {
      ASSERT_ND(reinterpret_cast<uintptr_t>(write_set[i - 1].owner_id_address_)
        < reinterpret_cast<uintptr_t>(write_set[i].owner_id_address_));
    }
  }
#endif  // NDEBUG
}

ErrorCode XctManagerPimpl::precommit_xct_lock_batch_track_moved(thread::Thread* context) {
  Xct& current_xct = context->get_current_xct();
  WriteXctAccess* write_set = current_xct.get_write_set();
  uint32_t        write_set_size = current_xct.get_write_set_size();
  uint32_t moved_count = 0;
  for (uint32_t i = 0; i < write_set_size; ++i) {
    WriteXctAccess* entry = write_set + i;
    auto* rec = entry->owner_id_address_;
    if (UNLIKELY(rec->needs_track_moved())) {
      if (!precommit_xct_lock_track_write(context, entry)) {
        DLOG(INFO) << "Failed to track moved record?? this must be very rare";
        return kErrorCodeXctRaceAbort;
      }
      ASSERT_ND(entry->owner_id_address_ != rec);
      ++moved_count;
    }
  }
  DVLOG(1) << "Tracked " << moved_count << " moved records in precommit_xct_lock.";
  if (moved_count > 100U) {
    LOG(INFO) << "Tracked " << moved_count << " moved records in precommit_xct_lock."
      << " That's a lot. maybe this is a batch-loading transaction?";
  }
  return kErrorCodeOk;
}

ErrorCode XctManagerPimpl::precommit_xct_lock(thread::Thread* context, XctId* max_xct_id) {
  Xct& current_xct = context->get_current_xct();
  WriteXctAccess* write_set = current_xct.get_write_set();
  uint32_t        write_set_size = current_xct.get_write_set_size();
  CurrentLockList* cll = current_xct.get_current_lock_list();
  const bool force_canonical
    = context->get_engine()->get_options().xct_.force_canonical_xlocks_in_precommit_;
  DVLOG(1) << *context << " #write_sets=" << write_set_size << ", addr=" << write_set;

#ifndef NDEBUG
  // Initially, write-sets must be ordered by the insertion order.
  for (uint32_t i = 0; i < write_set_size; ++i) {
    ASSERT_ND(write_set[i].write_set_ordinal_ == i);
    // Because of RLL, the write-set might or might not already have a corresponding lock
  }
#endif  // NDEBUG

moved_retry:
  CHECK_ERROR_CODE(precommit_xct_lock_batch_track_moved(context));
  precommit_xct_sort_access(context);

  // TODO(Hideaki) Because of how the new locks work, I'm not sure the prefetch still helps.
  // Previously it did, but not significant either, so let's disable for now and revisit this later.
  // we have to access the owner_id's pointed address. let's prefetch them in parallel
  // for (uint32_t i = 0; i < write_set_size; ++i) {
  //   assorted::prefetch_cacheline(write_set[i].owner_id_address_);
  // }

  // Create entries in CLL for all write sets. At this point they are not locked yet.
  cll->batch_insert_write_placeholders(write_set, write_set_size);

  ASSERT_ND(current_xct.assert_related_read_write());
  // Note: one alterantive is to sequentailly iterate over write-set and CLL,
  // both of which are sorted. It will be faster, but probably not that different
  // unless we have a large number of locks. For now binary_search each time.

  // Both write-set and CLL are sorted in canonical order. Simply iterate over in order.
  // This is way faster than invoking cll->binary_search() for each write-set entry.
  // Remember one thing, tho: write-set might have multiple entries for one record!
  LockListPosition last_locked_pos = cll->get_last_locked_entry();
  for (CurrentLockListIteratorForWriteSet it(write_set, cll, write_set_size);
        it.is_valid();
        it.next_writes()) {
    // for multiple writes on one record, only the first one (write_cur_pos_) takes the lock
    WriteXctAccess* entry = write_set + it.write_cur_pos_;

    LockListPosition lock_pos = it.cll_pos_;
    ASSERT_ND(lock_pos != kLockListPositionInvalid);  // we have put placeholders for all!
    LockEntry* lock_entry = cll->get_array() + lock_pos;
    ASSERT_ND(lock_entry->lock_ == entry->owner_id_address_);
    ASSERT_ND(lock_entry->preferred_mode_ == kWriteLock);
    if (lock_entry->taken_mode_ == kWriteLock) {
      DVLOG(2) << "Yay, already taken. Probably Thanks to RLL?";
    } else {
      // We need to take or upgrade the lock.
      // This might return kErrorCodeXctRaceAbort when we are not in canonical mode and
      // we could not immediately acquire the lock.
      if (force_canonical &&
        (last_locked_pos != kLockListPositionInvalid && last_locked_pos >= lock_pos)) {
        // We are not in canonical mode. Let's aggressively restore canonical mode.
        DVLOG(0) << "Aggressively releasing locks to restore canonical mode in precommit";
        context->mcs_release_all_current_locks_after(lock_entry->universal_lock_id_ - 1U);
        last_locked_pos = cll->get_last_locked_entry();
        ASSERT_ND(last_locked_pos == kLockListPositionInvalid || last_locked_pos < lock_pos);
      }
      CHECK_ERROR_CODE(cll->try_or_acquire_single_lock_impl(context, lock_pos, &last_locked_pos));
    }

    if (UNLIKELY(entry->owner_id_address_->needs_track_moved())) {
      // Because we invoked precommit_xct_lock_batch_track_moved beforehand,
      // this happens only when a concurrent thread again split some of the overlapping page.
      // Though rare, it happens. In that case redo the procedure.
      DLOG(INFO) << "Someone has split the page and moved our record after we check. Retry..";
      goto moved_retry;
    }

    ASSERT_ND(!entry->owner_id_address_->is_moved());
    ASSERT_ND(!entry->owner_id_address_->is_next_layer());
    ASSERT_ND(entry->owner_id_address_->is_keylocked());
    max_xct_id->store_max(entry->owner_id_address_->xct_id_);

    // If we have to abort, we should abort early to not waste time.
    // Thus, we check related read sets right here.
    // For other writes of the same record, too.
    for (uint32_t rec = it.write_cur_pos_; rec < it.write_next_pos_; ++rec) {
      WriteXctAccess* r = write_set + rec;
      ASSERT_ND(entry->owner_id_address_ == r->owner_id_address_);
      if (r->related_read_) {
        ASSERT_ND(r->related_read_->owner_id_address_ == r->owner_id_address_);
        if (r->owner_id_address_->xct_id_ != r->related_read_->observed_owner_id_) {
          return kErrorCodeXctRaceAbort;
        }
      }
    }
  }

  DVLOG(1) << *context << " locked write set";
#ifndef NDEBUG
  for (uint32_t i = 0; i < write_set_size; ++i) {
    ASSERT_ND(write_set[i].owner_id_address_->is_keylocked());
  }
#endif  // NDEBUG
  return kErrorCodeOk;
}

const uint16_t kReadsetPrefetchBatch = 16;

bool XctManagerPimpl::precommit_xct_verify_readonly(thread::Thread* context, Epoch *commit_epoch) {
  Xct& current_xct = context->get_current_xct();
  ReadXctAccess*    read_set = current_xct.get_read_set();
  const uint32_t    read_set_size = current_xct.get_read_set_size();
  storage::StorageManager* st = engine_->get_storage_manager();
  bool verification_failed = false;
  for (uint32_t i = 0; i < read_set_size; ++i) {
    ASSERT_ND(read_set[i].related_write_ == nullptr);
    // let's prefetch owner_id in parallel
    if (i % kReadsetPrefetchBatch == 0) {
      for (uint32_t j = i; j < i + kReadsetPrefetchBatch && j < read_set_size; ++j) {
        assorted::prefetch_cacheline(read_set[j].owner_id_address_);
      }
    }

    ReadXctAccess& access = read_set[i];
    DVLOG(2) << *context << "Verifying " << st->get_name(access.storage_id_)
      << ":" << access.owner_id_address_ << ". observed_xid=" << access.observed_owner_id_
        << ", now_xid=" << access.owner_id_address_->xct_id_;
    if (UNLIKELY(access.observed_owner_id_.is_being_written())) {
      // safety net. observing this case.
      // hm, this should be checked and retried during transaction.
      // probably there still is some code to forget that.
      // At least safe to abort here, so keep it this way for now.
      DLOG(WARNING) << *context << "?? this should have been checked. being_written! will abort";
      return false;
    }

    // Implementation Note: we do verify the versions whether we took a lock on this record or not.
    // In a sentence, this is the simplest and most reliable while wasted cost is not that much.

    // In a paragraph.. We could check whether it's locked, and in \e some case skip verification,
    // but think of case 1) read A without read-lock, later take a write-lock on A due to RLL.
    // also case 2) read A without read-lock, then read A again but this time with read-lock.
    // Yes, we could rule these cases out by checking something for each read/write,
    // but that's fragile. too much complexity for little. we just verify always. period.
    if (UNLIKELY(access.owner_id_address_->needs_track_moved())) {
      if (!precommit_xct_verify_track_read(context, &access)) {
        return false;
      }
    }
    if (access.observed_owner_id_ != access.owner_id_address_->xct_id_) {
      DLOG(WARNING) << *context << " read set changed by other transaction. will abort";
      // read clobbered, make it hotter
      access.owner_id_address_->hotter(context);

      // We go on to other read-sets to make them hotter, too.
      // This might sound like a bit of wasted effort, but anyway aborts must be reasonably
      // infrequent (otherwise we are screwed!) so this doesn't add too much.
      // Rather, we want to quickly mark problemetic pages as hot.
      // return false;
      verification_failed = true;
      continue;
    }

    // Remembers the highest epoch observed.
    commit_epoch->store_max(access.observed_owner_id_.get_epoch());
  }

  if (verification_failed) {
    return false;
  }

  DVLOG(1) << *context << "Read-only higest epoch observed: " << *commit_epoch;
  if (!commit_epoch->is_valid()) {
    DVLOG(1) << *context
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
  CurrentLockList*        cll = current_xct.get_current_lock_list();
  bool verification_failed = false;
  for (uint32_t i = 0; i < read_set_size; ++i) {
    // let's prefetch owner_id in parallel
    if (i % kReadsetPrefetchBatch == 0) {
      for (uint32_t j = i; j < i + kReadsetPrefetchBatch && j < read_set_size; ++j) {
        if (read_set[j].related_write_ == nullptr) {
          assorted::prefetch_cacheline(read_set[j].owner_id_address_);
        }
      }
    }
    // The owning transaction has changed.
    // We don't check ordinal here because there is no change we are racing with ourselves.
    ReadXctAccess& access = read_set[i];
    if (UNLIKELY(access.observed_owner_id_.is_being_written())) {
      // same as above.
      DLOG(WARNING) << *context << "?? this should have been checked. being_written! will abort";
      return false;
    }
    storage::StorageManager* st = engine_->get_storage_manager();
    if (access.related_write_) {
      // we already checked this in lock()
      DVLOG(3) << *context << " skipped read-sets that are already checked";
      ASSERT_ND(access.observed_owner_id_ == access.owner_id_address_->xct_id_);
      continue;
    }
    DVLOG(2) << *context << " Verifying " << st->get_name(access.storage_id_)
      << ":" << access.owner_id_address_ << ". observed_xid=" << access.observed_owner_id_
        << ", now_xid=" << access.owner_id_address_->xct_id_;
    // As noted in precommit_xct_verify_readonly, we verify read-set whether it's locked or not.

    // read-set has to also track moved records.
    // however, unlike write-set locks, we don't have to do retry-loop.
    // if the rare event (yet another concurrent split) happens, we just abort the transaction.
    if (UNLIKELY(access.owner_id_address_->needs_track_moved())) {
      if (!precommit_xct_verify_track_read(context, &access)) {
        return false;
      }
    }

    if (access.observed_owner_id_ != access.owner_id_address_->xct_id_) {
      access.owner_id_address_->hotter(context);
      DVLOG(1) << *context << " read set changed by other transaction. will abort";
      // same as read_only
      verification_failed = true;
      continue;
    }

    /*
    // Hideaki[2016Feb] I think I remember why I kept this here. When we didn't have the
    // "being_written" flag, we did need it even after splitting XID/TID.
    // But, now that we have it in XID, it's surely safe without this.
    // Still.. in case I miss something, I leave it here commented out.

    // Hideaki: Umm, after several changes, I'm now not sure if we still need this check.
    // As far as XID hasn't changed, do we care whether others locked it or not?
    // Thanks to the separation of XID and Lock-word, I think this is now unnecessary.
    // If it's our own lock, we haven't applied our changes yet, so safe. If it's other's lock,
    // why do we care as far as XID hasn'nt changed? Everyone updates XID -> unlocks in this order.
    // Anyways, we are in the course of a bigger change, so revisit it later.
    if (access.owner_id_address_->is_keylocked()) {
      DVLOG(2) << *context
        << " read set contained a locked record. was it myself who locked it?";
      LockListPosition my_lock_pos = cll->binary_search(access.owner_id_address_);
      if (my_lock_pos != kLockListPositionInvalid && cll->get_array()[my_lock_pos].is_locked()) {
        DVLOG(2) << *context << " okay, myself. go on.";
      } else {
        DVLOG(1) << *context << " no, not me. will abort";
        return false;
      }
    }
    */
    max_xct_id->store_max(access.observed_owner_id_);
  }

  // Check Page Pointer/Version
  if (verification_failed) {
    return false;
  }

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
  DVLOG(1) << *context << " applying.. write_set_size=" << write_set_size
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
    DVLOG(2) << *context << " Applying "
      << engine_->get_storage_manager()->get_name(write.storage_id_)
      << ":" << write.owner_id_address_;
    ASSERT_ND(write.owner_id_address_->is_keylocked());

    // We must be careful on the memory order of unlock and data write.
    // We must write data first (invoke_apply), then unlock.
    // Otherwise the correctness is not guaranteed.
    ASSERT_ND(write.log_entry_);
    write.log_entry_->header_.set_xct_id(new_xct_id);
    ASSERT_ND(new_xct_id.get_epoch().is_valid());
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
    } else {
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
      // This method does NOT unlock. we do it at the end, all locks together.
      // This is another difference from SILO.
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

ErrorCode XctManagerPimpl::abort_xct(thread::Thread* context) {
  Xct& current_xct = context->get_current_xct();
  if (!current_xct.is_active()) {
    return kErrorCodeXctNoXct;
  }
  DVLOG(1) << *context << " Aborted transaction in thread-" << context->get_thread_id();

  // When we abort, whether in precommit or via user's explicit abort, we construct RLL.
  // Abort may happen due to try-failure in reads, so we now put this in here, not precommit.
  // One drawback is that we can't check the "cause" of the abort.
  // We should probably construct RLL only in the case of race-abort.
  // So, we might revist this choice.
  if (engine_->get_options().xct_.enable_retrospective_lock_list_) {
    const uint32_t hot_threashold = engine_->get_options().storage_.hot_threshold_;
    const uint32_t conservative_threashold = (hot_threashold < 1U ? 0 : 1U);
    current_xct.get_retrospective_lock_list()->construct(context, conservative_threashold);
  } else {
    current_xct.get_retrospective_lock_list()->clear_entries();
  }

  release_and_clear_all_current_locks(context);
  current_xct.deactivate();
  context->get_thread_log_buffer().discard_current_xct_log();
  return kErrorCodeOk;
}

void XctManagerPimpl::release_and_clear_all_current_locks(thread::Thread* context) {
  context->mcs_release_all_current_locks_after(kNullUniversalLockId);
  CurrentLockList* cll = context->get_current_xct().get_current_lock_list();
  cll->clear_entries();
}

}  // namespace xct
}  // namespace foedus
