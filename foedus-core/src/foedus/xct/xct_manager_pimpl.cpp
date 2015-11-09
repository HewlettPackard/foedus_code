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

#ifndef NDEBUG
  ReadXctAccess* read_set = current_xct.get_read_set();
  for (uint32_t i = 0; i < current_xct.get_read_set_size(); i++) {
    ASSERT_ND(read_set[i].mcs_block_ == 0);
  }
  WriteXctAccess* write_set = current_xct.get_write_set();
  for (uint32_t i = 0; i < current_xct.get_write_set_size(); i++) {
    ASSERT_ND(write_set[i].mcs_block_ == 0);
  }
#endif  // NDEBUG

  ASSERT_ND(current_xct.assert_related_read_write());
  current_xct.deactivate();
  if (success) {
    return kErrorCodeOk;
  } else {
    //DLOG(WARNING) << *context << " Aborting because of contention";
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
    //DLOG(INFO) << *context << " Interesting. failed due to records moved far away or early abort";
    precommit_xct_unlock(context, true);
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
      ASSERT_ND(write_set[i].log_entry_);
      ASSERT_ND(write_set[i].payload_address_);
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
  }
  precommit_xct_unlock(context, true);  // for S-locks too
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

const int kLockAcquireRetries = 1;
xct::McsBlockIndex XctManagerPimpl::precommit_xct_upgrade_lock(
  thread::Thread* context, ReadXctAccess* read) {
  for (int i = 0; i < kLockAcquireRetries; i++) {
    auto block_index = context->mcs_try_upgrade_reader_lock(
      read->owner_id_address_->get_key_lock(),
      read->mcs_block_);
    if (block_index > 0) {
      return block_index;
    }
  }
  return 0;
}

bool XctManagerPimpl::precommit_xct_acquire_writer_lock(
  thread::Thread* context,
  WriteXctAccess *write) {
  for (int i = 0; i < kLockAcquireRetries; i++) {
    write->mcs_block_ = context->mcs_try_acquire_writer_lock(
      write->owner_id_address_->get_key_lock());
    if (write->mcs_block_) {
      return true;
    }
  }
  ASSERT_ND(write->mcs_block_ == 0);
  return false;
}

void XctManagerPimpl::precommit_xct_sort_access(
  thread::Thread* context,
  WriteXctAccess* write_set,
  uint32_t write_set_size,
  ReadXctAccess* read_set,
  uint32_t read_set_size) {
  Xct& current_xct = context->get_current_xct();
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

  std::sort(read_set, read_set + read_set_size, ReadXctAccess::compare);
  for (uint32_t i = 0; i < read_set_size; ++i) {
    ReadXctAccess* entry = read_set + i;
    if (entry->related_write_) {
      ASSERT_ND(entry->owner_id_address_ == entry->related_write_->owner_id_address_);
      ASSERT_ND(entry->related_write_->related_read_);
      entry->related_write_->related_read_ = entry;
    }
  }
  ASSERT_ND(current_xct.assert_related_read_write());

#ifndef NDEBUG
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

bool XctManagerPimpl::precommit_xct_lock(thread::Thread* context, XctId* max_xct_id) {
  Xct& current_xct = context->get_current_xct();
  WriteXctAccess* write_set = current_xct.get_write_set();
  ReadXctAccess* read_set = current_xct.get_read_set();
  uint32_t        write_set_size = current_xct.get_write_set_size();
  uint32_t        read_set_size = current_xct.get_read_set_size();
  DVLOG(1) << *context << " #write_sets=" << write_set_size << ", addr=" << write_set;

#ifndef NDEBUG
  // Initially, write-sets must be ordered by the insertion order.
  for (uint32_t i = 0; i < write_set_size; ++i) {
    ASSERT_ND(write_set[i].write_set_ordinal_ == i);
  }
#endif  // NDEBUG

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
      // TASK(Hideaki) if this happens often, this might be too frequent virtual method call.
      // maybe a batched version of this? I'm not sure if this is that often, though.
      WriteXctAccess* entry = write_set + i;
      ASSERT_ND(entry->owner_id_address_);
      if (UNLIKELY(entry->owner_id_address_->needs_track_moved())) {
        if (!precommit_xct_lock_track_write(entry)) {
          return false;
        }
      }
    }

    precommit_xct_sort_access(context, write_set, write_set_size, read_set, read_set_size);

    // One differences from original SILO protocol.
    // As there might be multiple write sets on one record, we check equality of next
    // write set and lock/unlock only at the last write-set of the record.
    bool needs_retry = false;
    uint32_t read_set_index = 0;
    uint32_t write_set_index = 0;
    ReadXctAccess* read_entry = NULL;

    while (write_set_index < write_set_size) {
      WriteXctAccess* write_entry = write_set + write_set_index;
      ASSERT_ND(write_entry->owner_id_address_);
      DVLOG(2) << *context << " Locking " << st->get_name(write_entry->storage_id_)
        << ":" << write_entry->owner_id_address_;
      // Fast forward to the last repeated read/write for the same record
      if (write_set_index < write_set_size - 1 &&
        write_entry->owner_id_address_ == write_set[write_set_index + 1].owner_id_address_) {
        ASSERT_ND(write_entry->mcs_block_ == 0);  // haven't X-locked it yet
        DVLOG(0) << *context << " Multiple write sets on record "
          << st->get_name(write_entry->storage_id_)
          << ":" << write_entry->owner_id_address_
          << ". Will lock/unlock at the last one";
        write_set_index++;
        continue;
      }

      // Try to find the matching read
      while (read_set_index < read_set_size) {
        // Fast forward to the last repeated read
        read_entry = read_set + read_set_index;
        if (read_set_index < read_set_size -1 &&
          read_entry->owner_id_address_ == read_set[read_set_index + 1].owner_id_address_) {
          read_entry->mcs_block_ = 0;
        } else if (write_entry->owner_id_address_ <= read_entry->owner_id_address_) {
          read_set_index++;  // for next write-set entry
          break;
        }
        read_set_index++;
      }

      // Acquire as a writer or upgrade?
      if (read_entry->owner_id_address_ == write_entry->owner_id_address_ &&
        read_entry->mcs_block_) {
        auto writer_block = precommit_xct_upgrade_lock(context, read_entry);
        if (writer_block == 0) {
          ASSERT_ND(read_entry->mcs_block_);
          return false;
        }
        write_entry->mcs_block_ = writer_block;
        ASSERT_ND(writer_block != read_entry->mcs_block_);
        read_entry->mcs_block_ = 0;
      } else {
        // like normal OCC, try to acquire as a writer
        if (!precommit_xct_acquire_writer_lock(context, write_entry)) {
          return false;
        }
      }
      ASSERT_ND(write_entry->mcs_block_);
      ASSERT_ND(read_entry->mcs_block_ == 0 || read_entry->mcs_block_ == write_entry->mcs_block_);

      if (UNLIKELY(write_entry->owner_id_address_->needs_track_moved())) {
        VLOG(0) << *context << " Interesting. moved-bit conflict in "
          << st->get_name(write_entry->storage_id_)
          << ":" << write_entry->owner_id_address_
          << ". This occasionally happens.";
        // release all locks acquired so far, retry
        // XXX(tzwang): if we unlock all writes, we need to do so for all reads
        // as well, otherwise we violate 2PL. So we're doing OCC now.
        precommit_xct_unlock(context, true);
        needs_retry = true;
        break;
      }
      ASSERT_ND(!write_entry->owner_id_address_->is_moved());
      ASSERT_ND(!write_entry->owner_id_address_->is_next_layer());
      ASSERT_ND(write_entry->owner_id_address_->is_keylocked());
      max_xct_id->store_max(write_entry->owner_id_address_->xct_id_);

      // If we have to abort, we should abort early to not waste time.
      // Thus, we check related read sets right here.
      if (write_entry->related_read_) {
        ASSERT_ND(write_entry->related_read_->owner_id_address_ == write_entry->owner_id_address_);
        if (write_entry->owner_id_address_->xct_id_ !=
          write_entry->related_read_->observed_owner_id_) {
          //DLOG(WARNING) << *context << " related read set changed. abort early";
          return false;
        }
      }
      write_set_index++;
    }

    if (!needs_retry) {
      break;
    }
  }

  DVLOG(1) << *context << " locked write set";
#ifndef NDEBUG
  for (uint32_t i = 0; i < write_set_size; ++i) {
    ASSERT_ND(write_set[i].owner_id_address_->lock_.is_locked());
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
    // Fast forward to the last one
    if (i < read_set_size - 1 &&
      access.owner_id_address_ == read_set[i + 1].owner_id_address_) {
      // We should have already cleared it during xct_lock
      ASSERT_ND(access.mcs_block_ == 0);
      continue;
    }
    storage::StorageManager* st = engine_->get_storage_manager();
    const WriteXctAccess*   write_set = current_xct.get_write_set();
    const uint32_t          write_set_size = current_xct.get_write_set_size();
    if (access.related_write_) {
      // we already checked this in lock()
      DVLOG(3) << *context << " skipped read-sets that are already checked";
      ASSERT_ND(access.observed_owner_id_ == access.owner_id_address_->xct_id_);
      continue;
    }
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
    if (access.owner_id_address_->is_keylocked()) {
      // Might be myself S-locked, or myself X-locked, or somebody else S/X-locked it.
      DVLOG(2) << *context
        << " read set contained a locked record. was it myself who locked it?";
      if (access.mcs_block_) {
        // Definitely myself, and it must be S-locked, because otherwise we should have
        // cleared this field during xct_lock (when upgrading to X-lock). Unlock here as well.
        context->mcs_release_reader_lock(
          access.owner_id_address_->get_key_lock(),
          access.mcs_block_);
        access.mcs_block_ = 0;
      } else {
        // Is it myself who X-locked during precommit after read it without taking S-lock?
        // write set is sorted. so we can do binary search.
        WriteXctAccess dummy;
        dummy.owner_id_address_ = access.owner_id_address_;
        dummy.write_set_ordinal_ = 0;  // will catch the smallest possible of the address
        const WriteXctAccess* lower_it = std::lower_bound(
          write_set,
          write_set + write_set_size,
          dummy,
          WriteXctAccess::compare);
        if (lower_it == write_set + write_set_size) {
          DLOG(WARNING) << *context << " no, not me. will abort";
          return false;
        }
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
      ASSERT_ND(write.mcs_block_);
      context->mcs_release_writer_lock(write.owner_id_address_->get_key_lock(), write.mcs_block_);
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

void XctManagerPimpl::precommit_xct_unlock(thread::Thread* context, bool sorted) {
  WriteXctAccess* write_set = context->get_current_xct().get_write_set();
  uint32_t        write_set_size = context->get_current_xct().get_write_set_size();
  ReadXctAccess*  read_set = context->get_current_xct().get_read_set();
  uint32_t        read_set_size = context->get_current_xct().get_read_set_size();
  if (!sorted) {
    precommit_xct_sort_access(context, write_set, write_set_size, read_set, read_set_size);
  }
  DVLOG(1) << *context << " unlocking without applying.. write_set_size=" << write_set_size;
  assorted::memory_fence_release();

  uint32_t read_set_index = 0;
  uint32_t write_set_index = 0;
  ReadXctAccess* read_entry = read_set + read_set_index;

  while (write_set_index < write_set_size) {
    WriteXctAccess* write_entry = write_set + write_set_index;

    // Fast forward to the last repeated read/write for the same record
    if (write_set_index < write_set_size - 1 &&
      write_entry->owner_id_address_ == write_set[write_set_index + 1].owner_id_address_) {
      DVLOG(0) << *context << " Multiple write sets on record "
        << engine_->get_storage_manager()->get_name(write_entry->storage_id_)
        << ":" << write_entry->owner_id_address_
        << ". Will lock/unlock at the last one";
      write_set_index++;
      write_entry->mcs_block_ = 0;
      continue;
    }
    write_set_index++;

    // Try to find the matching read (fast forward as well)
    while (read_set_index < read_set_size) {
      read_entry = read_set + read_set_index;
      if (read_set_index < read_set_size - 1 &&
        read_entry->owner_id_address_ == read_set[read_set_index + 1].owner_id_address_) {
        // We might be half-way through clearing the mcs_block_ fields in xct_lock, so
        // double check and clear it here
        read_entry->mcs_block_ = 0;
      } else {
        if (read_entry->mcs_block_) {
          DVLOG(2) << *context << " Unlocking "
            << engine_->get_storage_manager()->get_name(read_entry->storage_id_) << ":"
            << read_entry->owner_id_address_;
          context->mcs_release_reader_lock(
            read_entry->owner_id_address_->get_key_lock(),
            read_entry->mcs_block_);
          read_entry->mcs_block_ = 0;
        }
        if (write_entry->owner_id_address_ <= read_entry->owner_id_address_) {
          read_set_index++;  // for next write-set entry
          break;
        }
      }
      read_set_index++;
    }

    if (write_entry->mcs_block_) {
      DVLOG(2) << *context << " Unlocking "
        << engine_->get_storage_manager()->get_name(write_entry->storage_id_) << ":"
        << write_entry->owner_id_address_;
      context->mcs_release_writer_lock(
        write_entry->owner_id_address_->get_key_lock(),
        write_entry->mcs_block_);
      write_entry->mcs_block_ = 0;
    }

    if (write_entry->owner_id_address_ == read_entry->owner_id_address_ && read_entry->mcs_block_) {
        read_entry->mcs_block_ = 0;
    } else if (read_entry->mcs_block_) {
      // have to take care of the read_entry, because we already advanced read_set_index, we
      // would miss it otherwise.
      ASSERT_ND(read_entry->owner_id_address_ > write_entry->owner_id_address_);
      context->mcs_release_reader_lock(
        read_entry->owner_id_address_->get_key_lock(),
        read_entry->mcs_block_);
      read_entry->mcs_block_ = 0;
    }
    ASSERT_ND(read_entry->mcs_block_ == 0);
    ASSERT_ND(write_entry->mcs_block_ == 0);
  }

  // More reads?
  while (read_set_index < read_set_size) {
    read_entry = read_set + read_set_index;
    if (read_set_index < read_set_size - 1 &&
      read_entry->owner_id_address_ == read_set[read_set_index + 1].owner_id_address_) {
      read_entry->mcs_block_ = 0;
    } else if (read_entry->mcs_block_) {
      context->mcs_release_reader_lock(
        read_entry->owner_id_address_->get_key_lock(),
        read_entry->mcs_block_);
      read_entry->mcs_block_ = 0;
    }
    ASSERT_ND(read_entry->mcs_block_ == 0);
    read_set_index++;
  }
  assorted::memory_fence_release();
  DVLOG(1) << *context << " unlocked write set";

#ifndef NDEBUG
  for (uint32_t i = 0; i < read_set_size; i++) {
    ASSERT_ND(read_set[i].mcs_block_ == 0);
  }
  for (uint32_t i = 0; i < write_set_size; i++) {
    ASSERT_ND(write_set[i].mcs_block_ == 0);
  }
#endif  // NDEBUG
}

ErrorCode XctManagerPimpl::abort_xct(thread::Thread* context) {
  Xct& current_xct = context->get_current_xct();
  if (!current_xct.is_active()) {
    return kErrorCodeXctNoXct;
  }
  DVLOG(1) << *context << " Aborted transaction in thread-" << context->get_thread_id();
  precommit_xct_unlock(context, false);
  current_xct.deactivate();
  context->get_thread_log_buffer().discard_current_xct_log();
  return kErrorCodeOk;
}

}  // namespace xct
}  // namespace foedus
