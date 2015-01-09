/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_MANAGER_PIMPL_HPP_
#define FOEDUS_XCT_XCT_MANAGER_PIMPL_HPP_
#include <atomic>
#include <thread>

#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/soc/shared_cond.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/thread/condition_variable_impl.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/stoppable_thread_impl.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {
/** Shared data in XctManagerPimpl. */
struct XctManagerControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  XctManagerControlBlock() = delete;
  ~XctManagerControlBlock() = delete;

  void initialize() {
    current_global_epoch_advanced_.initialize();
    epoch_chime_wakeup_.initialize();
    new_transaction_paused_ = false;
  }
  void uninitialize() {
    epoch_chime_wakeup_.uninitialize();
    current_global_epoch_advanced_.uninitialize();
  }

  /**
   * @brief The current epoch of the entire engine.
   * @details
   * Currently running (committing) transactions will use this value as their serialization point.
   * No locks to protect this variable, but
   * \li There should be only one thread that might update this (XctManager).
   * \li Readers should take appropriate fence before reading this.
   * @invariant current_global_epoch_ > 0
   * (current_global_epoch_ begins with 1, not 0. So, epoch-0 is always an empty/dummy epoch)
   */
  std::atomic<Epoch::EpochInteger>  current_global_epoch_;
  /**
   * If some thread requested to immediately advance epoch, the requested epoch.
   * If this is less than or equal to current_global_epoch_, there is no immediate
   * advance request.
   * @invariant requested_global_epoch_.is_valid()
   */
  std::atomic<Epoch::EpochInteger>  requested_global_epoch_;

  /** Fired (broadcast) whenever current_global_epoch_ is advanced. */
  soc::SharedCond                   current_global_epoch_advanced_;

  /** Fired to wakeup epoch_chime_thread_ */
  soc::SharedCond                   epoch_chime_wakeup_;
  /** Protected by the mutex in epoch_chime_wakeup_ */
  std::atomic<bool>                 epoch_chime_terminate_requested_;

  /**
   * @brief If true, all new requests to begin_xct() will be paused until this becomes false.
   * @details
   * This is the mechanism for very rare events that need to separate out all concurrent transaction
   * executions, such as drop-volatile-page step after snapshotting.
   * This does not affect an already running transaction, so the snapshot thread must wait
   * for long enough after setting this value.
   * Also, the worker threads simply check-with-sleep to wait until this becomes false, not
   * SharedCond.
   * This is used only once per several minutes, so no need for optimization. Keep it simple!
   */
  std::atomic<bool>                 new_transaction_paused_;
};

/**
 * @brief Pimpl object of XctManager.
 * @ingroup XCT
 * @details
 * A private pimpl object for XctManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class XctManagerPimpl final : public DefaultInitializable {
 public:
  XctManagerPimpl() = delete;
  explicit XctManagerPimpl(Engine* engine) : engine_(engine) {}
  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  Epoch       get_current_global_epoch() const {
    return Epoch(control_block_->current_global_epoch_.load());
  }
  Epoch       get_requested_global_epoch() const {
    return Epoch(control_block_->requested_global_epoch_.load());
  }
  Epoch       get_current_global_epoch_weak() const {
    return Epoch(control_block_->current_global_epoch_.load(std::memory_order_relaxed));
  }

  ErrorCode   begin_xct(thread::Thread* context, IsolationLevel isolation_level);
  /**
   * This is the gut of commit protocol. It's mostly same as [TU2013].
   */
  ErrorCode   precommit_xct(thread::Thread* context, Epoch *commit_epoch);
  ErrorCode   abort_xct(thread::Thread* context);

  ErrorCode   wait_for_commit(Epoch commit_epoch, int64_t wait_microseconds);
  void        set_requested_global_epoch(Epoch request);
  void        advance_current_global_epoch();
  void        wait_for_current_global_epoch(Epoch target_epoch);
  void        wakeup_epoch_chime_thread();

  /**
   * @brief precommit_xct() if the transaction is read-only
   * @details
   * If the transaction is read-only, commit-epoch (serialization point) is the largest epoch
   * number in the read set. We don't have to take two memory fences in this case.
   */
  bool        precommit_xct_readonly(thread::Thread* context, Epoch *commit_epoch);
  /**
   * @brief precommit_xct() if the transaction is read-write
   * @details
   * See [TU2013] for the full protocol in this case.
   */
  bool        precommit_xct_readwrite(thread::Thread* context, Epoch *commit_epoch);

  /** used from precommit_xct_lock() to track moved record */
  bool        precommit_xct_lock_track_write(WriteXctAccess* entry);
  /** used from verification methods to track moved record */
  bool        precommit_xct_verify_track_read(ReadXctAccess* entry);
  /**
   * @brief Phase 1 of precommit_xct()
   * @param[in] context thread context
   * @param[out] max_xct_id largest xct_id this transaction depends on, or max(locked xct_id).
   * @return true if successful. false if we need to abort the transaction, in which case
   * locks are not obtained yet (so no need for unlock).
   * @details
   * Try to lock all records we are going to write.
   * After phase 2, we take memory fence.
   */
  bool        precommit_xct_lock(thread::Thread* context, XctId* max_xct_id);
  /**
   * @brief Phase 2 of precommit_xct() for read-only case
   * @return true if verification succeeded. false if we need to abort.
   * @details
   * Verify the observed read set and set the commit epoch to the highest epoch it observed.
   */
  bool        precommit_xct_verify_readonly(thread::Thread* context, Epoch *commit_epoch);
  /**
   * @brief Phase 2 of precommit_xct() for read-write case
   * @param[in] context thread context
   * @param[in,out] max_xct_id largest xct_id this transaction depends on, or max(all xct_id).
   * @return true if verification succeeded. false if we need to abort.
   * @details
   * Verify the observed read set and write set against the same record.
   * Because phase 2 is after the memory fence, no thread would take new locks while checking.
   */
  bool        precommit_xct_verify_readwrite(thread::Thread* context, XctId* max_xct_id);
  /** Returns false if there is any pointer set conflict */
  bool        precommit_xct_verify_pointer_set(thread::Thread* context);
  /** Returns false if there is any page version conflict */
  bool        precommit_xct_verify_page_version_set(thread::Thread* context);
  /**
   * @brief Phase 3 of precommit_xct()
   * @param[in] context thread context
   * @param[in] max_xct_id largest xct_id this transaction depends on, or max(all xct_id).
   * @param[in,out] commit_epoch commit epoch of this transaction. it's finalized in this function.
   * @details
   * Assuming phase 1 and 2 are successfully completed, apply all changes and unlock locks.
   */
  void        precommit_xct_apply(thread::Thread* context, XctId max_xct_id, Epoch *commit_epoch);
  /** unlocking all acquired locks, used when aborts. */
  void        precommit_xct_unlock(thread::Thread* context);

  /**
   * @brief Main routine for epoch_chime_thread_.
   * @details
   * This method keeps advancing global_epoch with the interval configured in XctOptions.
   * This method exits when this object's uninitialize() is called.
   */
  void        handle_epoch_chime();
  /** Makes sure all worker threads will commit with an epoch larger than grace_epoch. */
  void        handle_epoch_chime_wait_grace_period(Epoch grace_epoch);
  bool        is_stop_requested() const;

  /** Pause all begin_xct until you call resume_accepting_xct() */
  void        pause_accepting_xct();
  /** Make sure you call this after pause_accepting_xct(). */
  void        resume_accepting_xct();
  void        wait_until_resume_accepting_xct(thread::Thread* context);

  Engine* const                 engine_;
  XctManagerControlBlock*       control_block_;

  /**
   * This thread keeps advancing the current_global_epoch_.
   * Launched only in master engine.
   */
  std::thread epoch_chime_thread_;
};
static_assert(
  sizeof(XctManagerControlBlock) <= soc::GlobalMemoryAnchors::kXctManagerMemorySize,
  "XctManagerControlBlock is too large.");
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_MANAGER_PIMPL_HPP_
