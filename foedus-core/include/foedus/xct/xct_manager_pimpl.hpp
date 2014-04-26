/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_MANAGER_PIMPL_HPP_
#define FOEDUS_XCT_XCT_MANAGER_PIMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/xct/fwd.hpp>
#include <foedus/xct/epoch.hpp>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
namespace foedus {
namespace xct {
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
    explicit XctManagerPimpl(Engine* engine);
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    Epoch       get_global_epoch(bool fence_before_get) const {
        if (fence_before_get) {
            std::atomic_thread_fence(std::memory_order_acquire);
        }
        return global_epoch_;
    }
    ErrorStack  begin_xct(thread::Thread* context);
    /**
     * This is the gut of commit protocol. It's mostly same as [TU2013].
     */
    ErrorStack  prepare_commit_xct(thread::Thread* context, Epoch *commit_epoch);
    ErrorStack  abort_xct(thread::Thread* context);

    /**
     * @brief Phase 1 of prepare_commit_xct()
     * @details
     * Try to lock all records we are going to write.
     * After phase 2, we take memory fence.
     */
    void prepare_commit_xct_lock_phase(thread::Thread* context);
    /**
     * @brief Phase 2 of prepare_commit_xct()
     * @return true if verification succeeded. false if we need to abort.
     * @details
     * Verify the observed read set and write set against the same record.
     * Because phase 2 is after the memory fence, no thread would take new locks while checking.
     */
    bool prepare_commit_xct_verify_phase(thread::Thread* context);
    /**
     * @brief Phase 3 of prepare_commit_xct()
     * @details
     * Assuming phase 1 and 2 are successfully completed, apply all changes and unlock locks.
     */
    void prepare_commit_xct_apply_phase(thread::Thread* context, const Epoch &commit_epoch);
    /** unlocking all acquired locks, used when aborts. */
    void prepare_commit_xct_unlock(thread::Thread* context);

    /**
     * @brief Main routine for epoch_advance_thread_.
     * @details
     * This method keeps advancing global_epoch with the interval configured in XctOptions.
     * This method exits when this object's uninitialize() is called.
     */
    void        handle_epoch_advance();

    Engine* const           engine_;

    /**
     * The current epoch of the entire engine.
     * No locks to protect this variable, but
     * \li There should be only one thread that might update this (XctManager).
     * \li Readers should take appropriate fence before reading this (XctManager#begin_xct()).
     */
    Epoch                   global_epoch_;

    std::mutex              epoch_advance_mutex_;
    std::condition_variable epoch_advance_stop_condition_;
    std::thread             epoch_advance_thread_;
    bool                    epoch_advance_stop_requested_;
    bool                    epoch_advance_stopped_;
};
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_MANAGER_PIMPL_HPP_
