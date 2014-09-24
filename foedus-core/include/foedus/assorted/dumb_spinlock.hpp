/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ASSORTED_DUMB_SPINLOCK_HPP_
#define FOEDUS_ASSORTED_DUMB_SPINLOCK_HPP_

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/raw_atomics.hpp"

namespace foedus {
namespace assorted {

/**
 * @brief A simple spinlock using a boolean field.
 * @ingroup ASSORTED
 * @details
 * As the name suggests, this spinlock implementation is not scalable at all.
 * It's the dumbest implementation of lock, which might cause cacheline invalidation storm when
 * contended. However, in many places it's enough and also the simplicity helps.
 * You just need memory for one bool, that's it. It trivially works for a shared memory, too.
 *
 * Use this object where you don't expect much contention.
 */
class DumbSpinlock {
 public:
  DumbSpinlock(bool* locked, bool lock_initially = true) : locked_by_me_(false), locked_(locked) {
    if (lock_initially) {
      lock();
    }
  }
  /** automatically unlocks when out of scope. */
  ~DumbSpinlock() { unlock(); }

  bool is_locked_by_me() const { return locked_by_me_; }

  /** Locks it if I haven't locked it yet. This method is idempotent. */
  void lock() {
    if (locked_by_me_) {
      return;  // already locked
    }

    SPINLOCK_WHILE(true) {
      bool expected = false;
      if (raw_atomic_compare_exchange_weak<bool>(locked_, &expected, true)) {
        break;
      }
    }

    ASSERT_ND(*locked_);
    locked_by_me_ = true;
  }

  /** Unlocks it if I locked it. This method is idempotent. You can safely call many times. */
  void unlock() {
    if (locked_by_me_) {
      ASSERT_ND(*locked_);
      locked_by_me_ = false;
      assorted::memory_fence_acq_rel();
      *locked_ = false;
      assorted::memory_fence_acq_rel();
    }
  }

 private:
  bool        locked_by_me_;
  bool* const locked_;
};

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_DUMB_SPINLOCK_HPP_
