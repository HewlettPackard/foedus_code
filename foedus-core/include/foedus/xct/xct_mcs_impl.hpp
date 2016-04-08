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
#ifndef FOEDUS_XCT_XCT_MCS_IMPL_HPP_
#define FOEDUS_XCT_XCT_MCS_IMPL_HPP_

#include "foedus/compiler.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {

/**
 * @brief Implements an MCS-locking Algorithm.
 * @ingroup XCT
 * @details
 * @par Implementation Types
 * We implemented a few variants of MCS lock algorithm, so this switches the implementation
 * defined below. Individual implementations are defined as individual functions
 * so that we can also test each of them explicitly.
 *
 * @par Implementation Type 1: "Simple" (RW_BLOCK = McsRwSimpleBlock)
 * A combination of our MCSg lock and the original RW-MCS lock.
 * This doesn't nicely support \e cancel, but functionality-wise it works.
 * Cancellable-requests will cause frequent atomic operations in a shared place.
 * Unconditional-requests and try-requests work just fine \b EXCEPT try-reader,
 * which turns out to require doubly-linked list.
 * For this, we have a meta-method to return whether the impl supports try-reader or not.
 *
 * @par Implementation Type 2: "Extended" (RW_BLOCK = McsRwExtendedBlock)
 * A combination of our MCSg lock, the original RW-MCS lock, and cancellable queue lock.
 * This nicely supports \e cancel, allowing local spinning even for cancallable-requests.
 * This also supports parallel async-lock nicely, but comes with complexity and more
 * atomic instructions.
 *
 * @par Lock Mode
 * foedus::xct::McsWwLock supports only exclusive lock (we call it \e ww below).
 * foedus::xct::McsRwLock supports reader and writer (we call it \e rw below).
 * All implementations can handle both types so far.
 *
 * @par Lock Request Types
 * There are 3 lock request types:
 * \li \b Unconditional-lock. Simplest and fastest. When we have no risk of deadlock, this suffices.
 * It always succeds although it might have to wait for long time.
 * \li \b Try-lock. Also simple, but maybe costly \e per-try. It issues one atomic op to a
 * central place to acquire or fail.
 * \li \b Asynchronous-cancellable-lock. More complex. It allows \e come-back-later to
 * check whether it acquired lock or not, and to \e cancel the lock.
 * It must also allow doing this for multiple locks in parallel.
 *
 * @par Writer-Upgrade
 * You might notice that we don't have a so-called \e upgrade method here to convert
 * a read-lock to a write-lock. We don't need it in our architecture.
 * We always release the read-lock first and take a write-lock using a new queue node.
 * In traditional 2PL, this might violate serializability, but we are not using 2PL.
 * Serializability on the record is always guaranteed by the read-verification.
 * Also, writer-upgrade always has a risk of deadlock, even in a single-lock transaction.
 * By getting rid of it, we make the RLL protocol simpler and more flexible.
 *
 * @par References
 * TBD: original MCS paper and RW version.
 * TBD: link to MCSg paper
 *
 * @tparam ADAPTOR A template that implements the McsAdaptorConcept template concept.
 * We explicitly instantiate for all possible ADAPTOR types in cpp.
 * @tparam RW_BLOCK Queue node object for RW-lock.
 * Either \b McsRwSimpleBlock or \b McsRwExtendedBlock.
 * This also defines the implementation.
 * @see foedus::xct::McsAdaptorConcept
 */
template<typename ADAPTOR, typename RW_BLOCK>
class McsImpl {
 public:
  explicit McsImpl(ADAPTOR adaptor) : adaptor_(adaptor) {}
  //////////////////////////////////////////////////////////////////////////////////
  /// RW-lock methods: BEGIN

  //////////////////////////
  /// Unconditional-acquire
  /** [RW] Unconditionally takes a reader lock. */
  McsBlockIndex acquire_unconditional_rw_reader(McsRwLock* lock);

  /** [RW] Unconditionally takes a writer lock. */
  McsBlockIndex acquire_unconditional_rw_writer(McsRwLock* lock);

  //////////////////////////
  /// Try-acquire
  /**
   * [RW] Try to take a reader lock.
   * @pre the lock must \b NOT be taken by this thread yet.
   * @return 0 if failed, the block index if acquired.
   * @see  does_support_try_rw_reader()
   */
  McsBlockIndex acquire_try_rw_reader(McsRwLock* lock);
  /**
   * @return whether this impl supports acquire_try_rw_reader()/retry_async_rw_reader().
   * @see acquire_try_rw_reader()
   */
  static bool does_support_try_rw_reader();

  /**
   * [RW] Try to take a writer lock.
   * @pre the lock must \b NOT be taken by this thread yet (even in reader mode).
   * @return 0 if failed, the block index if acquired.
   */
  McsBlockIndex acquire_try_rw_writer(McsRwLock* lock);

  //////////////////////////
  /// \b Async-acquire trios (\b acquire, \b cancel, \b retry)

  /**
   * [RW] Asynchronously try to take a reader lock.
   * @pre the lock must \b NOT be taken by this thread yet.
   */
  AcquireAsyncRet acquire_async_rw_reader(McsRwLock* lock);
  /**
   * [RW] Asynchronously try to take a writer lock.
   * @pre the lock must \b NOT be taken by this thread yet (even in reader mode).
   */
  AcquireAsyncRet acquire_async_rw_writer(McsRwLock* lock);

  /**
   * [RW] Returns whether the lock requeust is now granted.
   * @pre block_index != 0
   * @note this is an instantenous check. Timeout is handled by the caller.
   * @see  does_support_try_rw_reader()
   */
  bool retry_async_rw_reader(McsRwLock* lock, McsBlockIndex block_index);
  bool retry_async_rw_writer(McsRwLock* lock, McsBlockIndex block_index);

  /**
   * [RW] Cancels the lock request.
   * @pre block_index != 0
   * @note this works no matter whether the request is no granted or not.
   */
  void cancel_async_rw_reader(McsRwLock* lock, McsBlockIndex block_index);
  void cancel_async_rw_writer(McsRwLock* lock, McsBlockIndex block_index);

  //////////////////////////
  /// Release and other stuffs
  /**
   * [RW] Releases a reader lock.
   * @pre the lock must be now in reader mode.
   */
  void release_rw_reader(McsRwLock* lock, McsBlockIndex block_index);
  /**
   * [RW] Releases a writer lock.
   * @pre the lock must be now in writer mode.
   */
  void release_rw_writer(McsRwLock* lock, McsBlockIndex block_index);
  /// RW-lock methods: END
  //////////////////////////////////////////////////////////////////////////////////

 private:
  ADAPTOR adaptor_;
};

/**
 * @brief A specialized/simplified implementation of an MCS-locking Algorithm
 * for exclusive-only (WW) locks.
 * @ingroup XCT
 * @details
 * This is exactly same as MCSg. Most places in our codebase now use RW locks,
 * but still there are a few WW-only places, such as page-lock (well, so far).
 */
template<typename ADAPTOR>
class McsWwImpl {
 public:
  explicit McsWwImpl(ADAPTOR adaptor) : adaptor_(adaptor) {}

  /** [WW] Unconditionally takes exclusive-only MCS lock on the given lock. */
  McsBlockIndex  acquire_unconditional(McsWwLock* lock);

  /**
   * [WW] Try to take an exclusive lock.
   * @pre the lock must \b NOT be taken by this thread yet.
   * @return 0 if failed, the block index if acquired.
   */
  McsBlockIndex  acquire_try(McsWwLock* lock);
  // We so far do not have asynchronous version of WW lock. probably we don't need it..

  /**
   * [WW] This doesn't use any atomic operation. only allowed when there is no race
   * TASK(Hideaki): This will be renamed to mcs_non_racy_lock(). "initial_lock" is ambiguous.
   */
  McsBlockIndex  initial(McsWwLock* lock);
  /** [WW] Unlcok an MCS lock acquired by this thread. */
  void           release(McsWwLock* lock, McsBlockIndex block_index);

  /** [WW-Guest] Unconditionally takes exclusive-only \b guest lock on the given MCSg lock. */
  static void     ownerless_acquire_unconditional(McsWwLock* lock);
  /**
   * [WW-Guest] Try to take an exclusive \b guest lock on the given MCSg lock.
   * @returns whether we got the lock. If you receive true, you are responsible to call
   * ownerless_release()
   */
  static bool     ownerless_acquire_try(McsWwLock* lock);
  static void     ownerless_release(McsWwLock* lock);
  static void     ownerless_initial(McsWwLock* lock);
  // No try/asynchronous versions for guests. Probably we don't need them.
  // TBD So far no RW versions for guests. We might need them later..

 private:
  ADAPTOR adaptor_;
};

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_MCS_IMPL_HPP_
