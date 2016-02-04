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
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {

enum McsImplType {
  /**
    * A combination of our MCSg lock and the original RW-MCS lock.
    * This doesn't nicely support \e cancel, but functionality-wise it works.
    * Cancellable-requests will cause frequent atomic operations in a shared place.
    * Unconditional-requests and try-requests work just fine.
    */
  kMcsImplSimple = 0,
  /**
    * A combination of our MCSg lock, the original RW-MCS lock, and cancellable queue lock.
    * This nicely supports \e cancel, allowing local spinning even for cancallable-requests.
    * This also supports parallel async-lock nicely, but comes with complexity.
    */
  kMcsImplExtended,
};

/**
 * @brief Implements an MCS-locking Algorithm.
 * @ingroup XCT
 * @details
 * We implemented a few variants of MCS lock algorithm, so this switches the implementation
 * defined as McsImplType. Individual implementations are defined as individual functions
 * so that we can also test each of them explicitly.
 *
 * @par Lock Mode
 * foedus::xct::McsLock supports only exclusive lock (we call it \e ww below).
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
 * @par References
 * TBD: original MCS paper and RW version.
 * TBD: link to MCSg paper
 *
 * @tparam ADAPTOR A template that implements the McsAdaptorInterface concept.
 * We explicitly instantiate for all possible ADAPTOR types in cpp.
 * @see foedus::xct::McsAdaptorInterface
 */
template<typename ADAPTOR>
struct McsImpl {
  McsImpl(ADAPTOR adaptor, McsImplType type) : adaptor_(adaptor), type_(type) {}

  //////////////////////////////////////////////////////////////////////////////////
  /// WW-lock methods: BEGIN

  /** [WW] Unconditionally takes exclusive-only MCS lock on the given lock. */
  McsBlockIndex  acquire_unconditional_ww(McsLock* lock);
  // TBD we so far do not have try/asynchronous versions of WW lock. Do we need them?

  /** [WW] This doesn't use any atomic operation. only allowed when there is no race */
  McsBlockIndex  initial_ww(McsLock* lock);
  /** [WW] Unlcok an MCS lock acquired by this thread. */
  void           release_ww(McsLock* lock, McsBlockIndex block_index);

  /** [WW-Guest] Unconditionally takes exclusive-only \b guest lock on the given MCSg lock. */
  static void ownerless_acquire_unconditional_ww(McsLock* lock);
  static void ownerless_release_ww(McsLock* lock);
  static void ownerless_initial_ww(McsLock* lock);
  // No try/asynchronous versions for guests. Probably we don't need them.
  // TBD So far no RW versions for guests. We might need them later..

  /// WW-lock methods: END
  //////////////////////////////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////////////////////////////
  /// RW-lock methods: BEGIN

  /** [RW] Unconditionally takes a reader lock. */
  void acquire_unconditional_rw_reader(McsRwLock* lock, McsBlockIndex* out) {
    if (type_ == kMcsImplSimple) {
      acquire_unconditional_rw_reader_simple(lock, out);
    } else {
      acquire_unconditional_rw_reader_extended(lock, out);
    }
  }
  void acquire_unconditional_rw_reader_simple(McsRwLock* lock, McsBlockIndex* out);
  void finalize_acquire_reader_simple(McsRwLock* lock, McsRwBlock* my_block);
  void acquire_unconditional_rw_reader_extended(McsRwLock* lock, McsBlockIndex* out);

  /** [RW] Unconditionally takes a writer lock. */
  void acquire_unconditional_rw_writer(McsRwLock* lock, McsBlockIndex* out) {
    if (type_ == kMcsImplSimple) {
      acquire_unconditional_rw_writer_simple(lock, out);
    } else {
      acquire_unconditional_rw_writer_extended(lock, out);
    }
  }
  void acquire_unconditional_rw_writer_simple(McsRwLock* lock, McsBlockIndex* out);
  void acquire_unconditional_rw_writer_extended(McsRwLock* lock, McsBlockIndex* out);

  /**
   * [RW] Try to take a reader lock.
   * @return whether acquired the lock or not.
   */
  bool acquire_try_rw_reader(McsRwLock* lock, McsBlockIndex* out) {
    if (type_ == kMcsImplSimple) {
      return acquire_try_rw_reader_simple(lock, out);
    } else {
      return acquire_try_rw_reader_extended(lock, out);
    }
  }
  bool acquire_try_rw_reader_simple(McsRwLock* lock, McsBlockIndex* out);
  bool acquire_try_rw_reader_extended(McsRwLock* lock, McsBlockIndex* out);

  /**
   * [RW] Try to take a writer lock.
   * @return whether acquired the lock or not.
   */
  bool acquire_try_rw_writer(McsRwLock* lock, McsBlockIndex* out) {
    if (type_ == kMcsImplSimple) {
      return acquire_try_rw_writer_simple(lock, out);
    } else {
      return acquire_try_rw_writer_extended(lock, out);
    }
  }
  bool acquire_try_rw_writer_simple(McsRwLock* lock, McsBlockIndex* out);
  bool acquire_try_rw_writer_extended(McsRwLock* lock, McsBlockIndex* out);

  /**
   * [RW] Try to upgrade a reader lock to a writer lock.
   * @pre the lock must be now in reader mode.
   * @return whether upgraded the lock or not.
   */
  bool acquire_try_rw_writer_upgrade(McsRwLock* lock, McsBlockIndex* out) {
    if (type_ == kMcsImplSimple) {
      return acquire_try_rw_writer_upgrade_simple(lock, out);
    } else {
      return acquire_try_rw_writer_upgrade_extended(lock, out);
    }
  }
  bool acquire_try_rw_writer_upgrade_simple(McsRwLock* lock, McsBlockIndex* out);
  bool acquire_try_rw_writer_upgrade_extended(McsRwLock* lock, McsBlockIndex* out);

  /**
   * [RW] Releases a reader lock.
   * @pre the lock must be now in reader mode.
   */
  void release_rw_reader(McsRwLock* lock, McsBlockIndex block_index) {
    if (type_ == kMcsImplSimple) {
      release_rw_reader_simple(lock, block_index);
    } else {
      release_rw_reader_extended(lock, block_index);
    }
  }
  void release_rw_reader_simple(McsRwLock* lock, McsBlockIndex block_index);
  void release_rw_reader_extended(McsRwLock* lock, McsBlockIndex block_index);
  /**
   * [RW] Releases a writer lock.
   * @pre the lock must be now in writer mode.
   */
  void release_rw_writer(McsRwLock* lock, McsBlockIndex block_index) {
    if (type_ == kMcsImplSimple) {
      release_rw_writer_simple(lock, block_index);
    } else {
      release_rw_writer_extended(lock, block_index);
    }
  }
  void release_rw_writer_simple(McsRwLock* lock, McsBlockIndex block_index);
  void release_rw_writer_extended(McsRwLock* lock, McsBlockIndex block_index);

  McsRwBlock* dereference_rw_tail_block(uint32_t tail_int) {
    McsRwLock tail_tmp;
    tail_tmp.tail_ = tail_int;
    uint32_t tail_id = tail_tmp.get_tail_waiter();
    uint32_t tail_block = tail_tmp.get_tail_waiter_block();
    return adaptor_.get_rw_other_block(tail_id, tail_block);
  }


  /// RW-lock methods: END
  //////////////////////////////////////////////////////////////////////////////////

  ADAPTOR adaptor_;
  const McsImplType type_;
};

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_MCS_IMPL_HPP_
