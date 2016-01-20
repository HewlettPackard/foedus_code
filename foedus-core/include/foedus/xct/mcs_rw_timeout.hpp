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
#ifndef FOEDUS_XCT_MCS_RW_TIMEOUT_HPP_
#define FOEDUS_XCT_MCS_RW_TIMEOUT_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/assorted/raw_atomics.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread_id.hpp"

namespace foedus {
namespace xct {

typedef uint32_t McsBlockIndex;

struct McsRwLock {
  friend std::ostream& operator<<(std::ostream& o, const McsRwLock& v);
  static const thread::ThreadId kNextWriterNone = 0xFFFFU;

  uint32_t tail_;                 // +4 => 4
  /* FIXME(tzwang): ThreadId starts from 0, so we use 0xFFFF as the "invalid"
   * marker, unless we make the lock even larger than 8 bytes. This essentially
   * limits the largest allowed number of cores we support to 256 sockets x 256
   * cores per socket - 1.
   */
  thread::ThreadId next_writer_;  // +2 => 6
  uint16_t nreaders_;             // +2 => 8

  inline void reset() {
    tail_ = nreaders_ = 0;
    set_next_writer(kNextWriterNone);
    assorted::memory_fence_release();
  }
  inline void increment_nreaders() {
    assorted::raw_atomic_fetch_add<uint16_t>(&nreaders_, 1);
  }
  inline uint16_t decrement_nreaders() {
    return assorted::raw_atomic_fetch_add<uint16_t>(&nreaders_, -1);
  }
  inline uint16_t nreaders() {
    return assorted::atomic_load_acquire<uint16_t>(&nreaders_);
  }
  inline McsBlockIndex get_tail_waiter_block() const { return tail_ & 0xFFFFU; }
  inline thread::ThreadId get_tail_waiter() const { return tail_ >> 16U; }
  inline bool has_next_writer() const {
    return assorted::atomic_load_acquire<thread::ThreadId>(&next_writer_) != kNextWriterNone;
  }
  inline void set_next_writer(thread::ThreadId thread_id) {
    xchg_next_writer(thread_id);  // sub-word access...
  }
  inline thread::ThreadId get_next_writer() {
    return assorted::atomic_load_acquire<thread::ThreadId>(&next_writer_);
  }
  inline thread::ThreadId xchg_next_writer(thread::ThreadId id) {
    return assorted::raw_atomic_exchange<thread::ThreadId>(&next_writer_, id);
  }
  bool cas_next_writer_weak(thread::ThreadId expected, thread::ThreadId desired) {
    return assorted::raw_atomic_compare_exchange_weak<thread::ThreadId>(
      &next_writer_, &expected, desired);
  }
  inline uint32_t xchg_tail(uint32_t new_tail) {
    return assorted::raw_atomic_exchange<uint32_t>(&tail_, new_tail);
  }
  inline bool cas_tail_weak(uint32_t expected, uint32_t desired) {
    return assorted::raw_atomic_compare_exchange_weak<uint32_t>(&tail_, &expected, desired);
  }
  static inline uint32_t to_tail_int(
    thread::ThreadId tail_waiter,
    McsBlockIndex tail_waiter_block) {
    ASSERT_ND(tail_waiter_block <= 0xFFFFU);
    return static_cast<uint32_t>(tail_waiter) << 16 | (tail_waiter_block & 0xFFFFU);
  }
  inline bool is_locked() const {
    return tail_ != 0;
  }
};

struct McsRwBlock {
  /**
   * The state_ field in struct Components:
   * Bit: |-31-8-|-7-6-|-5--1-|--0--|
   * For: |unused|class|unused|state|
   *
   * Grant state is only meaningful when request state is 1.
   */
  static const uint32_t kStateMask            = 1U;
  static const uint32_t kStateWaiting         = 0U;
  static const uint32_t kStateGranted         = 1U;

  static const uint32_t kClassMask            = 3U << 6;
  static const uint32_t kClassReader          = 1U << 6;
  static const uint32_t kClassWriter          = 2U << 6;

  /* Possible values of the successor_class_ field */
  static const uint32_t kSuccessorClassMask   = 3U;
  static const uint32_t kSuccessorClassNone   = 0U;
  static const uint32_t kSuccessorClassReader = 1U;
  static const uint32_t kSuccessorClassWriter = 3U;

  /* States pred_int_ might carry */
  static const uint32_t kPredStateBusy         = 0xFFFFFFFF;
  static const uint32_t kPredStateWaitUpdate   = 0xFFFFFFFE;
  static const uint32_t kPredStateLeaving      = 0xFFFFFFFD;

  /* States succ_int_ might carry */
  static const uint32_t kSuccStateLeaving          = 0xFFFFFFFF;
  static const uint32_t kSuccStateSuccessorLeaving = 0xFFFFFFFE;
  static const uint32_t kSuccStateReleasing        = 0xFFFFFFFD;

  // data_, pred_int_, and succ_int_ are all word-sized,
  // no need to worry about partial-word access.
  union Self {
    uint64_t data_;                       // +8 => 8
    struct Components {
      uint32_t successor_class_;
      uint32_t state_;
    } components_;
  } self_;
  uint32_t pred_int_;                     // +4 => 12
  uint32_t succ_int_;                     // +4 => 16

  static inline void assert_pred_is_normal(uint32_t pred) {
    ASSERT_ND(pred != kPredStateBusy &&
      pred != kPredStateWaitUpdate &&
      pred != kPredStateLeaving);
  }
  static inline void assert_succ_is_normal(uint32_t succ) {
    ASSERT_ND(succ != kSuccStateLeaving &&
      succ != kSuccStateSuccessorLeaving &&
      succ != kSuccStateReleasing);
  }

  inline uint32_t xchg_pred_int(uint32_t pred) {
    return assorted::raw_atomic_exchange<uint32_t>(&pred_int_, pred);
  }
  inline uint32_t xchg_succ_int(uint32_t succ) {
    return assorted::raw_atomic_exchange<uint32_t>(&succ_int_, succ);
  }
  inline bool cas_succ_weak(uint32_t expected, uint32_t desired) {
    return assorted::raw_atomic_compare_exchange_weak<uint32_t>(&succ_int_, &expected, desired);
  }
  inline bool cas_pred_weak(uint32_t expected, uint32_t desired) {
    return assorted::raw_atomic_compare_exchange_weak<uint32_t>(&pred_int_, &expected, desired);
  }
  inline uint32_t get_succ_int() {
    return assorted::atomic_load_acquire<uint32_t>(&succ_int_);
  }

  inline uint32_t get_pred_int() {
    return assorted::atomic_load_acquire<uint32_t>(&pred_int_);
  }
  inline void set_pred_int(uint32_t pred) {
    assorted::atomic_store_release<uint32_t>(&pred_int_, pred);
  }
  inline bool cas_state_weak(uint64_t expected, uint64_t desired) {
    return assorted::raw_atomic_compare_exchange_weak<uint64_t>(&self_.data_, &expected, desired);
  }

  inline void init_reader() {
    self_.components_.state_ = kClassReader;
    init_common();
    ASSERT_ND(is_waiting());
    ASSERT_ND(is_reader());
  }
  inline void init_writer() {
    self_.components_.state_ = kClassWriter;
    init_common();
    ASSERT_ND(is_waiting());
    ASSERT_ND(is_writer());
  }
  inline void init_common() {
    self_.components_.state_ |= kStateWaiting;
    self_.components_.successor_class_ = kSuccessorClassNone;
    pred_int_ = succ_int_ = 0;
    assorted::memory_fence_release();
  }
  inline uint32_t read_state() {
    return assorted::atomic_load_acquire<uint32_t>(&self_.components_.state_);
  }
  inline bool is_waiting() {
    return (read_state() & kStateMask) == kStateWaiting;
  }
  inline bool is_granted() {
    return (read_state() & kStateMask) == kStateGranted;
  }
  inline bool is_reader() {
    return (read_state() & kClassMask) == kClassReader;
  }
  inline bool is_writer() {
    return (read_state() & kClassMask) == kClassWriter;
  }
  inline uint32_t read_successor_class() {
    return assorted::atomic_load_acquire<uint32_t>(&self_.components_.successor_class_);
  }
  inline void set_state_granted() {
    ASSERT_ND(is_waiting());
    assorted::raw_atomic_fetch_and_bitwise_or<uint32_t>(&self_.components_.state_, kStateGranted);
  }
  inline void set_succ_int(uint32_t succ) {
    assorted::atomic_store_release<uint32_t>(&succ_int_, succ);
  }
  inline bool successor_is_ready() {
    auto s = assorted::atomic_load_acquire<uint32_t>(&succ_int_);
    return s != 0 &&
      s != kSuccStateLeaving &&
      s != kSuccStateSuccessorLeaving &&
      s != kSuccStateReleasing;
  }
  inline bool state_has_reader_successor() {
    return read_successor_class() == kSuccessorClassReader;
  }
  inline bool state_has_writer_successor() {
    return read_successor_class() == kSuccessorClassWriter;
  }
  inline uint64_t make_waiting_with_reader_successor_state() {
    return (((uint64_t)(read_state() & ~kStateMask)) << 32) | (uint64_t)kSuccessorClassReader;
  }
  inline uint64_t make_waiting_with_no_successor_state() {
    return (((uint64_t)(read_state() & ~kStateMask)) << 32) | (uint64_t)kSuccessorClassNone;
  }
  inline bool timeout_granted(uint32_t timeout) {
    if (timeout == 0) {
      while (!is_granted()) {}
      ASSERT_ND(is_granted());
    } else {
      uint32_t cycles = 0;
      do {
        if (is_granted()) {
          return true;
        }
      } while (++cycles < timeout);
    }
    return is_granted();
  }
};

}  // namespace xct
}  // namespace foedus

#endif  // FOEDUS_XCT_MCS_RW_TIMEOUT_HPP_
