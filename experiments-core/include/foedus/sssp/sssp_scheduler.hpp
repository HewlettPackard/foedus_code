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
#ifndef FOEDUS_EXPERIMENTS_SSSP_SCHEDULER_HPP_
#define FOEDUS_EXPERIMENTS_SSSP_SCHEDULER_HPP_

#include <stdint.h>

#include <atomic>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"

namespace foedus {
namespace sssp {

/**
 * @brief A scalable mechanism to detect remaining tasks by counting updates.
 * @details
 * We don't maintain a full-fledged task-queue because it causes one or more of
 * the following:
 * \li scalability bottleneck to synchronize task pop/push
 * \li memory management and deadlock
 * ("hey his queue is full, let's wait. btw my queue is full too.")
 * \li complexity to handle all of these headaches
 *
 * Rather, we use this simple counter and still keep the latency to detect a new
 * task quite low.
 */
class VersionCounter {
 public:
  typedef uint16_t CounterInt;
  /** Invoked from a foreign counter to record an update */
  void on_update() ALWAYS_INLINE;

  /**
   * Checks if there was any update.
   */
  bool has_update(std::memory_order barrier) const ALWAYS_INLINE;

  CounterInt get_updated_counter(std::memory_order barrier) ALWAYS_INLINE;
  void set_checked_counter(CounterInt new_value) ALWAYS_INLINE;

 private:
  /**
   * This counter is atomically incremented by foreign threads who updated some of the nodes
   * in this block.
   */
  std::atomic< CounterInt > updated_counter_;

  /**
   * This counter is set (no need to be atomic) by the owner thread
   * \b after_ observing updated_counter_ that is larger than the current
   * value of checked_counter_ and \b before it actually checks \e descendants;
   * sub-blocks or individual nodes.
   *
   * Two important invariants:
   * \li When checked_counter_ is not equal to updated_counter_,
   * there \b might be an updated node.
   * \li When checked_counter_ is equal to updated_counter_,
   * it is guaranteed that there is no updated node.
   *
   * This variable is only read/written by the owner thread. Thus not std::atomic
   * @invariant checked_counter_ <= updated_counter_.
   */
  CounterInt  checked_counter_;
};

inline void VersionCounter::on_update() {
  updated_counter_.fetch_add(1U);
}

inline VersionCounter::CounterInt VersionCounter::get_updated_counter(std::memory_order barrier) {
  return updated_counter_.load(barrier);
}
inline void VersionCounter::set_checked_counter(VersionCounter::CounterInt new_value) {
  ASSERT_ND(new_value <= updated_counter_.load());
  checked_counter_ = new_value;
}

inline bool VersionCounter::has_update(std::memory_order barrier) const {
  CounterInt updated = updated_counter_.load(barrier);
  ASSERT_ND(updated >= checked_counter_);
  return updated != checked_counter_;
}

/**
 * First (highest) level contains 128 entries, thus 0.5 KB.
 * Each worker is usually spinning in this layer.
 * When someone has updated updated_counter_,
 * our cost to check is still dominated by re-retrieving the cacheline.
 * Scanning 0.5 KB of already-existing cachelines has negligible effect on latency.
 *
 * Some numbers in context:
 * 32 nodes per block. Block is the smallest granularity (L2).
 * Suppose 96M nodes data, 96 workers for SSSP. Each worker is responsible for
 * 1M nodes (=32k blocks). There will be 256 blocks per L1 entry, not bad.
 * Even if we scale up the data size, it will be within a reasonable range.
 * So, we simply use 2-levels where L2 size is dynamic (blocks-per-thread / kL1VersionFactors).
 */
const uint32_t kL1VersionFactors = 1 << 7;

}  // namespace sssp
}  // namespace foedus

#endif  // FOEDUS_EXPERIMENTS_SSSP_SCHEDULER_HPP_
