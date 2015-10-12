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
  /** Invoked from a foreign counter to record an update */
  void on_update() ALWAYS_INLINE;

  /**
   * Efficiently checks if there was any update.
   * Forconcurrent access, we might miss an update for now (relaxed), but we will see
   * it eventually.
   */
  bool was_any_update() ALWAYS_INLINE;

  /** Invoke action when there was some update, and sets internal counter for next loop */
  template <typename ACTION>
  void invoke_if_updated(ACTION action) ALWAYS_INLINE;

  /** Batched version of invoke_if_updated(). */
  template <typename ACTION>
  void invoke_if_updated_batch(ACTION action, uint32_t to_index) ALWAYS_INLINE;

 private:
  /**
   * This counter is atomically incremented by foreign threads who updated some of the nodes
   * in this block.
   */
  std::atomic< uint32_t > updated_counter_;

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
   * @invariant checked_counter_ <= updated_counter_.
   */
  std::atomic< uint32_t > checked_counter_;
};

inline void VersionCounter::on_update() {
  updated_counter_.fetch_add(1U);
}

inline bool VersionCounter::was_any_update() {
  uint32_t updated = updated_counter_.load(std::memory_order_relaxed);
  uint32_t checked = checked_counter_.load(std::memory_order_relaxed);
  ASSERT_ND(updated >= checked);
  return updated != checked;
}

template <typename ACTION>
inline void VersionCounter::invoke_if_updated(ACTION action) {
  if (LIKELY(!was_any_update())) {
    return;
  }

  // at least the accesses are ordered, we are safe. No need to use acquire.
  uint32_t updated_safe = updated_counter_.load(std::memory_order_consume);
  // I'm the only one who loads/stores checked_.
  uint32_t checked_safe = checked_counter_.load(std::memory_order_relaxed);
  if (updated_safe > checked_safe) {
    checked_counter_.store(updated_safe, std::memory_order_relaxed);
    action();
  }
}

template <typename ACTION>
inline void VersionCounter::invoke_if_updated_batched(ACTION action, uint32_t to_index) {
  // hopefully compiler unrolls this loop.
  for (uint32_t i = 0; i < to_index; ++i) {
    (this + to_index)->invoke_if_updated(action);
  }
}

/**
 * First (highest) level contains 128 entries, thus 1 kbytes.
 * Each worker is usually spinning in this layer.
 * When someone has updated updated_counter_,
 * our cost to check is still dominated by re-retrieving the cacheline.
 * Scanning 1 kbytes of already-existing cachelines has negligible effect on latency.
 *
 * Some numbers in context:
 * 32 nodes per block. Block is the smallest granularity (L2).
 * Suppose 98M nodes data, 98 workers for SSSP. Each worker is responsible for
 * 1M nodes (=32k blocks). There will be 256 blocks per L1 entry, not bad.
 * Even if we scale up the data size, it will be within a reasonable range.
 * So, we simply use 2-levels where L2 size is dynamic (blocks-per-thread / kL1VersionFactors).
 */
const uint32_t kL1VersionFactors = 1 << 7;

}  // namespace sssp
}  // namespace foedus

#endif  // FOEDUS_EXPERIMENTS_SSSP_SCHEDULER_HPP_
