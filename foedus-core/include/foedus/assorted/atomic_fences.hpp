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
#ifndef FOEDUS_ASSORTED_ATOMIC_FENCES_HPP_
#define FOEDUS_ASSORTED_ATOMIC_FENCES_HPP_

/**
 * @file foedus/assorted/atomic_fences.hpp
 * @ingroup ASSORTED
 * @brief Atomic fence methods that work for both C++11 and non-C++11 code.
 * @details
 * Especially on TSO architecture like x86, most memory fence is trivial thus supposedly very fast.
 * Invoking a non-inlined function for memory fence is thus not ideal.
 * The followings \e define memory fences for public headers that need them for inline methods.
 * We use gcc's builtin (__atomic_thread_fence) to avoid C++11 code. Kind of stupid, but
 * this also works on AArch64. We can add ifdef for clang later.
 */
namespace foedus {
namespace assorted {

/**
 * @brief Equivalent to std::atomic_thread_fence(std::memory_order_acquire).
 * @ingroup ASSORTED
 * @details
 * A load operation with this memory order performs the acquire operation on the affected memory
 * location: prior writes made to other memory locations by the thread that did the release become
 * visible in this thread.
 */
inline void memory_fence_acquire() {
  ::__atomic_thread_fence(__ATOMIC_ACQUIRE);
}

/**
 * @brief Equivalent to std::atomic_thread_fence(std::memory_order_release).
 * @ingroup ASSORTED
 * @details
 * A store operation with this memory order performs the release operation: prior writes to other
 * memory locations become visible to the threads that do a consume or an acquire on the same
 * location.
 */
inline void memory_fence_release() {
  ::__atomic_thread_fence(__ATOMIC_RELEASE);
}

/**
 * @brief Equivalent to std::atomic_thread_fence(std::memory_order_acq_rel).
 * @ingroup ASSORTED
 * @details
 * A load operation with this memory order performs the acquire operation on the affected memory
 * location and a store operation with this memory order performs the release operation.
 */
inline void memory_fence_acq_rel() {
  ::__atomic_thread_fence(__ATOMIC_ACQ_REL);
}

/**
 * @brief Equivalent to std::atomic_thread_fence(std::memory_order_consume).
 * @ingroup ASSORTED
 * @details
 * A load operation with this memory order performs a consume operation on the affected memory
 * location: prior writes to data-dependent memory locations made by the thread that did a release
 * operation become visible to this thread.
 */
inline void memory_fence_consume() {
  ::__atomic_thread_fence(__ATOMIC_CONSUME);
}

/**
 * @brief Equivalent to std::atomic_thread_fence(std::memory_order_seq_cst).
 * @ingroup ASSORTED
 * @details
 * Same as memory_order_acq_rel, plus a single total order exists in which all threads observe all
 * modifications in the same order.
 */
inline void memory_fence_seq_cst() {
  ::__atomic_thread_fence(__ATOMIC_SEQ_CST);
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_ATOMIC_FENCES_HPP_
