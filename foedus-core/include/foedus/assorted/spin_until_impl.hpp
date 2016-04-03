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
#ifndef FOEDUS_ASSORTED_SPIN_UNTIL_IMPL_HPP_
#define FOEDUS_ASSORTED_SPIN_UNTIL_IMPL_HPP_

#ifndef NDEBUG
#include <glog/logging.h>
#endif  // NDEBUG

#include <valgrind/valgrind.h>

#include "foedus/assorted/assorted_func.hpp"

#ifndef NDEBUG
#include "foedus/debugging/rdtsc_watch.hpp"
#endif  // NDEBUG

namespace foedus {
namespace assorted {

/**
 * @brief Spin locally until the given condition returns true.
 * @details
 * Even if you think your while-loop is trivial, make sure you use this.
 * The yield is necessary for valgrind runs.
 * This template has a negligible overhead in non-valgrind release compilation.
 *
 * This is a frequently appearing pattern without which valgrind runs
 * would go into an infinite loop. Unfortunately it needs lambda,
 * so this is an _impl file.
 *
 * @par Example
 * @code{.cpp}
 * spin_until([mcs_lock, address]{
 *  uint32_t old_int = McsLock::to_int(0, 0);
 *  return assorted::raw_atomic_compare_exchange_weak<uint32_t>(
 *    address,
 *    &old_int,
 *    kMcsGuestId);
 * });
 * @endcode
 *
 * In general:
 * @code{.cpp}
 * while(XXXX) {}
 *   is equal to
 * spin_until([]{ return !XXXX; });
 * @endcode
 * Notice the !. spin_"until", thus opposite to "while".
 */
template <typename COND>
inline void spin_until(COND spin_until_cond) {
#ifndef NDEBUG
  DVLOG(1) << "Locally spinning...";
  debugging::RdtscWatch watch;
#endif  // NDEBUG

  while (!spin_until_cond()) {
    // Valgrind never switches context without this.
    // This if should have a negligible overhead.
    if (RUNNING_ON_VALGRIND) {
      assorted::spinlock_yield();
    }
  }

#ifndef NDEBUG
  watch.stop();
  DVLOG(1) << "Spin ended. Spent " << (watch.elapsed() / 1000000.0) << "M cycles";
#endif  // NDEBUG
}

/**
 * @brief Use this in your while as a stop-gap before switching to spin_until().
 * @see spin_until()
 */
inline void yield_if_valgrind() {
  if (RUNNING_ON_VALGRIND) {
    assorted::spinlock_yield();
  }
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_SPIN_UNTIL_IMPL_HPP_
