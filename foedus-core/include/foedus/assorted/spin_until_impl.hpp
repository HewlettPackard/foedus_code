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

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/rdtsc_watch.hpp"

namespace foedus {
namespace assorted {

/**
 * @return whether this process is running on valgrind.
 * Equivalent to RUNNING_ON_VALGRIND macro (but you don't have to include valgrind.h just for it.)
 */
bool is_running_on_valgrind();

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
 * spin_until([block_address]{
 *  return (*block_address) != 0;  // Spin until the block becomes non-zero
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
 * @return the number of cycles (using RDTSC) spent in this function.
 */
template <typename COND>
inline uint64_t spin_until(COND spin_until_cond) {
  debugging::RdtscWatch watch;

  const bool on_valgrind = is_running_on_valgrind();
  while (!spin_until_cond()) {
    // Valgrind never switches context without this.
    // This if should have a negligible overhead.
    if (on_valgrind) {
      assorted::spinlock_yield();
    }
  }

  watch.stop();
  return watch.elapsed();
}

/**
 * @brief Use this in your while as a stop-gap before switching to spin_until().
 * @see spin_until()
 */
inline void yield_if_valgrind() {
  const bool on_valgrind = is_running_on_valgrind();
  if (on_valgrind) {
    assorted::spinlock_yield();
  }
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_SPIN_UNTIL_IMPL_HPP_
