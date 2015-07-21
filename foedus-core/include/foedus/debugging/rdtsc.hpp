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
#ifndef FOEDUS_DEBUGGING_RDTSC_HPP_
#define FOEDUS_DEBUGGING_RDTSC_HPP_
/**
 * @file foedus/debugging/rdtsc.hpp
 * @brief Implements an RDTSC (Real-time time stamp counter) wait to emulate latency on slower
 * devices.
 * @ingroup DEBUGGING
 */

#include <stdint.h>

namespace foedus {
namespace debugging {
/**
 * @brief Returns the current CPU cycle via x86 RDTSC.
 * @ingroup DEBUGGING
 */
inline uint64_t get_rdtsc() {
#ifndef __aarch64__
  // x86.
  uint32_t low, high;
  asm volatile("rdtsc" : "=a" (low), "=d" (high));
  return (static_cast<uint64_t>(high) << 32) | low;
#else  // __aarch64__
  // AArch64. "cntvct_el0" gives read-only physical 64bit timer.
  // http://infocenter.arm.com/help/index.jsp?topic=/com.arm.doc.ddi0488d/ch09s03s01.html
  uint64_t ret;
  asm volatile("isb; mrs %0, cntvct_el0" : "=r" (ret));
  return ret;
#endif  // __aarch64__
}

/**
 * @brief Wait until the given CPU cycles elapse.
 * @param[in] cycles CPU cycles to wait for
 * @ingroup DEBUGGING
 * @details
 * In case of context switch to a different CPU that has a very different timing (esp on NUMA),
 * we also check if the RDTSC value is not bogus. In that case, we exit the wait.
 * This is also a safety net for wrap-around.
 * Anyways, it's a rare case.
 */
inline void wait_rdtsc_cycles(uint64_t cycles) {
  uint64_t cycle_error = get_rdtsc() - cycles;
  uint64_t cycle_until = get_rdtsc() + cycles;
  while (true) {
    uint64_t current = get_rdtsc();
    if (current >= cycle_until || current <= cycle_error) {
      break;
    }
  }
}

}  // namespace debugging
}  // namespace foedus
#endif  // FOEDUS_DEBUGGING_RDTSC_HPP_
