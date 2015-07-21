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
#ifndef FOEDUS_DEBUGGING_RDTSC_WATCH_HPP_
#define FOEDUS_DEBUGGING_RDTSC_WATCH_HPP_

#include <stdint.h>

#include "foedus/compiler.hpp"
#include "foedus/debugging/rdtsc.hpp"

namespace foedus {
namespace debugging {
/**
 * @brief A RDTSC-based low-overhead stop watch.
 * @ingroup DEBUGGING
 * @details
 * Unlike foedus::debugging::StopWatch, this watch is extremely low-overhead thus can be used
 * in performance sensitive places. Instead, it can only show cycles elapsed. You have to
 * do the math to convert it to sec/ms/us/ns yourself.
 * This doesn't take care of wrap around (VERY rare), either.
 */
class RdtscWatch {
 public:
  inline RdtscWatch() ALWAYS_INLINE : started_(0), stopped_(0) { start(); }

  /** Take current time tick. */
  inline void start() ALWAYS_INLINE {
    started_ = get_rdtsc();
  }

  /** Take another current time tick. Returns elapsed nanosec. */
  inline uint64_t stop() ALWAYS_INLINE {
    stopped_ = get_rdtsc();
    return elapsed();
  }

  inline uint64_t elapsed() const ALWAYS_INLINE {
    return stopped_ - started_;
  }

 private:
  uint64_t started_;
  uint64_t stopped_;
};

}  // namespace debugging
}  // namespace foedus

#endif  // FOEDUS_DEBUGGING_RDTSC_WATCH_HPP_
