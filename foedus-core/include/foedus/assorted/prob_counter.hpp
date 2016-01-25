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
#ifndef FOEDUS_ASSORTED_PROB_COUNTER_HPP_
#define FOEDUS_ASSORTED_PROB_COUNTER_HPP_

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/uniform_random.hpp"

namespace foedus {
namespace assorted {
/**
 * @brief Implements a probabilistic counter [Morris 1978].
 * The major user is HCC's temperature field maintained in each page header.
 *
 * [Morris 1987] Robert Morris, Counting large numbers of events in small registers,
 * Commun. ACM 21, 10 (October 1978), 840-842.
 *
 * Unlike the paper, we do much simpler thing. We just compare with power of two,
 * so that we don't need exp/log. No information stored in static/global whatever.
 * @ingroup ASSORTED
 */
struct ProbCounter {
  /**
   * Log arithmic counter of aborts.
   * There were \b about 2^value_ aborts.
   * Keep it single member so sizeof(ProbCounter) == sizeof(uint8_t).
   */
  uint8_t value_;

  ProbCounter() : value_(0) {
    ASSERT_ND(sizeof(uint8_t) == sizeof(ProbCounter));
  }

  inline void reset() { value_ = 0; }

  inline void increment(UniformRandom* rnd) {
    // We increment the value_ for exponentially smaller probability.
    // value_==0: always
    // value_==1: 50% chance
    // value_==2: 25% chance... etc
    // We can implement this by simple bit-shifts.

    // Extract middle 24-bits, which is typically done in hashing. This is kinda hashing too.
    // This also means the following makes sense only for value_<=24. Should be a reasonable assmp.
    const uint32_t int_val = rnd->next_uint32();
    const uint32_t int24_val = (int_val >> 4) & 0xFFFFFFU;
    const uint32_t threshold = 1U << (24 - value_);
    if (int24_val < threshold) {
      ++value_;
    }
  }

  // XXX(tzwang): how should we actually implement decrement()? blind --? follow prob.?
  inline void decrement_force() {
    --value_;
  }
};
}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_PROB_COUNTER_HPP_
