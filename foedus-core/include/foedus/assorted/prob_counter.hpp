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
#endif

#include <cmath>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
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
 * To avoid re-calculating the deltas each time, before the system starts to
 * process transactions, somewhere the user should call initialize().
 *
 * @ingroup ASSORTED
 */
struct ProbCounter {
  static const uint16_t ndeltas = 1U << 8;
  static uint8_t deltas[ndeltas];
  static double A;  // parameter a - controls max count and expected error
  static assorted::UniformRandom rnd;
  static bool initialized;

  // Keep it single member so sizeof(ProbCounter) == sizeof(uint8_t)
  uint8_t value_;

  // Not protected, make sure only one guy calls this only once in the entire run
  static void initialize(double a, uint64_t seed = 673577);

  ProbCounter() : value_(0) {
    ASSERT_ND(sizeof(uint8_t) == sizeof(ProbCounter));
  }

  inline void reset() { value_ = 0; }

  inline void increment() {
    ASSERT_ND(initialized);
#ifndef NDEBUG
    auto v = value_;
    ASSERT_ND(deltas[v] == (uint8_t)(std::pow(A / (A + 1), v) * 100));
#endif
    auto prob = rnd.uniform_within(0, 100);
    if (prob < deltas[value_]) {
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
