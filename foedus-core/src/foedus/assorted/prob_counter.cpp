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
#include "foedus/assorted/prob_counter.hpp"
/* So far we do much simpler thing.
namespace foedus {
namespace assorted {
uint64_t ProbCounter::deltas[ProbCounter::ndeltas];
double ProbCounter::A = 30;
uint64_t ProbCounter::max_rnd = 0;
assorted::UniformRandom ProbCounter::rnd;
bool ProbCounter::initialized = false;

void ProbCounter::initialize(double a, uint64_t seed) {
  if (initialized) {
    return;
  }
  A = a;
  rnd.set_current_seed(seed);

  double mydeltas[ProbCounter::ndeltas];

  // Calculate deltas in double type first, then we need to figure out a
  // multiplier so that only one delta is 0 (because our rnd only generates ints).
  //
  // XXX(tzwang): values in mydeltas decrease, so if you want to put a "barrier"
  // in the middle so that once the value is pass a certain threshold, it never
  // gets incremented more, you can tune the value of multiplier, such that after
  // times multiplier, the "barrier" delta value is 0. Note mydelta's type is
  // double and it's value is always between 0 and 1. So it's only a matter of
  // how many decimal points to leave and truncate (which happens when you put
  // the values in "deltas" and use with the rng).
  //
  // For our setting in HCC (8-bit counter, A=30 same as Morris' paper),
  // multiplier should be 10000. The minimum is 2, and max is 10000.
  // So we set the last element in deltas to 0, such that the value will
  // never pass 255 (max of uint8_t).
  for (uint16_t i = 0; i < ndeltas; ++i) {
    mydeltas[i] = std::pow(A / (A + 1), i);
  }

  // find the multiplier
  uint64_t multiplier = 1;
  for (uint16_t i = 0; i < ndeltas; ++i) {
    if ((uint64_t)(mydeltas[i] * multiplier)) {
      continue;
    } else {
      if (multiplier * 10 > multiplier) {
        multiplier *= 10;
      } else {  // multiplier overflow, dude, how many deltas do you have...
        LOG(FATAL) << "ProbCounter: multiplier overflow!";
      }
    }
  }
  for (uint16_t i = 0; i < ndeltas; ++i) {
    deltas[i] = mydeltas[i] * multiplier;
  }
  deltas[ndeltas - 1] = 0;
  max_rnd = multiplier;
  initialized = true;
}

}  // namespace assorted
}  // namespace foedus
*/
