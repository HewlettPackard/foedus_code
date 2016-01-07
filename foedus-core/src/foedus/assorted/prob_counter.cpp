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

namespace foedus {
namespace assorted {
uint8_t ProbCounter::deltas[ProbCounter::ndeltas];
double ProbCounter::A = 30;
assorted::UniformRandom ProbCounter::rnd;
bool ProbCounter::initialized = false;

void ProbCounter::initialize(double a, uint64_t seed) {
  A = a;
  ASSERT_ND(!initialized);
  rnd.set_current_seed(seed);
  for (uint16_t i = 0; i < ndeltas; ++i) {
    deltas[i] = std::pow(A / (A + 1), i) * 100;
  }
  initialized = true;
}

}  // namespace assorted
}  // namespace foedus
