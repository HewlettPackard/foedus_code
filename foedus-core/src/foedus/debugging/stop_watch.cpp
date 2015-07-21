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
#include "foedus/debugging/stop_watch.hpp"

#include <chrono>

namespace foedus {
namespace debugging {

uint64_t get_now_nanosec() {
  std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

void StopWatch::start() {
  started_ = get_now_nanosec();
  stopped_ = started_;
}

uint64_t StopWatch::stop() {
  stopped_ = get_now_nanosec();
  return elapsed_ns();
}
uint64_t StopWatch::peek_elapsed_ns() const {
  return get_now_nanosec() - started_;
}

}  // namespace debugging
}  // namespace foedus
