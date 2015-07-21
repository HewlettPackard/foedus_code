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
#include "foedus/soc/shared_rendezvous.hpp"

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"

namespace foedus {
namespace soc {

void SharedRendezvous::initialize() {
  uninitialize();
  signaled_ = false;
  initialized_ = true;
  cond_.initialize();
}

void SharedRendezvous::uninitialize() {
  if (!is_initialized()) {
    return;
  }
}

void SharedRendezvous::wait() {
  if (is_signaled_weak()) {  // just an optimization.
    return;
  }

  uint64_t demand = cond_.acquire_ticket();
  if (is_signaled()) {
    return;
  }
  cond_.wait(demand);
  ASSERT_ND(is_signaled());
}

bool SharedRendezvous::wait_for(uint64_t timeout_nanosec) {
  if (is_signaled_weak()) {  // just an optimization.
    return true;
  }

  uint64_t demand = cond_.acquire_ticket();
  if (is_signaled()) {
    return true;
  }
  bool received = cond_.timedwait(demand, timeout_nanosec / 1000);
  ASSERT_ND(!received || is_signaled());
  return is_signaled();
}

void SharedRendezvous::signal() {
  signaled_ = true;
  cond_.signal();
}

}  // namespace soc
}  // namespace foedus
