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
#ifndef FOEDUS_SOC_SHARED_RENDEZVOUS_HPP_
#define FOEDUS_SOC_SHARED_RENDEZVOUS_HPP_

#include "foedus/cxx11.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/soc/shared_polling.hpp"

namespace foedus {
namespace soc {
/**
 * @brief A one-time single-producer multiple-consumer event synchronization in shared memory
 * for multiple processes.
 * @ingroup SOC
 * @details
 * Analogous to SharedMutex. This is a shared version of foedus::thread::Rendezvous.
 */
class SharedRendezvous CXX11_FINAL {
 public:
  SharedRendezvous() : signaled_(false), initialized_(false), cond_() { initialize(); }
  ~SharedRendezvous() { uninitialize(); }

  // Disable copy constructors
  SharedRendezvous(const SharedRendezvous&) CXX11_FUNC_DELETE;
  SharedRendezvous& operator=(const SharedRendezvous&) CXX11_FUNC_DELETE;

  void initialize();
  void uninitialize();
  bool is_initialized() const { return initialized_; }

  /** returns whether the even has signaled. */
  bool is_signaled() const {
    assorted::memory_fence_acquire();
    return signaled_;
  }
  /** weak version without fence. */
  bool is_signaled_weak() const { return signaled_; }


  /**
   * @brief Block until the event happens.
   */
  void wait();

  /**
   * @brief Block until the event happens \b or the given period elapses.
   * @return whether the event happened by now.
   */
  bool wait_for(uint64_t timeout_nanosec);

  /**
   * @brief Notify all waiters that the event has happened.
   * @details
   * There must be only one thread that might call this method, and it should call this only once.
   * Otherwise, the behavior is undefined.
   */
  void signal();

 private:
  /** whether the event has signaled. */
  volatile bool       signaled_;
  bool                initialized_;
  /** used to notify waiters to wakeup. */
  SharedPolling       cond_;
};

}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SHARED_RENDEZVOUS_HPP_
