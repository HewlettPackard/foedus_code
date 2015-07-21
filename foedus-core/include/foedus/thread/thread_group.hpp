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
#ifndef FOEDUS_THREAD_THREAD_GROUP_HPP_
#define FOEDUS_THREAD_THREAD_GROUP_HPP_

#include <iosfwd>
#include <vector>

#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace thread {
/**
 * @brief Represents a group of pre-allocated threads running in one NUMA node.
 * @ingroup THREAD
 * @details
 * Detailed description of this class.
 */
class ThreadGroup CXX11_FINAL : public DefaultInitializable {
 public:
  ThreadGroup() CXX11_FUNC_DELETE;
  ThreadGroup(Engine* engine, ThreadGroupId group_id);
  ~ThreadGroup();
  ErrorStack  initialize_once() CXX11_OVERRIDE;
  ErrorStack  uninitialize_once() CXX11_OVERRIDE;

  ThreadGroupId           get_group_id() const { return group_id_; }
  memory::NumaNodeMemory* get_node_memory() const { return node_memory_; }

  /** Returns Thread object for the given ordinal in this group. */
  Thread*                 get_thread(ThreadLocalOrdinal ordinal) const { return threads_[ordinal]; }

  friend  std::ostream& operator<<(std::ostream& o, const ThreadGroup& v);

 private:
  Engine* const           engine_;

  /** ID of this thread group. */
  ThreadGroupId           group_id_;

  /**
   * Memory repository shared among threads in this group.
   * ThreadGroup does NOT own it, meaning it doesn't call its initialize()/uninitialize().
   * EngineMemory owns it in terms of that.
   */
  memory::NumaNodeMemory* node_memory_;

  /**
   * List of Thread in this group. Index is ThreadLocalOrdinal.
   */
  std::vector<Thread*>    threads_;
};

}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_GROUP_HPP_
