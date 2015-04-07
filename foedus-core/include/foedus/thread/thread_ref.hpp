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
#ifndef FOEDUS_THREAD_THREAD_REF_HPP_
#define FOEDUS_THREAD_THREAD_REF_HPP_

#include <iosfwd>
#include <vector>

#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/thread/thread_id.hpp"
#include "foedus/xct/fwd.hpp"

namespace foedus {
namespace thread {
/**
 * @brief A view of Thread object for other SOCs and master engine.
 * @ingroup THREAD
 */
class ThreadRef CXX11_FINAL {
 public:
  ThreadRef();
  ThreadRef(Engine* engine, ThreadId id);

  /**
   * Conditionally try to occupy this thread, or impersonate. If it fails, it immediately returns.
   * @param[in] proc_name the name of the procedure to run on this thread.
   * @param[in] task_input input data of arbitrary format for the procedure.
   * @param[in] task_input_size byte size of the input data to copy into the thread's memory.
   * @param[out] session the session to run on this thread. On success, the session receives a
   * ticket so that the caller can wait for the completion.
   * @return whether successfully impersonated.
   */
  bool          try_impersonate(
    const proc::ProcName& proc_name,
    const void* task_input,
    uint64_t task_input_size,
    ImpersonateSession *session);

  Engine*       get_engine() const { return engine_; }
  ThreadId      get_thread_id() const { return id_; }
  ThreadGroupId get_numa_node() const { return decompose_numa_node(id_); }
  void*         get_task_input_memory() const { return task_input_memory_; }
  void*         get_task_output_memory() const { return task_output_memory_; }
  xct::McsBlock* get_mcs_blocks() const { return mcs_blocks_; }
  ThreadControlBlock* get_control_block() const { return control_block_; }

  /** @see foedus::xct::InCommitEpochGuard  */
  Epoch         get_in_commit_epoch() const;

  uint64_t      get_snapshot_cache_hits() const;
  uint64_t      get_snapshot_cache_misses() const;
  void          reset_snapshot_cache_counts() const;

  friend std::ostream& operator<<(std::ostream& o, const ThreadRef& v);

 private:
  Engine*               engine_;

  /** Unique ID of this thread. */
  ThreadId              id_;

  ThreadControlBlock*   control_block_;
  void*                 task_input_memory_;
  void*                 task_output_memory_;

  /** Pre-allocated MCS blocks. index 0 is not used so that successor_block=0 means null. */
  xct::McsBlock*        mcs_blocks_;
};


/**
 * @brief A view of Thread group object for other SOCs and master engine.
 * @ingroup THREAD
 */
class ThreadGroupRef CXX11_FINAL {
 public:
  ThreadGroupRef();
  ThreadGroupRef(Engine* engine, ThreadGroupId group_id);

  ThreadGroupId           get_group_id() const { return group_id_; }

  /** Returns Thread object for the given ordinal in this group. */
  ThreadRef*              get_thread(ThreadLocalOrdinal ordinal) { return &threads_[ordinal]; }

  /**
   * Returns the oldest in-commit epoch of the threads in this group.
   * Empty in-commit epoch is skipped. If all of them are empty, returns an invalid epoch
   * (meaning all of them will get the latest current epoch and are safe).
   * @see foedus::xct::InCommitEpochGuard
   */
  Epoch                   get_min_in_commit_epoch() const;

  friend std::ostream& operator<<(std::ostream& o, const ThreadGroupRef& v);

 private:
  Engine*                 engine_;
  ThreadGroupId           group_id_;
  std::vector<ThreadRef>  threads_;
};


}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_REF_HPP_
