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
#include "foedus/thread/thread_pool.hpp"

#include <ostream>

#include "foedus/assert_nd.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"

namespace foedus {
namespace thread {

ThreadPool::ThreadPool(Engine *engine) : pimpl_(nullptr) {
  pimpl_ = new ThreadPoolPimpl(engine);
}
ThreadPool::~ThreadPool() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack ThreadPool::initialize() { return pimpl_->initialize(); }
bool ThreadPool::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack ThreadPool::uninitialize() { return pimpl_->uninitialize(); }

bool ThreadPool::impersonate(
  const proc::ProcName& proc_name,
  const void* task_input,
  uint64_t task_input_size,
  ImpersonateSession *session) {
  return pimpl_->impersonate(proc_name, task_input, task_input_size, session);
}

bool ThreadPool::impersonate_on_numa_node(
  ThreadGroupId node,
  const proc::ProcName& proc_name,
  const void* task_input,
  uint64_t task_input_size,
  ImpersonateSession *session) {
  return pimpl_->impersonate_on_numa_node(node, proc_name, task_input, task_input_size, session);
}

bool ThreadPool::impersonate_on_numa_core(
  ThreadId core,
  const proc::ProcName& proc_name,
  const void* task_input,
  uint64_t task_input_size,
  ImpersonateSession *session) {
  return pimpl_->impersonate_on_numa_core(core, proc_name, task_input, task_input_size, session);
}

ThreadGroupRef* ThreadPool::get_group_ref(ThreadGroupId numa_node) {
  return pimpl_->get_group(numa_node);
}

ThreadRef* ThreadPool::get_thread_ref(ThreadId id) {
  return pimpl_->get_thread(id);
}


std::ostream& operator<<(std::ostream& o, const ThreadPool& v) {
  o << *v.pimpl_;
  return o;
}


}  // namespace thread
}  // namespace foedus
