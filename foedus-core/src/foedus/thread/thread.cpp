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
#include "foedus/thread/thread.hpp"

#include <ostream>

#include "foedus/engine.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/thread/thread_pimpl.hpp"

namespace foedus {
namespace thread {
Thread::Thread(
  Engine* engine,
  ThreadId id,
  ThreadGlobalOrdinal global_ordinal)
  : pimpl_(nullptr),
  stat_lock_request_failures_(0),
  stat_lock_request_successes_(0),
  stat_lock_acquire_failures_(0),
  stat_lock_acquire_successes_(0) {
  lock_rnd_.set_current_seed(global_ordinal);
  pimpl_ = new ThreadPimpl(engine, this, id, global_ordinal);
}
Thread::~Thread() {
  delete pimpl_;
  pimpl_ = nullptr;
  LOG(INFO)
    << "<lock_request_successes>" << stat_lock_request_successes_ << "</lock_request_successes>"
    << "<lock_request_failures>" << stat_lock_request_failures_ << "</lock_request_failures>"
    << "<lock_acquire_successes>" << stat_lock_acquire_successes_ << "</lock_acquire_successes>"
    << "<lock_acquire_failures>" << stat_lock_acquire_failures_ << "</lock_acquire_failures>";
}

ErrorStack Thread::initialize() {
  CHECK_ERROR(pimpl_->initialize());
  return kRetOk;
}
bool Thread::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack Thread::uninitialize() { return pimpl_->uninitialize(); }

Engine*     Thread::get_engine()        const { return pimpl_->engine_; }
ThreadId    Thread::get_thread_id()     const { return pimpl_->id_; }
ThreadGlobalOrdinal Thread::get_thread_global_ordinal() const { return pimpl_->global_ordinal_; }
Epoch* Thread::get_in_commit_epoch_address() { return &pimpl_->control_block_->in_commit_epoch_; }

memory::NumaCoreMemory* Thread::get_thread_memory() const { return pimpl_->core_memory_; }
memory::NumaNodeMemory* Thread::get_node_memory() const {
  return pimpl_->core_memory_->get_node_memory();
}

uint64_t Thread::get_snapshot_cache_hits() const {
  return pimpl_->control_block_->stat_snapshot_cache_hits_;
}

uint64_t Thread::get_snapshot_cache_misses() const {
  return pimpl_->control_block_->stat_snapshot_cache_misses_;
}

void Thread::reset_snapshot_cache_counts() const {
  pimpl_->control_block_->stat_snapshot_cache_hits_ = 0;
  pimpl_->control_block_->stat_snapshot_cache_misses_ = 0;
}

xct::Xct&   Thread::get_current_xct()   { return pimpl_->current_xct_; }
bool        Thread::is_running_xct()    const { return pimpl_->current_xct_.is_active(); }

xct::RwLockableXctId* Thread::get_canonical_address() {
  return pimpl_->canonical_address_;
}
void        Thread::set_canonical_address(xct::RwLockableXctId *ca) {
  pimpl_->canonical_address_ = ca;
}

log::ThreadLogBuffer& Thread::get_thread_log_buffer() { return pimpl_->log_buffer_; }

const memory::LocalPageResolver& Thread::get_local_volatile_page_resolver() const {
  return pimpl_->local_volatile_page_resolver_;
}

ErrorCode Thread::read_a_snapshot_page(
  storage::SnapshotPagePointer page_id,
  storage::Page* buffer) {
  return pimpl_->read_a_snapshot_page(page_id, buffer);
}
ErrorCode Thread::read_snapshot_pages(
  storage::SnapshotPagePointer page_id_begin,
  uint32_t page_count,
  storage::Page* buffer) {
  return pimpl_->read_snapshot_pages(page_id_begin, page_count, buffer);
}
ErrorCode Thread::find_or_read_a_snapshot_page(
  storage::SnapshotPagePointer page_id,
  storage::Page** out) {
  return pimpl_->find_or_read_a_snapshot_page(page_id, out);
}
ErrorCode Thread::find_or_read_snapshot_pages_batch(
  uint16_t batch_size,
  const storage::SnapshotPagePointer* page_ids,
  storage::Page** out) {
  return pimpl_->find_or_read_snapshot_pages_batch(batch_size, page_ids, out);
}

ErrorCode Thread::install_a_volatile_page(
  storage::DualPagePointer* pointer,
  storage::Page** installed_page) {
  return pimpl_->install_a_volatile_page(pointer, installed_page);
}

void Thread::collect_retired_volatile_page(storage::VolatilePagePointer ptr) {
  pimpl_->collect_retired_volatile_page(ptr);
}


std::ostream& operator<<(std::ostream& o, const Thread& v) {
  o << "Thread-" << v.get_thread_global_ordinal() << "(id=" << v.get_thread_id() << ") [";
  o << "status=" << (v.pimpl_->control_block_->status_);
  o << "]";
  return o;
}

const memory::GlobalVolatilePageResolver& Thread::get_global_volatile_page_resolver() const {
  return pimpl_->global_volatile_page_resolver_;
}

storage::Page* Thread::resolve(storage::VolatilePagePointer ptr) const {
  return get_global_volatile_page_resolver().resolve_offset(ptr);
}
storage::Page* Thread::resolve_newpage(storage::VolatilePagePointer ptr) const {
  return get_global_volatile_page_resolver().resolve_offset_newpage(ptr);
}
storage::Page* Thread::resolve(memory::PagePoolOffset offset) const {
  return get_local_volatile_page_resolver().resolve_offset(offset);
}
storage::Page* Thread::resolve_newpage(memory::PagePoolOffset offset) const {
  return get_local_volatile_page_resolver().resolve_offset_newpage(offset);
}


}  // namespace thread
}  // namespace foedus
