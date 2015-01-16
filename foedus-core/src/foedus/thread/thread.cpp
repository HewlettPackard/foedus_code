/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
  : pimpl_(nullptr) {
  pimpl_ = new ThreadPimpl(engine, this, id, global_ordinal);
}
Thread::~Thread() {
  delete pimpl_;
  pimpl_ = nullptr;
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

log::ThreadLogBuffer& Thread::get_thread_log_buffer() { return pimpl_->log_buffer_; }

const memory::LocalPageResolver& Thread::get_local_volatile_page_resolver() const {
  return pimpl_->local_volatile_page_resolver_;
}

ErrorCode Thread::read_a_snapshot_page(
  storage::SnapshotPagePointer page_id,
  storage::Page* buffer) {
  return pimpl_->read_a_snapshot_page(page_id, buffer);
}
ErrorCode Thread::find_or_read_a_snapshot_page(
  storage::SnapshotPagePointer page_id,
  storage::Page** out) {
  return pimpl_->find_or_read_a_snapshot_page(page_id, out);
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
