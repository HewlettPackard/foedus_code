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
  ThreadGroupPimpl* group,
  ThreadId id,
  ThreadGlobalOrdinal global_ordinal)
  : pimpl_(nullptr) {
  pimpl_ = new ThreadPimpl(engine, group, this, id, global_ordinal);
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

memory::NumaCoreMemory* Thread::get_thread_memory() const { return pimpl_->core_memory_; }
memory::NumaNodeMemory* Thread::get_node_memory() const {
  return pimpl_->core_memory_->get_node_memory();
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

void Thread::hack_handle_one_task(ImpersonateTask* task, ImpersonateSession* session) {
  pimpl_->hack_handle_one_task(task, session);
}

std::ostream& operator<<(std::ostream& o, const Thread& v) {
  o << "Thread-" << v.get_thread_global_ordinal() << "(id=" << v.get_thread_id() << ") [";
  o << (v.pimpl_->current_task_.load() ? "I" : " ");
  o << (v.pimpl_->raw_thread_.is_stop_requested() ? "R" : " ");
  o << (v.pimpl_->raw_thread_.is_stopped() ? "E" : " ");
  o << "]";
  return o;
}

const memory::GlobalVolatilePageResolver& Thread::get_global_volatile_page_resolver() const {
  return pimpl_->engine_->get_memory_manager().get_global_volatile_page_resolver();
}

ThreadRef::ThreadRef()
  : engine_(CXX11_NULLPTR), id_(0), control_block_(CXX11_NULLPTR), mcs_blocks_(CXX11_NULLPTR) {}

ThreadRef::ThreadRef(Engine* engine, ThreadId id) : engine_(engine), id_(id) {
  soc::SharedMemoryRepo* memory_repo = engine->get_soc_manager().get_shared_memory_repo();
  soc::ThreadMemoryAnchors* anchors = memory_repo->get_thread_memory_anchors(id);
  control_block_ = anchors->thread_memory_;
  mcs_blocks_ = anchors->mcs_lock_memories_;
}


}  // namespace thread
}  // namespace foedus
