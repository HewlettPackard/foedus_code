/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/thread.hpp>
#include <foedus/thread/thread_pimpl.hpp>
namespace foedus {
namespace thread {
Thread::Thread(Engine* engine, ThreadGroupPimpl* group, ThreadId id) : pimpl_(nullptr) {
    pimpl_ = new ThreadPimpl(engine, group, this, id);
}
Thread::~Thread() {
    delete pimpl_;
    pimpl_ = nullptr;
}

ErrorStack Thread::initialize() { return pimpl_->initialize(); }
bool Thread::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack Thread::uninitialize() { return pimpl_->uninitialize(); }

Engine*     Thread::get_engine()        const { return pimpl_->engine_; }
ThreadId    Thread::get_thread_id()     const { return pimpl_->id_; }
bool        Thread::is_running_xct()    const { return pimpl_->current_xct_.is_active(); }
memory::NumaCoreMemory* Thread::get_thread_memory() const { return pimpl_->core_memory_; }
void        Thread::activate_xct()      { return pimpl_->activate_xct(); }
void        Thread::deactivate_xct()    { return pimpl_->deactivate_xct(); }
xct::Xct&   Thread::get_current_xct()   { return pimpl_->current_xct_; }
log::ThreadLogBuffer& Thread::get_thread_log_buffer() { return pimpl_->log_buffer_; }


}  // namespace thread
}  // namespace foedus
