/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/thread.hpp>
#include <foedus/thread/thread_pimpl.hpp>
namespace foedus {
namespace thread {
Thread::Thread(Engine* engine, ThreadGroupPimpl* group, ThreadId id) {
    pimpl_ = new ThreadPimpl(engine, group, this, id);
}
Thread::~Thread() {
    delete pimpl_;
    pimpl_ = nullptr;
}

ErrorStack Thread::initialize() { return pimpl_->initialize(); }
bool Thread::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack Thread::uninitialize() { return pimpl_->uninitialize(); }

Engine*     Thread::get_engine() const { return pimpl_->engine_; }
ThreadId    Thread::get_thread_id() const { return pimpl_->id_; }

}  // namespace thread
}  // namespace foedus
