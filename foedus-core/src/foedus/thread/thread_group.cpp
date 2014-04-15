/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/thread/thread_group.hpp>
#include <foedus/thread/thread_group_pimpl.hpp>
#include <vector>
namespace foedus {
namespace thread {
ThreadGroup::ThreadGroup(Engine *engine, ThreadGroupId group_id) {
    pimpl_ = new ThreadGroupPimpl(engine, group_id);
}
ThreadGroup::~ThreadGroup() {
    delete pimpl_;
    pimpl_ = nullptr;
}

ErrorStack ThreadGroup::initialize() { return pimpl_->initialize(); }
bool ThreadGroup::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack ThreadGroup::uninitialize() { return pimpl_->uninitialize(); }

ThreadGroupId ThreadGroup::get_group_id() const { return pimpl_->group_id_; }
memory::NumaNodeMemory* ThreadGroup::get_node_memory() const { return pimpl_->node_memory_; }
ThreadLocalOrdinal ThreadGroup::get_thread_count() const { return pimpl_->threads_.size(); }
Thread* ThreadGroup::get_thread(ThreadLocalOrdinal ordinal) const {
    return pimpl_->threads_[ordinal];
}

}  // namespace thread
}  // namespace foedus
