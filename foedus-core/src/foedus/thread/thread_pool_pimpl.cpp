/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/thread/thread_group.hpp>
#include <foedus/thread/thread_group_pimpl.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread_pool_pimpl.hpp>
#include <foedus/thread/thread_options.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/thread/thread_pimpl.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/engine_options.hpp>
#include <cassert>
#include <atomic>
namespace foedus {
namespace thread {
ErrorStack ThreadPoolPimpl::initialize_once() {
    if (!engine_->get_memory_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    no_more_impersonation_ = false;
    const ThreadOptions &options = engine_->get_options().thread_;
    for (ThreadGroupId group_id = 0; group_id < options.group_count_; ++group_id) {
        groups_.push_back(new ThreadGroup(engine_, group_id));
        CHECK_ERROR(groups_.back()->initialize());
    }
    return RET_OK;
}

ErrorStack ThreadPoolPimpl::uninitialize_once() {
    ErrorStackBatch batch;
    if (!engine_->get_memory_manager().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }

    // first, announce that further impersonation is not allowed.
    no_more_impersonation_ = true;
    std::atomic_thread_fence(std::memory_order_release);
    batch.uninitialize_and_delete_all(&groups_);
    return SUMMARIZE_ERROR_BATCH(batch);
}
ThreadGroup* ThreadPoolPimpl::get_group(ThreadGroupId numa_node) const {
    return groups_[numa_node];
}
Thread* ThreadPoolPimpl::get_thread(ThreadId id) const {
    return get_group(decompose_numa_node(id))->get_thread(decompose_numa_local_ordinal(id));
}

ImpersonateSession ThreadPoolPimpl::impersonate(ImpersonateTask* task,
                                               TimeoutMicrosec /*timeout*/) {
    ImpersonateSession session(task);
    std::atomic_thread_fence(std::memory_order_acquire);
    if (no_more_impersonation_) {
        session.invalid_cause_ = ERROR_STACK(ERROR_CODE_BEING_SHUTDOWN);
        return session;
    }

    for (size_t i = 0; i < groups_.size(); ++i) {
        ThreadGroupPimpl* group = groups_[i]->pimpl_;
        for (size_t j = 0; j < group->threads_.size(); ++j) {
            Thread* thread = group->threads_[j];
            if (thread->pimpl_->try_impersonate(&session)) {
                return session;
            }
        }
    }
    // TODO(Hideaki) : currently, timeout is ignored. It behaves as if timeout=0
    session.invalid_cause_ = ERROR_STACK(ERROR_CODE_TIMEOUT);
    return session;
}
ImpersonateSession ThreadPoolPimpl::impersonate_on_numa_node(ImpersonateTask* task,
                                    ThreadGroupId numa_node, TimeoutMicrosec /*timeout*/) {
    ImpersonateSession session(task);
    std::atomic_thread_fence(std::memory_order_acquire);
    if (no_more_impersonation_) {
        session.invalid_cause_ = ERROR_STACK(ERROR_CODE_BEING_SHUTDOWN);
        return session;
    }

    ThreadGroupPimpl* group = get_group(numa_node)->pimpl_;
    for (size_t i = 0; i < group->threads_.size(); ++i) {
        Thread* thread = group->threads_[i];
        if (thread->pimpl_->try_impersonate(&session)) {
            return session;
        }
    }
    // TODO(Hideaki) : currently, timeout is ignored. It behaves as if timeout=0
    session.invalid_cause_ = ERROR_STACK(ERROR_CODE_TIMEOUT);
    return session;
}
ImpersonateSession ThreadPoolPimpl::impersonate_on_numa_core(ImpersonateTask* task,
                                    ThreadId numa_core, TimeoutMicrosec /*timeout*/) {
    ImpersonateSession session(task);
    std::atomic_thread_fence(std::memory_order_acquire);
    if (no_more_impersonation_) {
        session.invalid_cause_ = ERROR_STACK(ERROR_CODE_BEING_SHUTDOWN);
        return session;
    }

    Thread* thread = get_thread(numa_core);
    if (!thread->pimpl_->try_impersonate(&session)) {
        // TODO(Hideaki) : currently, timeout is ignored. It behaves as if timeout=0
        session.invalid_cause_ = ERROR_STACK(ERROR_CODE_TIMEOUT);
    }
    return session;
}

}  // namespace thread
}  // namespace foedus
