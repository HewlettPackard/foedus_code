/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/assert_nd.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/thread/thread_group.hpp>
#include <foedus/thread/thread_group_pimpl.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread_pool_pimpl.hpp>
#include <foedus/thread/thread_options.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/thread/thread_pimpl.hpp>
#include <foedus/memory/memory_id.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <glog/logging.h>
#include <ostream>
namespace foedus {
namespace thread {
ErrorStack ThreadPoolPimpl::initialize_once() {
    if (!engine_->get_memory_manager().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    no_more_impersonation_ = false;
    ASSERT_ND(groups_.empty());
    const ThreadOptions &options = engine_->get_options().thread_;
    for (ThreadGroupId group_id = 0; group_id < options.group_count_; ++group_id) {
        memory::ScopedNumaPreferred numa_scope(group_id);
        groups_.push_back(new ThreadGroup(engine_, group_id));
        CHECK_ERROR(groups_.back()->initialize());
    }
    return kRetOk;
}

ErrorStack ThreadPoolPimpl::uninitialize_once() {
    ErrorStackBatch batch;
    if (!engine_->get_memory_manager().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }

    // first, announce that further impersonation is not allowed.
    no_more_impersonation_ = true;
    assorted::memory_fence_release();
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
    assorted::memory_fence_acquire();
    if (no_more_impersonation_) {
        session.invalid_cause_ = ERROR_STACK(ERROR_CODE_BEING_SHUTDOWN);
        return session;
    }

    for (ThreadGroup* group : groups_) {
        for (size_t j = 0; j < group->get_thread_count(); ++j) {
            Thread* thread = group->get_thread(j);
            if (thread->get_pimpl()->try_impersonate(&session)) {
                return session;
            }
        }
    }
    // TODO(Hideaki) : currently, timeout is ignored. It behaves as if timeout=0
    session.invalid_cause_ = ERROR_STACK(ERROR_CODE_TIMEOUT);
    LOG(WARNING) << "Failed to impersonate. pool=" << *this;
    return session;
}
ImpersonateSession ThreadPoolPimpl::impersonate_on_numa_node(ImpersonateTask* task,
                                    ThreadGroupId numa_node, TimeoutMicrosec /*timeout*/) {
    ImpersonateSession session(task);
    assorted::memory_fence_acquire();
    if (no_more_impersonation_) {
        session.invalid_cause_ = ERROR_STACK(ERROR_CODE_BEING_SHUTDOWN);
        return session;
    }

    ThreadGroup* group = groups_[numa_node];
    for (size_t i = 0; i < group->get_thread_count(); ++i) {
        Thread* thread = group->get_thread(i);
        if (thread->get_pimpl()->try_impersonate(&session)) {
            return session;
        }
    }
    // TODO(Hideaki) : currently, timeout is ignored. It behaves as if timeout=0
    session.invalid_cause_ = ERROR_STACK(ERROR_CODE_TIMEOUT);
    LOG(WARNING) << "Failed to impersonate(node="
        << static_cast<int>(numa_node)
        << "). pool=" << *this;
    return session;
}
ImpersonateSession ThreadPoolPimpl::impersonate_on_numa_core(ImpersonateTask* task,
                                    ThreadId numa_core, TimeoutMicrosec /*timeout*/) {
    ImpersonateSession session(task);
    assorted::memory_fence_acquire();
    if (no_more_impersonation_) {
        session.invalid_cause_ = ERROR_STACK(ERROR_CODE_BEING_SHUTDOWN);
        return session;
    }

    Thread* thread = get_thread(numa_core);
    if (!thread->get_pimpl()->try_impersonate(&session)) {
        // TODO(Hideaki) : currently, timeout is ignored. It behaves as if timeout=0
        session.invalid_cause_ = ERROR_STACK(ERROR_CODE_TIMEOUT);
    }
    LOG(WARNING) << "Failed to impersonate(core=" << numa_core << "). pool=" << *this;
    return session;
}

std::ostream& operator<<(std::ostream& o, const ThreadPoolPimpl& v) {
    o << "<ThreadPool>";
    o << "<no_more_impersonation_>" << v.no_more_impersonation_ << "</no_more_impersonation_>";
    o << "<groups>";
    for (ThreadGroup* group : v.groups_) {
        o << *group;
    }
    o << "</groups>";
    o << "</ThreadPool>";
    return o;
}

}  // namespace thread
}  // namespace foedus
