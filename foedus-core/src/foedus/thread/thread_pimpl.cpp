/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/thread/impersonate_task_pimpl.hpp>
#include <foedus/thread/numa_thread_scope.hpp>
#include <foedus/thread/thread_pimpl.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread_pool_pimpl.hpp>
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <glog/logging.h>
#include <numa.h>
#include <atomic>
#include <future>
#include <mutex>
#include <thread>
namespace foedus {
namespace thread {
ThreadPimpl::ThreadPimpl(Engine* engine, ThreadGroupPimpl* group, Thread* holder, ThreadId id)
    : engine_(engine), group_(group), holder_(holder), id_(id), core_memory_(nullptr),
        log_buffer_(engine, id), current_task_(nullptr), current_xct_(engine, id) {
}

ErrorStack ThreadPimpl::initialize_once() {
    ASSERT_ND(engine_->get_memory_manager().is_initialized());
    core_memory_ = engine_->get_memory_manager().get_core_memory(id_);
    current_task_ = nullptr;
    current_xct_.initialize(id_, core_memory_);
    CHECK_ERROR(log_buffer_.initialize());
    raw_thread_.initialize("Thread-", id_,
                    std::thread(&ThreadPimpl::handle_tasks, this),
                    std::chrono::milliseconds(100));
    return kRetOk;
}
ErrorStack ThreadPimpl::uninitialize_once() {
    ErrorStackBatch batch;
    raw_thread_.stop();
    batch.emprace_back(log_buffer_.uninitialize());
    core_memory_ = nullptr;
    return SUMMARIZE_ERROR_BATCH(batch);
}

void ThreadPimpl::handle_tasks() {
    int numa_node = static_cast<int>(decompose_numa_node(id_));
    LOG(INFO) << "Thread-" << id_ << " started running on NUMA node: " << numa_node;
    NumaThreadScope scope(numa_node);
    // Actual xct processing can't start until XctManager is initialized.
    SPINLOCK_WHILE(!raw_thread_.is_stop_requested()
        && !engine_->get_xct_manager().is_initialized()) {
        assorted::memory_fence_acquire();
    }
    LOG(INFO) << "Thread-" << id_ << " now starts processing transactions";
    while (!raw_thread_.sleep()) {
        VLOG(0) << "Thread-" << id_ << " woke up";
        // Keeps running if the client sets a new task immediately after this.
        while (!raw_thread_.is_stop_requested()) {
            ImpersonateTask* task = current_task_.load();
            if (task) {
                VLOG(0) << "Thread-" << id_ << " retrieved a task";
                ErrorStack result = task->run(holder_);
                VLOG(0) << "Thread-" << id_ << " run(task) returned. result =" << result;
                ASSERT_ND(current_task_.load() == task);
                current_task_.store(nullptr);  // start receiving next task
                task->pimpl_->set_result(result);  // this wakes up the client
                VLOG(0) << "Thread-" << id_ << " finished a task. result =" << result;
            } else {
                // NULL functor is the signal to terminate
                break;
            }
        }
    }
    LOG(INFO) << "Thread-" << id_ << " exits";
}

bool ThreadPimpl::try_impersonate(ImpersonateSession *session) {
    ImpersonateTask* task = nullptr;
    session->thread_ = holder_;
    if (current_task_.compare_exchange_strong(task, session->task_)) {
        // successfully acquired.
        VLOG(0) << "Impersonation succeeded for Thread-" << id_ << ".";
        raw_thread_.wakeup();
        return true;
    } else {
        // no, someone else took it.
        ASSERT_ND(task);
        session->thread_ = nullptr;
        DVLOG(0) << "Someone already took Thread-" << id_ << ".";
        return false;
    }
}

}  // namespace thread
}  // namespace foedus
