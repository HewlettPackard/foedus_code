/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/thread/thread_pimpl.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread_pool_pimpl.hpp>
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/error_stack_batch.hpp>
#include <glog/logging.h>
#include <numa.h>
#include <foedus/assert_nd.hpp>
#include <atomic>
#include <future>
#include <mutex>
#include <thread>
namespace foedus {
namespace thread {
ThreadPimpl::ThreadPimpl(Engine* engine, ThreadGroupPimpl* group, Thread* holder, ThreadId id)
    : engine_(engine), group_(group), holder_(holder), id_(id), core_memory_(nullptr),
        log_buffer_(engine, id), exit_requested_(false), exitted_(false), impersonated_(false) {
}

ErrorStack ThreadPimpl::initialize_once() {
    core_memory_ = engine_->get_memory_manager().get_core_memory(id_);
    current_xct_.initialize(id_, core_memory_);
    impersonated_task_ = std::promise<ImpersonateTask*>();  // reset the promise/future pair
    raw_thread_ = std::thread();  // reset the thread object
    CHECK_ERROR(log_buffer_.initialize());
    raw_thread_ = std::thread(&ThreadPimpl::handle_tasks, this);
    return RET_OK;
}
ErrorStack ThreadPimpl::uninitialize_once() {
    ErrorStackBatch batch;
    if (!exit_requested_ && raw_thread_.joinable()) {
        exit_requested_ = true;
        impersonated_task_.set_value(nullptr);  // this signals that the thread should exit.
        raw_thread_.join();
    }
    batch.emprace_back(log_buffer_.uninitialize());
    core_memory_ = nullptr;
    return SUMMARIZE_ERROR_BATCH(batch);
}

void ThreadPimpl::handle_tasks() {
    int numa_node = static_cast<int>(decompose_numa_node(id_));
    LOG(INFO) << "Thread-" << id_ << " started running on NUMA node: " << numa_node;
    ::numa_run_on_node(numa_node);
    while (true) {
        VLOG(0) << "Thread-" << id_ << " waiting for a task...";
        std::future<ImpersonateTask*> task_future = impersonated_task_.get_future();
        ImpersonateTask* functor = task_future.get();
        impersonated_task_ = std::promise<ImpersonateTask*>();  // reset the promise/future pair
        if (functor) {
            ASSERT_ND(impersonated_);
            VLOG(0) << "Thread-" << id_ << " retrieved a task";
            ErrorStack result = functor->run(holder_);
            assorted::memory_fence_release();
            impersonated_ = false;
            assorted::memory_fence_release();
            impersonated_task_result_.set_value(result);  // this wakes up the client
            assorted::memory_fence_release();
            VLOG(0) << "Thread-" << id_ << " finished a task. result =" << result;
        } else {
            // NULL functor is the signal to terminate
            break;
        }
    }
    exitted_ = true;
    assorted::memory_fence_release();
    LOG(INFO) << "Thread-" << id_ << " exits";
}

bool ThreadPimpl::try_impersonate(ImpersonateSession *session) {
    bool cas_tmp = false;
    if (impersonated_.compare_exchange_weak(cas_tmp, true)) {
        // successfully acquired. set a new promise for this session.
        VLOG(0) << "Impersonation succeeded for Thread-" << id_ << ". Setting a task..";
        // Assign a new promise for *me*
        std::promise<ErrorStack> new_promise;
        impersonated_task_result_.swap(new_promise);
        session->thread_ = holder_;
        *reinterpret_cast< std::shared_future< ErrorStack >* >(session->result_future_)
            = impersonated_task_result_.get_future().share();
        assorted::memory_fence_acq_rel();
        impersonated_task_.set_value(session->task_);
        return true;
    } else {
        // no, someone else took it.
        DVLOG(0) << "Someone already took Thread-" << id_ << ".";
        return false;
    }
}

}  // namespace thread
}  // namespace foedus
