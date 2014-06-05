/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_PIMPL_HPP_
#define FOEDUS_THREAD_THREAD_PIMPL_HPP_
#include <foedus/initializable.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/thread/stoppable_thread_impl.hpp>
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/xct/xct.hpp>
#include <atomic>
namespace foedus {
namespace thread {
/**
 * @brief Pimpl object of Thread.
 * @ingroup THREAD
 * @details
 * A private pimpl object for Thread.
 * Do not include this header from a client program unless you know what you are doing.
 *
 * Especially, this class heavily uses C++11 classes, which is why we separate this class
 * from Thread. Be aware of notices in \ref CXX11 unless your client program allows C++11.
 */
class ThreadPimpl final : public DefaultInitializable {
 public:
    ThreadPimpl() = delete;
    ThreadPimpl(Engine* engine, ThreadGroupPimpl* group, Thread* holder, ThreadId id);
    ErrorStack  initialize_once() override final;
    ErrorStack  uninitialize_once() override final;

    /**
     * @brief Main routine of the worker thread.
     * @details
     * This method keeps checking current_task_. Whenever it retrieves a task, it runs
     * it and re-sets current_task_ when it's done. It exists when exit_requested_ is set.
     */
    void        handle_tasks();

    /**
     * Conditionally try to occupy this thread, or impersonate. If it fails, it immediately returns.
     * @param[in] session the session to run on this thread
     * @return whether successfully impersonated.
     */
    bool        try_impersonate(ImpersonateSession *session);

    Engine* const           engine_;

    /**
     * The thread group (NUMA node) this thread belongs to.
     */
    ThreadGroupPimpl* const group_;

    /**
     * The public object that holds this pimpl object.
     */
    Thread* const           holder_;

    /**
     * Unique ID of this thread.
     */
    const ThreadId          id_;

    /**
     * Private memory repository of this thread.
     * ThreadPimpl does NOT own it, meaning it doesn't call its initialize()/uninitialize().
     * EngineMemory owns it in terms of that.
     */
    memory::NumaCoreMemory* core_memory_;

    /**
     * Thread-private log buffer.
     */
    log::ThreadLogBuffer    log_buffer_;

    /**
     * Encapsulates raw thread object.
     * This is initialized/uninitialized in initialize()/uninitialize().
     */
    StoppableThread         raw_thread_;

    /**
     * The task this thread is currently running or will run when it wakes up.
     * Only one caller can impersonate a thread at once.
     * If this thread is not impersonated, null.
     */
    std::atomic<ImpersonateTask*>   current_task_;

    /**
     * Current transaction this thread is conveying.
     * Each thread can run at most one transaction at once.
     * If this thread is not conveying any transaction, current_xct_.is_active() == false.
     */
    xct::Xct                current_xct_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_PIMPL_HPP_
