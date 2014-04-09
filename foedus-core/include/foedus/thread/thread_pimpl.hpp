/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_PIMPL_HPP_
#define FOEDUS_THREAD_THREAD_PIMPL_HPP_
#include <foedus/initializable.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <atomic>
#include <future>
#include <mutex>
#include <thread>
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
class ThreadPimpl : public DefaultInitializable {
 public:
    ThreadPimpl() = delete;
    ThreadPimpl(Engine* engine, ThreadGroupPimpl* group, Thread* holder, ThreadId id)
        : engine_(engine), group_(group), holder_(holder), id_(id),
            core_memory_(nullptr), raw_thread_(nullptr), exitted_(false) {}
    ErrorStack  initialize_once() override final;
    ErrorStack  uninitialize_once() override final;

    /**
     * @brief Main routine of the worker thread.
     * @details
     * This method keeps checking impersonated_task_. Whenever it retrieves a task, it runs
     * it and re-sets impersonated_task_ when it's done.
     */
    void        handle_tasks();

    /**
     * Conditionally try to occupy this thread, or impersonate. If it fails, it immediately returns.
     * @param[in] session the session to run on this thread
     * @return whether successfully impersonated.
     */
    bool        try_impersonate(ImpersonateSessionPimpl *session);

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
     * Encapsulated raw C++11 thread object.
     * This is allocated/deallocated in initialize()/uninitialize().
     */
    std::thread*            raw_thread_;

    /**
     * Whether this thread has ended.
     */
    bool                    exitted_;

    /**
     * Whether this thread is impersonated and running some code.
     * Only one caller can impersonate a thread at once.
     */
    std::atomic<bool>       impersonated_;

    /**
     * This thread waits for future from this std::promise to retrieve next functor to run.
     * Whenever the thread finishes a task, it re-set this std::promise to receive next task.
     * When the engine shuts down, it calls this task with NULL functor.
     * It works as a signal to let this thread exit rather than waiting for yet another task.
     */
    std::promise< ImpersonateTask* > impersonated_task_;

    /**
     * Protects from concurrent accesses to impersonated_task_.
     */
    std::mutex                  impersonated_task_mutex_;

    /**
     * The \e promise of the result of previous impersonated execution.
     * The pooled thread calls set_value when it finishes the current task.
     * When this receives a new task (when impersonated_ sets true), it creates a new promise
     * so that the client program of the old session can still see the corresponding future.
     */
    std::promise<ErrorStack>    impersonated_task_result_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_PIMPL_HPP_
