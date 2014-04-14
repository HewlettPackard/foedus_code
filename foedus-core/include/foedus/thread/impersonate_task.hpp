/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_IMPERSONATE_TASK_HPP_
#define FOEDUS_THREAD_IMPERSONATE_TASK_HPP_
#include <foedus/error_stack.hpp>
#include <foedus/thread/fwd.hpp>
namespace foedus {
namespace thread {
/**
 * @brief A pure-virtual functor object to be executed when impersonation succedes.
 * @ingroup THREADPOOL
 * @details
 * The user program should define a class that derives from this class and pass it to
 * ThreadPool#impersonate(). Each task might be fine-grained (eg. one short transaction as a task)
 * or coarse-graiend (eg. a series of transactions assigned to one thread/CPU-core, like DORA).
 * This interface is totally general, so it's up to the client.
 */
class ImpersonateTask {
 public:
    virtual ~ImpersonateTask() {}

    /**
     * @brief The method called back from the engine on one of its pooled threads.
     * @param[in] context the impersonated thread
     * @details
     * Note that the thread calling this method is NOT the client program's thread
     * that invoked ThreadPool#impersonate(), but an asynchronous thread that was pre-allocated
     * in the engine.
     * When this method returns, the engine releases the impersonated thread to the thread pool
     * so that other clients can grab it for new sessions.
     */
    virtual ErrorStack run(Thread* context) = 0;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_IMPERSONATE_TASK_HPP_
