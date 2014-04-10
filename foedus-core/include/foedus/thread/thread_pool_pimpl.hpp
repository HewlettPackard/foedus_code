/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_POOL_PIMPL_HPP_
#define FOEDUS_THREAD_THREAD_POOL_PIMPL_HPP_
#include <foedus/initializable.hpp>
#include <foedus/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <future>
#include <thread>
#include <vector>
namespace foedus {
namespace thread {

/**
 * @brief Pimpl object of ImpersonateSession.
 * @ingroup THREADPOOL
 * @details
 * A private pimpl object for ImpersonateSession.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ImpersonateSessionPimpl {
 public:
    explicit ImpersonateSessionPimpl(ImpersonateTask* task) : thread_(nullptr), task_(task) {}
    ImpersonateSessionPimpl(const ImpersonateSessionPimpl& other) { operator=(other); }
    ImpersonateSessionPimpl& operator=(const ImpersonateSessionPimpl& other) {
        thread_ = other.thread_;
        task_ = other.task_;
        failure_cause_ = other.failure_cause_;
        result_future_ = other.result_future_;
        return *this;
    }

    bool is_valid() const { return thread_ != nullptr; }
    ImpersonateSession::Status wait_for(TimeoutMicrosec timeout) const;

    /** The impersonated thread. If impersonation failed, NULL. */
    Thread*             thread_;

    /** The impersonated task running on this session. */
    ImpersonateTask*    task_;

    /** If impersonation failed, this indicates why it failed. */
    ErrorStack          failure_cause_;

    /** Expected result of the impersonated task. */
    std::shared_future<ErrorStack> result_future_;
};

/**
 * @brief Pimpl object of ThreadPool.
 * @ingroup THREADPOOL
 * @details
 * A private pimpl object for ThreadPool.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ThreadPoolPimpl : public DefaultInitializable {
 public:
    ThreadPoolPimpl() = delete;
    explicit ThreadPoolPimpl(Engine* engine) : engine_(engine), no_more_impersonation_(false) {}
    ErrorStack  initialize_once() override final;
    ErrorStack  uninitialize_once() override final;

    ImpersonateSession  impersonate(ImpersonateTask* functor, TimeoutMicrosec timeout);
    ImpersonateSession  impersonate_on_numa_node(ImpersonateTask* functor,
                                        ThreadGroupId numa_node, TimeoutMicrosec timeout);
    ImpersonateSession  impersonate_on_numa_core(ImpersonateTask* functor,
                                        ThreadId numa_core, TimeoutMicrosec timeout);

    ThreadGroup*        get_group(ThreadGroupId numa_node) const;
    Thread*             get_thread(ThreadId id) const;

    Engine* const               engine_;

    /**
     * List of ThreadGroup, one for each NUMA node in this engine.
     * Index is ThreadGroupId.
     */
    std::vector<ThreadGroup*>   groups_;

    /**
     * @brief Whether this thread pool has stopped allowing further impersonation.
     * @details
     * As the first step to terminate the entire engine, uninitialize() sets this to true,
     * prohibiting further impersonations from client code.
     */
    bool                        no_more_impersonation_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_POOL_PIMPL_HPP_
