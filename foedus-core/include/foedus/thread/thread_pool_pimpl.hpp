/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_POOL_PIMPL_HPP_
#define FOEDUS_THREAD_THREAD_POOL_PIMPL_HPP_
#include <foedus/initializable.hpp>
#include <foedus/fwd.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <vector>
namespace foedus {
namespace thread {
/**
 * @brief Pimpl object of ThreadPool.
 * @ingroup THREADPOOL
 * @details
 * A private pimpl object for ThreadPool.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ThreadPoolPimpl : public DefaultInitializable {
 public:
    ThreadPoolPimpl() CXX11_FUNC_DELETE;
    explicit ThreadPoolPimpl(Engine* engine) : engine_(engine) {}
    ErrorStack  initialize_once() CXX11_OVERRIDE;
    ErrorStack  uninitialize_once() CXX11_OVERRIDE;

 private:
    Engine* const               engine_;

    /**
     * List of ThreadGroup, one for each NUMA node in this engine.
     * Index is ThreadGroupId.
     */
    std::vector<ThreadGroup*>   groups_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_POOL_PIMPL_HPP_
