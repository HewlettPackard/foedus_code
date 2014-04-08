/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_POOL_HPP_
#define FOEDUS_THREAD_THREAD_POOL_HPP_
#include <foedus/initializable.hpp>
#include <foedus/thread/fwd.hpp>
namespace foedus {
namespace thread {

/**
 * @defgroup THREADPOOL Thread-Pool and Impersonation
 * @brief APIs to \b pool and \b impersonate worker threads in libfoedus-core
 * @ingroup THREAD
 * @details
 * @par Thread Pool
 * bluh
 * @par Impersonation
 * bluh
 * @par Examples
 * bluh
 */

/**
 * @brief The pool of pre-allocated threads in the engine to execute transactions.
 * @ingroup THREADPOOL
 * @details
 * This is the main API class of thread package.
 */
class ThreadPool : public virtual Initializable {
 public:
    ThreadPool() CXX11_FUNC_DELETE;
    explicit ThreadPool(const ThreadOptions &options);
    ~ThreadPool();
    ErrorStack  initialize() CXX11_OVERRIDE CXX11_FINAL;
    bool        is_initialized() const CXX11_OVERRIDE CXX11_FINAL;
    ErrorStack  uninitialize() CXX11_OVERRIDE CXX11_FINAL;

 private:
    ThreadPoolPimpl*    pimpl_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_POOL_HPP_
