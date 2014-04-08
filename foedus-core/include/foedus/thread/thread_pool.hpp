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
 * @brief The pool of pre-allocated threads in the engine to execute transactions.
 * @ingroup THREAD
 * @details
 * Detailed description of this class.
 */
class ThreadPool : public DefaultInitializable {
 public:
    ThreadPool() CXX11_FUNC_DELETE;
    explicit ThreadPool(const ThreadOptions &options);
    ErrorStack  initialize_once() CXX11_OVERRIDE;
    ErrorStack  uninitialize_once() CXX11_OVERRIDE;

 private:
    const ThreadOptions &options_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_POOL_HPP_
