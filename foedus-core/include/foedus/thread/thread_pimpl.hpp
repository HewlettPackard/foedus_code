/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_PIMPL_HPP_
#define FOEDUS_THREAD_THREAD_PIMPL_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/initializable.hpp>
#include <foedus/thread/fwd.hpp>
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
 * Especially, this class heavily uses C++11's std::thread, which is why we separate this class
 * from Thread. Be aware of notices in \ref CXX11 unless your client program allows C++11.
 */
class ThreadPimpl : public DefaultInitializable {
 public:
    ThreadPimpl() CXX11_FUNC_DELETE;
    ThreadPimpl(ThreadGroup* group, ThreadId id);
    ErrorStack  initialize_once() CXX11_OVERRIDE;
    ErrorStack  uninitialize_once() CXX11_OVERRIDE;

    /**
     * The thread group (NUMA node) this thread belongs to.
     */
    ThreadGroup* const      group_;

    /**
     * Unique ID of this thread.
     */
    const ThreadId          id_;

    /**
     * Encapsulated raw C++11 thread object.
     */
    std::thread*            raw_thread_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_PIMPL_HPP_
