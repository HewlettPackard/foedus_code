/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_RENDEZVOUS_IMPL_HPP_
#define FOEDUS_THREAD_RENDEZVOUS_IMPL_HPP_
#include <foedus/assert_nd.hpp>
#include <chrono>
#include <condition_variable>
#include <mutex>
namespace foedus {
namespace thread {
/**
 * @brief The frequently appearing triplet of condition_varible, "signal" flag for spurious wakeup,
 * and mutex for a one-time single-producer multiple-consumer event synchronization.
 * @ingroup THREAD
 * @details
 * This is basically equivalent to std::promise/future pair with no parameter.
 * The frequent use case is to synchronize with some event for one producer and many waiters.
 * We did use std::promise/future pair for this purpose, but we encountered a bug
 * in libstdc's implementation of std::promise/future.
 *   https://gcc.gnu.org/bugzilla/show_bug.cgi?id=57440
 *
 * We are not sure when the fix will be made, nor when the fixed version of gcc/libstdc++ will be
 * prevalent to all environments we support. Very unlikely we can't afford to wait for it.
 * Therefore, we roll it our own.
 *
 * As this depends on C++11, the name of this file ends with impl. Thus, only private implementation
 * classes directly use this class. If you are okay with C++11, you can use it from client programs,
 * too.
 *
 * This class is totally header-only.
 */
class Rendezvous final {
 public:
    Rendezvous() : signaled_(false) {}

    // not copyable, assignable.
    Rendezvous(const Rendezvous &other) = delete;
    Rendezvous& operator=(const Rendezvous &other) = delete;
    Rendezvous(Rendezvous &&other) = delete;
    Rendezvous& operator=(Rendezvous &&other) = delete;

    /**
     * @brief Block until the event happens.
     * @details
     * Equivalent to std::future<void>::wait().
     */
    void wait() {
        if (signaled_) {
            return;
        }
        std::unique_lock<std::mutex> the_lock(mutex_);
        condition_.wait(the_lock, [this]{ return signaled_; });
    }

    /**
     * @brief Block until the event happens \b or the given period elapses.
     * @return whether the event happened by now.
     * @details
     * Equivalent to std::future<void>::wait_for().
     */
    template<class REP, class PERIOD>
    bool wait_for(const std::chrono::duration<REP, PERIOD>& timeout) {
        if (signaled_) {
            return true;
        }
        std::unique_lock<std::mutex> the_lock(mutex_);
        return condition_.wait_for< REP, PERIOD >(the_lock, timeout, [this]{ return signaled_; });
    }

    /**
     * @brief Block until the event happens \b or the given time point arrives.
     * @return whether the event happened by now.
     * @details
     * Equivalent to std::future<void>::wait_until().
     */
    template< class CLOCK, class DURATION >
    bool wait_until(const std::chrono::time_point<CLOCK, DURATION>& until) {
        if (signaled_) {
            return true;
        }
        std::unique_lock<std::mutex> the_lock(mutex_);
        return condition_.wait_for< CLOCK, DURATION >(the_lock, until, [this]{ return signaled_; });
    }

    /**
     * @brief Notify all waiters that the event has happened.
     * @details
     * Equivalent to std::promise<void>::set_value().
     * There must be only one thread that might call this method, and it should call this only once.
     * Otherwise, the behavior is undefined.
     */
    void signal() {
        ASSERT_ND(!signaled_);
        signaled_ = true;
        condition_.notify_all();
    }

    /** returns whether this thread has stopped (if the thread hasn't started, false too). */
    bool is_signaled() const { return signaled_; }

 private:
    /** protects the condition variable. */
    std::mutex                      mutex_;
    /** used to notify waiters to wakeup. */
    std::condition_variable         condition_;
    /** whether this thread has stopped (if the thread hasn't started, false too). */
    bool                            signaled_;
};


}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_RENDEZVOUS_IMPL_HPP_
