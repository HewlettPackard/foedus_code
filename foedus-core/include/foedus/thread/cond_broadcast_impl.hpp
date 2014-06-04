/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_COND_BROADCAST_IMPL_HPP_
#define FOEDUS_THREAD_COND_BROADCAST_IMPL_HPP_
#include <foedus/assert_nd.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <stdint.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
namespace foedus {
namespace thread {
/**
 * @brief An analogue of pthread's condition variable and std::condition_variable
 * that is used with pthread_cond_broadcast() and notify_all() to avoid glibc's bug.
 * @ingroup THREAD
 * @details
 * The whole purpose of this class it to workaound a bug in glibc's pthread_cond_broadcast(),
 * which is also used by std::condition_variable::notify_all(). Use this if you have to wakeup
 * multiple waitors. If you have at most one waitor, just use std::condition_variable::notify_one().
 *
 * The bug is already fixed here:
 *   https://sourceware.org/git/?p=glibc.git;a=commitdiff;h=8f630cca5c36941db1cb48726016bbed80ec1041
 * However, it will not be available until glibc 2.20, which will be employed in major linux distros
 * much much later. So, we work around the issue ourselves for a while.
 *
 * As this depends on C++11, the name of this file ends with impl. Thus, only private implementation
 * classes directly use this class. If you are okay with C++11, you can use it from client programs,
 * too.
 *
 * This class is totally header-only.
 */
class CondBroadcast final {
 public:
    CondBroadcast() : waiters_(0), notifiers_(0) {}
    ~CondBroadcast() {
        ASSERT_ND(waiters_ == 0);
        // we must wait until all notifiers exit notify_all
        while (notifiers_ > 0) {
            assorted::spinlock_yield();
        }
    }

    // not copyable, assignable.
    CondBroadcast(const CondBroadcast &other) = delete;
    CondBroadcast& operator=(const CondBroadcast &other) = delete;
    CondBroadcast(CondBroadcast &&other) = delete;
    CondBroadcast& operator=(CondBroadcast &&other) = delete;

    /**
     * @brief Block until the event happens.
     * @details
     * Equivalent to std::condition_variable::wait().
     */
    template<typename PREDICATE>
    void wait(std::unique_lock<std::mutex>& lock,  // NOLINT same as std::condition_variable
              PREDICATE predicate) {
        ASSERT_ND(lock.owns_lock());
        ++waiters_;
        condition_.wait(lock, predicate);
        ASSERT_ND(lock.owns_lock());
        ASSERT_ND(waiters_ > 0);
        --waiters_;
    }

    /**
     * @brief Block until the event happens \b or the given period elapses.
     * @return whether the event happened by now.
     * @details
     * Equivalent to std::condition_variable::wait_for().
     */
    template<class REP, class PERIOD, typename PREDICATE>
    bool wait_for(std::unique_lock<std::mutex>& lock,  // NOLINT same as std::condition_variable
        const std::chrono::duration<REP, PERIOD>& timeout,
        PREDICATE predicate) {
        ASSERT_ND(lock.owns_lock());
        ++waiters_;
        bool happened = condition_.wait_for(lock, timeout, predicate);
        ASSERT_ND(lock.owns_lock());
        ASSERT_ND(waiters_ > 0);
        --waiters_;
        return happened;
    }

    /**
     * @brief Block until the event happens \b or the given time point arrives.
     * @return whether the event happened by now.
     * @details
     * Equivalent to std::condition_variable::wait_until().
     */
    template< class CLOCK, class DURATION, typename PREDICATE>
    bool wait_until(std::unique_lock<std::mutex>& lock,  // NOLINT same as std::condition_variable
                    const std::chrono::time_point<CLOCK, DURATION>& until,
                    PREDICATE predicate) {
        ASSERT_ND(lock.owns_lock());
        ++waiters_;
        bool happened = condition_.wait_until(lock, until, predicate);
        ASSERT_ND(lock.owns_lock());
        ASSERT_ND(waiters_ > 0);
        --waiters_;
        return happened;
    }

    /**
     * @brief Notify all waiters that the event has happened.
     * @details
     * Equivalent to std::condition_variable::notify_all().
     * To workaround the pthread_cond_broadcast bug, this method notifies one by one.
     * We might add a switch of the behavior by checking glibc version.
     */
    void notify_all(std::unique_lock<std::mutex>& lock) {  // NOLINT same as std::condition_variable
        ++notifiers_;
        while (true) {
            ASSERT_ND(lock.owns_lock());
            if (waiters_ > 0) {
                lock.unlock();
                condition_.notify_one();
                assorted::spinlock_yield();
                lock.lock();
            } else {
                break;
            }
        }
        --notifiers_;
    }

 private:
    /** used to notify waiters to wakeup. */
    std::condition_variable         condition_;

    /** Number of waitors. */
    std::atomic<uint32_t>           waiters_;
    /** Number of notifiers, used to safely destruct this object. */
    std::atomic<uint32_t>           notifiers_;
};


}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_COND_BROADCAST_IMPL_HPP_
