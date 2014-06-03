/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_RENDEZVOUS_IMPL_HPP_
#define FOEDUS_THREAD_RENDEZVOUS_IMPL_HPP_
#include <foedus/assert_nd.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <stdint.h>
#include <atomic>
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
    Rendezvous() : signaled_(false)
#ifndef NDEBUG
        , debug_already_destroyed_(false), debug_waitors_(0)
#endif  // NDEBUG
    {
    }

    ~Rendezvous() {
#ifndef NDEBUG
        ASSERT_ND(!debug_already_destroyed_.load());
        ASSERT_ND(debug_waitors_.load() == 0);
        debug_already_destroyed_.store(true);
#endif  // NDEBUG
    }

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
#ifndef NDEBUG
        WaitorScope waitor_scope(this);
#endif  // NDEBUG
        if (is_signaled()) {
            return;
        }
        std::unique_lock<std::mutex> the_lock(mutex_);
        debug_assert_already_destroyed();
        condition_.wait(the_lock, [this]{ return is_signaled(); });
    }

    /**
     * @brief Block until the event happens \b or the given period elapses.
     * @return whether the event happened by now.
     * @details
     * Equivalent to std::future<void>::wait_for().
     */
    template<class REP, class PERIOD>
    bool wait_for(const std::chrono::duration<REP, PERIOD>& timeout) {
#ifndef NDEBUG
        WaitorScope waitor_scope(this);
#endif  // NDEBUG
        if (is_signaled()) {
            return true;
        }
        std::unique_lock<std::mutex> the_lock(mutex_);
        debug_assert_already_destroyed();
        return condition_.wait_for<REP, PERIOD>(the_lock, timeout, [this]{ return is_signaled(); });
    }

    /**
     * @brief Block until the event happens \b or the given time point arrives.
     * @return whether the event happened by now.
     * @details
     * Equivalent to std::future<void>::wait_until().
     */
    template< class CLOCK, class DURATION >
    bool wait_until(const std::chrono::time_point<CLOCK, DURATION>& until) {
#ifndef NDEBUG
        WaitorScope waitor_scope(this);
#endif  // NDEBUG
        if (is_signaled()) {
            return true;
        }
        std::unique_lock<std::mutex> the_lock(mutex_);
        debug_assert_already_destroyed();
        return condition_.wait_for<CLOCK, DURATION>(the_lock, until, [this]{
            return is_signaled();
        });
    }

    /**
     * @brief Notify all waiters that the event has happened.
     * @details
     * Equivalent to std::promise<void>::set_value().
     * There must be only one thread that might call this method, and it should call this only once.
     * Otherwise, the behavior is undefined.
     */
    void signal() {
        ASSERT_ND(!is_signaled());
        // signal while holding lock. This is to avoid lost signal or spurious blocking.
        // http://www.domaigne.com/blog/computing/condvars-signal-with-mutex-locked-or-not/
        // http://stackoverflow.com/questions/15072479/stdcondition-variable-spurious-blocking
        // yes, you will also see contradicting articles on the web, but this is the safe way.
        std::lock_guard<std::mutex> guard(mutex_);
        signaled_.store(true);
        debug_assert_already_destroyed();
        condition_.notify_all();
        // after notify_all, it IS possible that this object is deleted by concurrent thread,
        // but that should be fine.
    }

    /** returns whether this thread has stopped (if the thread hasn't started, false too). */
    bool is_signaled() const {
        debug_assert_already_destroyed();
        return signaled_.load();
    }
    /** non-atomic is_signaled(). */
    bool is_signaled_weak() const {
        debug_assert_already_destroyed();
        return signaled_.load(std::memory_order_relaxed);
    }

    /** In release mode, this function will go away. */
    void debug_assert_already_destroyed() const {
#ifndef NDEBUG
        ASSERT_ND(!debug_already_destroyed_.load());
#endif  // NDEBUG
    }

 private:
    /** protects the condition variable. */
    std::mutex                      mutex_;
    /** used to notify waiters to wakeup. */
    std::condition_variable         condition_;
    /** whether this thread has stopped (if the thread hasn't started, false too). */
    std::atomic<bool>               signaled_;

#ifndef NDEBUG
    /** Check for double-free. */
    std::atomic<bool>               debug_already_destroyed_;
    /**
     * Only for sanity check in debug mode.
     * pthread_cond_broadcast or kernel futex has some bug??
     * http://www.redhat.com/archives/phil-list/2004-April/msg00002.html
     * I shouldn't be hitting this decade-old bug, but what is it??
     *
     * Ohhh, probabably this:
     * https://sourceware.org/git/?p=glibc.git;a=commitdiff;h=8f630cca5c36941db1cb48726016bbed80ec1041
     * Crap, it's a glibc bug.
     */
    std::atomic<uint32_t>           debug_waitors_;
    struct WaitorScope {
        explicit WaitorScope(Rendezvous* enclosure) : enclosure_(enclosure) {
            ++enclosure_->debug_waitors_;
        }
        ~WaitorScope() {
            --enclosure_->debug_waitors_;
        }
        Rendezvous* const enclosure_;
    };
#endif  // NDEBUG
};


}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_RENDEZVOUS_IMPL_HPP_
