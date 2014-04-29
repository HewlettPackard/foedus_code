/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_STOPPABLE_THREAD_HPP_
#define FOEDUS_THREAD_STOPPABLE_THREAD_HPP_
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
namespace foedus {
namespace thread {
/**
 * @brief The frequently appearing quartet of std::thread, condition_varible, stop-request flag,
 * and mutex.
 * @ingroup THREAD
 * @details
 * This quartet helps implement a thread that consumes some task and also occasionally checks
 * if someone requested to stop this thread.
 * As this depends on C++11, the name of this file ends with impl. Thus, only private implementation
 * classes directly use this class. If you are okay with C++11, you can use it from client programs,
 * too.
 */
class StoppableThread final {
 public:
    StoppableThread() : stop_requested_(false) {}
    StoppableThread(const StoppableThread &other) = delete;
    StoppableThread& operator=(const StoppableThread &other) = delete;

    /** Initializes this object for the given thread. */
    void initialize(const std::string &name,
        std::thread &&the_thread, const std::chrono::microseconds &sleep_interval);
    /** An overload to receive the common pattern of names "Xxx-ordinal". */
    void initialize(const std::string &name_prefix, int32_t name_ordinal,
        std::thread &&the_thread, const std::chrono::microseconds &sleep_interval);

    /**
     * If the thread is still running, requests the thread to stop and waits until it exists.
     * If the thread has not started or has already stopped, do nothing (so, this is idempotent).
     */
    void stop();

    /**
     * If the thread is still running and also sleeping, requests the thread to immediately wakeup
     * and do its job. If the thread is not running or not sleeping, has no effect.
     */
    void wakeup();

    /**
     * Sleep until the interval elapses or someone requests to stop this thread.
     * @return whether someone has requested to stop this thread.
     */
    bool sleep();

    bool is_stop_requested() const { return stop_requested_; }

 private:
    /** Used only for debug logging. */
    std::string                     name_;
    /** Actual thread object. */
    std::thread                     thread_;
    /** How long do we sleep at most for each sleep() call. */
    std::chrono::microseconds       sleep_interval_;
    /** protectes the condition variable. */
    std::mutex                      mutex_;
    /** used to notify the thread to wakeup. */
    std::condition_variable         condition_;
    /** whether someone has requested to stop this. */
    bool                            stop_requested_;
};


}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_STOPPABLE_THREAD_HPP_
