/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_STOPPABLE_THREAD_IMPL_HPP_
#define FOEDUS_THREAD_STOPPABLE_THREAD_IMPL_HPP_
#include <foedus/thread/condition_variable_impl.hpp>
#include <atomic>
#include <chrono>
#include <iosfwd>
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
 *
 * @todo initialize() should only set thread. We should separate other initialization from it
 * because the new thread might be already accessing those properties
 * (at least valgrind is unhappy). We should have 'launch()' method instead.
 */
class StoppableThread final {
 public:
  StoppableThread() : sleep_interval_(0),
    started_(false), stop_requested_(false), stopped_(false) {}

  // non-copyable assignable. (maybe better to provide move, but no need so far)
  StoppableThread(const StoppableThread &other) = delete;
  StoppableThread& operator=(const StoppableThread &other) = delete;
  StoppableThread(StoppableThread &&other) = delete;
  StoppableThread& operator=(StoppableThread &&other) = delete;

  /** Initializes this object for the given thread. */
  void initialize(const std::string &name,
    std::thread &&the_thread, const std::chrono::microseconds &sleep_interval);
  /** An overload to receive the common pattern of names "Xxx-ordinal". */
  void initialize(const std::string &name_prefix, int32_t name_ordinal,
    std::thread &&the_thread, const std::chrono::microseconds &sleep_interval);

  /**
   * request_stop() plus wait_for_stop().
   * This method is idempotent. If the thread is not running already, this immediately returns.
   */
  void stop();

  /**
   * If the thread is still running, requests the thread to stop and waits until it exists.
   * If the thread has not started or has already stopped, do nothing (so, this is idempotent).
   * Unlike stop(), this method doesn't join the thread. So, it immediately returns.
   */
  void request_stop();

  /**
   * Blocks until the thread stops. If the thread is not running anyways, this does nothing.
   */
  void wait_for_stop();

  /**
   * If the thread is still running and also sleeping, requests the thread to immediately wakeup
   * and do its job. If the thread is not running or not sleeping, has no effect.
   */
  void wakeup();

  /**
   * Sleep until the interval elapses or someone requests to stop this thread.
   * @return whether someone has requested to stop this thread.
   * @details
   * For example, use it as follows.
   * @code{.cpp}
   * void my_thread_handler(StoppableThread* me) {
   *   while (!me->sleep()) {
   *     // some stuff
   *   }
   * }
   * @endcode
   */
  bool sleep();

  /** returns whether someone has requested to stop this. */
  bool is_stop_requested() const { return stop_requested_; }
  /** non-atomic is_stop_requested(). */
  bool is_stop_requested_weak() const {
    return stop_requested_.load(std::memory_order_relaxed);
  }

  /** returns whether this thread has stopped (if the thread hasn't started, false too). */
  bool is_stopped() const { return stopped_; }
  /** non-atomic is_stopped(). */
  bool is_stopped_weak() const { return stopped_.load(std::memory_order_relaxed); }

  std::string             to_string() const;
  friend std::ostream&    operator<<(std::ostream& o, const StoppableThread& v);

 private:
  /** Used only for debug logging. */
  std::string                     name_;
  /** Actual thread object. */
  std::thread                     thread_;
  /** How long do we sleep at most for each sleep() call. */
  std::chrono::microseconds       sleep_interval_;
  /** used to notify the thread to wakeup. */
  ConditionVariable               condition_;
  /** whether this thread has started. */
  std::atomic<bool>               started_;
  /** whether someone has requested to stop this. */
  std::atomic<bool>               stop_requested_;
  /** whether this thread has stopped (if the thread hasn't started, false too). */
  std::atomic<bool>               stopped_;
};


}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_STOPPABLE_THREAD_IMPL_HPP_
