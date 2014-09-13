/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SOC_SHARED_COND_HPP_
#define FOEDUS_SOC_SHARED_COND_HPP_

#include <pthread.h>
#include <stdint.h>

#include "foedus/cxx11.hpp"
#include "foedus/soc/shared_mutex.hpp"

namespace foedus {
namespace soc {
/**
 * @brief A conditional variable that can be placed in shared memory
 * and used from multiple processes.
 * @ingroup SOC
 * @details
 * Analogous to SharedMutex. This is for conditional variable.
 * This class also avoids the pthread bug described in foedus::thread::ConditionVariable.
 * This file does not assume C++11.
 */
class SharedCond CXX11_FINAL {
 public:
  SharedCond() : initialized_(false), waiters_(0), notifiers_(0) { initialize(); }
  ~SharedCond() { uninitialize(); }

  // Disable copy constructors
  SharedCond(const SharedCond&) CXX11_FUNC_DELETE;
  SharedCond& operator=(const SharedCond&) CXX11_FUNC_DELETE;

  void initialize();
  void uninitialize();
  bool is_initialized() const { return initialized_; }

  /**
   * @brief Unconditionally wait for the event.
   * @param[in,out] scope the mutex scope that protects this conditional variable
   * @pre scope->is_locked_by_me() (the caller must have locked it)
   * @pre scope->get_mutex() == &mutex_ (the scope should be protecting this variable)
   * @post scope is still locked
   * @details
   * This method does \b NOT rule out spurrious wakeup.
   * We could receive a lambda to check the condition, but this class should be C++11-free.
   * So, the caller should do the loop herself if she doesn't want a spurrious wakeup.
   * Instead, you can easily avoid lost signals by checking the condition after locking the mutex
   * before calling this method.
   */
  void wait(SharedMutexScope* scope);

  /**
   * @brief Wait for the event up to the given timeout.
   * @param[in,out] scope the mutex scope that protects this conditional variable
   * @param[in] timeout_nanosec timeout in nanoseconds
   * @pre scope->is_locked_by_me() (the caller must have locked it)
   * @pre scope->get_mutex() == &mutex_ (the scope should be protecting this variable)
   * @post scope is still locked
   * @return whether this thread received the signal (though still spurrious wakeup possible).
   * false if timeout happened.
   * @details
   * This method does \b NOT rule out spurrious wakeup as described above.
   */
  bool timedwait(SharedMutexScope* scope, uint64_t timeout_nanosec);

  /**
   * @brief Unblock all waiters
   * @param[in,out] scope the mutex scope that protects this conditional variable
   * @pre scope->is_locked_by_me() (the caller must have locked it)
   * @pre scope->get_mutex() == &mutex_ (the scope should be protecting this variable)
   * @post scope is no longer locked
   * @details
   * You should set the real condition variable itself after locking the mutex before
   * calling this method to avoid lost signals.
   */
  void broadcast(SharedMutexScope* scope);

  /**
   * @brief Unblock one waiter
   * @param[in,out] scope the mutex scope that protects this conditional variable
   * @pre scope->is_locked_by_me() (the caller must have locked it)
   * @pre scope->get_mutex() == &mutex_ (the scope should be protecting this variable)
   * @post scope is no longer locked
   * @details
   * You should set the real condition variable itself after locking the mutex before
   * calling this method to avoid lost signals.
   */
  void signal(SharedMutexScope* scope);

  /**
   * @brief Returns the mutex that protects this condition variable.
   * @details
   * You must lock this mutex \b BEFORE you call wait/notify/etc in this class along with
   * checking the real boolean condition itself. Otherwise, you will get lost signals.
   * This is why the methods above receive SharedMutexScope as parameter.
   */
  SharedMutex* get_mutex() { return &mutex_; }

 private:
  /** Whether this mutex is ready for use. We don't tolerate race in initialization. */
  bool                initialized_;

  SharedMutex         mutex_;
  pthread_cond_t      cond_;
  pthread_condattr_t  attr_;
  /** Number of waitors. */
  volatile uint32_t   waiters_;
  /** Number of notifiers, used to safely destruct this object. */
  volatile uint32_t   notifiers_;

  void common_assert(SharedMutexScope* scope);
};

}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SHARED_COND_HPP_
