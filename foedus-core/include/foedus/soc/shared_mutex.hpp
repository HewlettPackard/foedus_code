/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SOC_SHARED_MUTEX_HPP_
#define FOEDUS_SOC_SHARED_MUTEX_HPP_

#include <pthread.h>
#include <stdint.h>

#include "foedus/cxx11.hpp"

namespace foedus {
namespace soc {
/**
 * @brief A mutex that can be placed in shared memory and used from multiple processes.
 * @ingroup SOC
 * @details
 * C++11's mutex doesn't work for multi-process.
 * We need to directly manipulate pthread_mutexattr_setpshared, hence this class.
 * This object is also shared-memory friendly, meaning it has no heap-allocated member.
 * It can be reset to a usable state via memset(zero), too.
 *
 * Example usage:
 * @code{.cpp}
 * SharedMutex mtx;
 * mtx.initialize();  // omit this if you invoked constructor. be careful on reinterpret_cast
 * mtx.lock();
 * do_something();
 * mtx.unlock();  // or automatically released when mtx gets out of scope
 * mtx.uninitialize();  // same above, but not the end of the world
 * @endcode
 */
class SharedMutex CXX11_FINAL {
 public:
  SharedMutex() : initialized_(false), recursive_(false) { initialize(); }
  ~SharedMutex() { uninitialize(); }

  // Disable copy constructors
  SharedMutex(const SharedMutex&) CXX11_FUNC_DELETE;
  SharedMutex& operator=(const SharedMutex&) CXX11_FUNC_DELETE;

  void initialize(bool recursive = false);
  void uninitialize();
  bool is_initialized() const { return initialized_; }
  bool is_recursive() const   { return recursive_; }

  /** Unconditionally lock */
  void lock();

  /**
   * Try lock up to the given timeout
   * @param[in] timeout_nanosec timeout in nanoseconds
   * @return whether this thread acquired the lock
   */
  bool timedlock(uint64_t timeout_nanosec);

  /**
   * Instantaneously try the lock.
   * @return whether this thread acquired the lock
   */
  bool trylock();

  /** Unlock it */
  void unlock();

  pthread_mutex_t*  get_raw_mutex() { return &mutex_; }

 private:
  /** Whether this mutex is ready for use. We don't tolerate race in initialization. */
  bool                initialized_;
  /** Whether this mutex is a recursive (re-lockable) mutex */
  bool                recursive_;

  pthread_mutex_t     mutex_;
  pthread_mutexattr_t attr_;
};

/**
 * @brief Auto-lock scope object for SharedMutex.
 * @ingroup SOC
 * @details
 * SharedMutex itself has auto-release feature, but only when it is on stack.
 * In many cases SharedMutex is placed in shared memory, so its destructor is never called.
 * Instead, this object provides the auto-release semantics.
 */
class SharedMutexScope CXX11_FINAL {
 public:
  SharedMutexScope(SharedMutex* mutex, bool lock_initially = true)
    : mutex_(mutex), locked_by_me_(false) {
    if (lock_initially) {
      lock();
    }
  }
  ~SharedMutexScope() { unlock(); }

  // Disable copy constructors
  SharedMutexScope(const SharedMutexScope&) CXX11_FUNC_DELETE;
  SharedMutexScope& operator=(const SharedMutexScope&) CXX11_FUNC_DELETE;

  bool is_locked_by_me() const { return locked_by_me_; }
  SharedMutex* get_mutex() const { return mutex_; }

  void lock();
  void unlock();

 private:
  SharedMutex* const  mutex_;
  bool                locked_by_me_;
};

}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SHARED_MUTEX_HPP_
