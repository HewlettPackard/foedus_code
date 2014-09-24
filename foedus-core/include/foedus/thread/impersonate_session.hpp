/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_IMPERSONATE_SESSION_HPP_
#define FOEDUS_THREAD_IMPERSONATE_SESSION_HPP_
#include <iosfwd>

#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace thread {
/**
 * @brief A user session running on an \e impersonated thread.
 * @ingroup THREADPOOL
 * @details
 * @par Overview
 * This object represents an impersonated session, which is obtained by calling
 * ThreadPool#impersonate() and running the given task on a pre-allocated thread.
 * This object also works as a \e future to synchronously or asynchronously
 * wait for the completion of the functor from the client program's thread.
 *
 * @par Result of Impersonation
 * The client program has to first check is_valid() for the given session because
 * there are two possible cases as the result of impersonation.
 *  \li Impersonation succeded (is_valid()==true), running the task on the impersonated thread.
 * In this case, the client's thread (original thread that invoked ThreadPool#impersonate()) can
 * either wait for the end of the impersonated thread (call wait() or wait_for()),
 * or do other things (e.g., launch more impersonated threads).
 *
 *  \li Impersonation failed (is_valid()==false). This might happen for various reasons; timeout,
 * the engine shuts down while waiting, unexpected errors, etc.
 * In this case, get_invalid_cause() indicates the causes of the failed impersonation.
 *
 * @par Wait for the completion of the session
 * When is_valid()==true, this object also behaves as std::shared_future<ErrorStack>,
 * and we actually use it. But, because of the \ref CXX11 issue, we wrap it as a usual class.
 * This object is copiable like std::shared_future, not std::future.
 * Actually, this class is based on std::shared_future just to provide copy semantics
 * for non-C++11 clients. Additional overheads shouldn't matter, hopeully.
 *
 * @par Copy/Move
 * This object is not copy-able because the destructor closes the session in shared memory.
 * However, it is moveable, which moves the ownership (available only with C++11 though).
 */
struct ImpersonateSession CXX11_FINAL {
  ImpersonateSession() : thread_(CXX11_NULLPTR), ticket_(0) {}
  ~ImpersonateSession() { release(); }

  // Not copy-able
  ImpersonateSession(const ImpersonateSession& other) CXX11_FUNC_DELETE;
  ImpersonateSession& operator=(const ImpersonateSession& other) CXX11_FUNC_DELETE;

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  // but move-able (only in C++11)
  ImpersonateSession(ImpersonateSession&& other);
  ImpersonateSession& operator=(ImpersonateSession&& other);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

  /**
   * Returns if the impersonation succeeded.
   */
  bool        is_valid() const { return thread_ != CXX11_NULLPTR && ticket_ != 0; }
  /** Returns if the task is still running*/
  bool        is_running() const;

  /**
   * @brief Waits until the completion of the asynchronous session and retrieves the result.
   * @details
   * It effectively calls wait() in order to wait for the result.
   * The behavior is undefined if is_valid()== false.
   * @pre is_valid()==true
   */
  ErrorStack  get_result();
  /** Returns the byte size of output */
  uint64_t    get_output_size();
  /**
   * Copies the output to the given buffer, whose size must be at least get_output_size().
   */
  void        get_output(void* output_buffer);
  /** Returns the pointer to the raw output buffer on shared memory. */
  const void* get_raw_output_buffer();

  /**
   * @brief Blocks until the completion of the asynchronous session.
   * @details
   * The behavior is undefined if is_valid()== false.
   * It effectively calls wait_for(-1) in order to unconditionally wait for the result.
   * This is analogous to std::future::wait().
   * @pre is_valid()==true
   */
  void        wait() const;

  /**
   * @brief Releases all resources and ownerships this session has acquired.
   * @details
   * This method is idempotent, you can call it in any state and many times.
   * Actually, this is also called from the destructor.
   * This method might block if the session is still running.
   * We do not allow the user to discard this session without completing the currently running task.
   * @attention Once you invoke this method, you can't call get_result(), get_output(),
   * get_output_size() any longer.
   */
  void        release();

  friend std::ostream& operator<<(std::ostream& o, const ImpersonateSession& v);

  /** The impersonated thread. If impersonation failed, null. */
  thread::ThreadRef*    thread_;

  /** The ticket issued as of impersonation. If impersonation failed, 0. */
  thread::ThreadTicket  ticket_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_IMPERSONATE_SESSION_HPP_
