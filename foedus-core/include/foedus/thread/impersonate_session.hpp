/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_IMPERSONATE_SESSION_HPP_
#define FOEDUS_THREAD_IMPERSONATE_SESSION_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/thread/fwd.hpp>
#include <iosfwd>
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
 * @par Moveable/Copiable
 * This object is copiable. If C++11 is enabled, it is also moveable.
 */
struct ImpersonateSession CXX11_FINAL {
    /** Result of wait_for() */
    enum Status {
        /** If called for an invalid session. */
        INVALID_SESSION = 0,
        /** The session has completed. */
        READY,
        /** Timeout duration has elapsed. */
        TIMEOUT,
    };

    ImpersonateSession();
    explicit ImpersonateSession(ImpersonateTask* task);
    ~ImpersonateSession();
    ImpersonateSession(const ImpersonateSession& other);
    ImpersonateSession& operator=(const ImpersonateSession& other);

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
    /** Move constructor is provided only when C++11 is supported. */
    ImpersonateSession(ImpersonateSession &&other);

    /** Move assignment is provided only when C++11 is supported. */
    ImpersonateSession& operator=(ImpersonateSession &&other);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
    /**
     * Returns if the impersonation succeeded.
     */
    bool        is_valid() const { return thread_ != CXX11_NULLPTR; }

    /**
     * @brief Waits until the completion of the asynchronous session and retrieves the result.
     * @details
     * It effectively calls wait() in order to wait for the result.
     * The behavior is undefined if is_valid()== false.
     * This is analogous to std::future::get().
     * @pre is_valid()==true
     */
    ErrorStack  get_result();

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
     * @brief Waits for the completion of the asynchronous session, blocking until specified timeout
     * duration has elapsed or the session completes, whichever comes first.
     * @param[in] timeout timeout duration in microseconds.
     * @details
     * This is analogous to std::future::wait_for() although we don't provide std::chrono.
     */
    Status      wait_for(TimeoutMicrosec timeout) const;

    friend std::ostream& operator<<(std::ostream& o, const ImpersonateSession& v);

    /** The impersonated thread. If impersonation failed, NULL. */
    Thread*             thread_;

    /** The impersonated task running on this session. */
    ImpersonateTask*    task_;

    /** If impersonation failed, this indicates why it failed. */
    ErrorStack          invalid_cause_;

    /**
     * @brief Expected result of the impersonated task.
     * @details
     * Actually std::shared_future<ErrorStack>, which we'd like to hide from public headers.
     * However, pimpl idiom requires new/delete which is too expensive for such a small object.
     * So, we use so-called aligned-storage. std::aligned_storage is also C++11, so we do it
     * ourselves. We do sanity-check of the size (16) in cpp by static_assert.
     * @see http://stackoverflow.com/questions/4921932/pimpl-idiom-without-using-dynamic-memory-allocation
     */
    char                result_future_[16];
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_IMPERSONATE_SESSION_HPP_