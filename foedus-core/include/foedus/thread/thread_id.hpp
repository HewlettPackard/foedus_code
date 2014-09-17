/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_ID_HPP_
#define FOEDUS_THREAD_THREAD_ID_HPP_
#include <stdint.h>

#include "foedus/cxx11.hpp"

/**
 * @file foedus/thread/thread_id.hpp
 * @brief Typedefs of ID types used in thread package.
 * @ingroup THREAD
 */
namespace foedus {
namespace thread {
/**
 * @typedef ThreadGroupId
 * @brief Typedef for an ID of ThreadGroup (NUMA node).
 * @ingroup THREAD
 * @details
 * Currently, we assume there are at most 256 NUMA nodes.
 */
typedef uint8_t ThreadGroupId;

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
CXX11_STATIC_ASSERT (sizeof(ThreadGroupId) == 1, "Max NUMA node count must be 1 byte");
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

/**
 * @var kMaxThreadGroupId
 * @brief Maximum possible value of ThreadGroupId.
 * @ingroup THREAD
 */
const ThreadGroupId kMaxThreadGroupId = 0xFF;

/**
 * @typedef ThreadLocalOrdinal
 * @brief Typedef for a \b local ID of Thread (core), which is \b NOT unique across NUMA nodes.
 * @ingroup THREAD
 * @details
 * Currently, we assume there are at most 256 cores per NUMA node.
 */
typedef uint8_t ThreadLocalOrdinal;

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
CXX11_STATIC_ASSERT (sizeof(ThreadLocalOrdinal) == 1, "Max core-per-node must be 1 byte");
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

/**
 * @var kMaxThreadLocalOrdinal
 * @brief Maximum possible value of ThreadLocalOrdinal.
 * @ingroup THREAD
 */
const ThreadLocalOrdinal kMaxThreadLocalOrdinal = 0xFF;

/**
 * @typedef ThreadId
 * @brief Typedef for a \b global ID of Thread (core), which is unique across NUMA nodes.
 * @ingroup THREAD
 * @details
 * This is a composite of ThreadGroupId (high 1 byte) and ThreadLocalOrdinal (low 1 byte).
 * For example, if there are 2 NUMA nodes and 8 cores each:
 * thread-0=node-0 core-0, thread-1=node-0 core-1, ..., thread-256=node-1 core-0,...
 */
typedef uint16_t ThreadId;

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
CXX11_STATIC_ASSERT (sizeof(ThreadId) == 2, "Max thread count must be 2 bytes");
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

/**
 * @typedef ThreadGlobalOrdinal
 * @brief Typedef for a globally and contiguously numbered ID of thread.
 * @ingroup THREAD
 * @details
 * ThreadId is not contiguous because its high byte is NUMA node, and that is what we use
 * in most places. However, sometimes we need a contiguously numberd ID of thread, such
 * as when we allocate an array of objects, one for each thread.
 * This number is used in such cases.
 * Be extra careful, both ThreadId and this are uint16_t, so compiler won't warn anything
 * if we use them incorrectly.
 */
typedef uint16_t ThreadGlobalOrdinal;

/**
 * @var kMaxThreadId
 * @brief Maximum possible value of ThreadId.
 * @ingroup THREAD
 */
const ThreadId kMaxThreadId = 0xFFFF;

/**
 * @typedef ThreadTicket
 * @brief Typedef for a monotonically increasing ticket for thread impersonation.
 * @ingroup THREAD
 * @details
 * For every impersonation, the thread increments this value in its control block.
 * Each session receives the ticket and checks the ticket whenever it checks the status
 * of the thread. This is required to avoid double-free and modifying input/output data
 * of other sessions.
 */
typedef uint64_t ThreadTicket;

/**
 * Returns a globally unique ID of Thread (core) for the given node and ordinal in the node.
 * @ingroup THREAD
 */
inline ThreadId compose_thread_id(ThreadGroupId node, ThreadLocalOrdinal local_core) {
  return (node << 8) | local_core;
}

/**
 * Extracts NUMA node ID from the given globally unique ID of Thread (core).
 * @ingroup THREAD
 */
inline ThreadGroupId decompose_numa_node(ThreadId global_id) {
  return (global_id >> 8) & 0xFF;
}

/**
 * Extracts local ordinal from the given globally unique ID of Thread (core).
 * @ingroup THREAD
 */
inline ThreadLocalOrdinal decompose_numa_local_ordinal(ThreadId global_id) {
  return global_id & 0xFF;
}

/**
 * Calculate ThreadGlobalOrdinal from ThreadId.
 * @ingroup THREAD
 */
inline ThreadGlobalOrdinal to_global_ordinal(ThreadId thread_id, uint8_t threads_per_nodes) {
  return decompose_numa_node(thread_id) * threads_per_nodes
    + decompose_numa_local_ordinal(thread_id);
}

/**
 * @brief Used as a general timeout parameter (in microseconds) for synchronous methods.
 * @details
 * If the method had to wait for this length, it gives up and returns a failure.
 * Negative value means forever. 0 means no wait, in other words it's \e conditional (we
 * execute the function on the condition of immediate availability).
 */
typedef int64_t TimeoutMicrosec;

/**
 * Thread policy for worker threads. The values are compatible with pthread's values.
 * @ingroup THREAD
 * @see http://man7.org/linux/man-pages/man3/pthread_getschedparam.3.html
 */
enum ThreadPolicy {
  /** SCHED_OTHER */
  kScheduleOther = 0,
  /** SCHED_FIFO */
  kScheduleFifo = 1,
  /** SCHED_RR */
  kScheduleRr = 2,
  /** SCHED_BATCH */
  kScheduleBatch = 3,
  /** SCHED_IDLE */
  kScheduleIdle = 5,
  // no SCHED_DEADLINE. Too new (linux 3.14~).
};
/**
 * Thread priority for worker threads. The values are compatible with pthread's values.
 * Depending on policy, the lowest-highest might be overwritten by
 * what sched_get_priority_max()/min returns.
 * @ingroup THREAD
 * @see http://man7.org/linux/man-pages/man3/pthread_getschedparam.3.html
 */
enum ThreadPriority {
  kPriorityIdle = 0,
  kPriorityLowest = 1,
  kPriorityDefault = 50,
  kPriorityHighest = 99,
};

/**
 * @brief Impersonation status of each worker thread.
 * @ingroup THREAD
 * @details
 * The transition is basically only to next one.
 * Exceptions are:
 *  \li Every state might jump to kTerminated for whatever reason.
 *  \li kWaitingForClientRelease goes back to kWaitingForTask when the client picks the result up
 * and closes the session.
 */
enum ThreadStatus {
  /** Initial state. The thread does nothing in this state */
  kNotInitialized = 0,
  /** Idle state, receiving a new task. */
  kWaitingForTask,
  /** A client has set a next task. The thread has not picked it up yet. */
  kWaitingForExecution,
  /** The thread has picked the task up and is now running. */
  kRunningTask,
  /** The thread has completed the task and set the result. The client has not picked it up yet. */
  kWaitingForClientRelease,
  /** The thread is requested to terminate */
  kWaitingForTerminate,
  /** The thread has terminated (either error or normal, check the result to differentiate them). */
  kTerminated,
};

}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_ID_HPP_
