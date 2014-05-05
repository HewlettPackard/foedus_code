/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ASSORTED_ATOMIC_FENCES_HPP_
#define FOEDUS_ASSORTED_ATOMIC_FENCES_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/compiler.hpp>
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
#include <atomic>
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
namespace foedus {
namespace assorted {

/**
 * cpp implementation in case the caller disables c++11 and also the architecture is non-TSO.
 * This causes one function call overhead, so avoid using these if possible.
 */
void memory_fence_acquire_impl();
void memory_fence_release_impl();
void memory_fence_acq_rel_impl();
void memory_fence_consume_impl();
void memory_fence_seq_cst_impl();
// "_relaxed" ommitted.

/**
 * @brief Prohibits read/write reordering by compiler, which is sufficient for most (except seq_cst)
 * memory fence semantics on TSO architecture (eg x86).
 * @ingroup ASSORTED
 */
inline void prohibit_compiler_reorder() {
#ifdef __x86_64
    asm volatile("" ::: "memory");  // basically no-op
#else  // __x86_64
    // TODO(Hideaki) ARMv8? anyway this is not enough on non-TSO..
#endif  // __x86_64
}

/**
 * @brief Equivalent to std::atomic_thread_fence(std::memory_order_acquire).
 * @ingroup ASSORTED
 * @details
 * A load operation with this memory order performs the acquire operation on the affected memory
 * location: prior writes made to other memory locations by the thread that did the release become
 * visible in this thread.
 * These \e define memory fences for public headers that need them for inline methods.
 * cpp and private headers can anyway invoke std::atomic_thread_fence.
 */
inline void memory_fence_acquire() {
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
    std::atomic_thread_fence(std::memory_order_acquire);  // basically no-op in TSO
#else  // DISABLE_CXX11_IN_PUBLIC_HEADERS
#ifdef __x86_64
    prohibit_compiler_reorder();
#else  // __x86_64
    memory_fence_acquire_impl();
#endif  // __x86_64
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
}

/**
 * @brief Equivalent to std::atomic_thread_fence(std::memory_order_release).
 * @ingroup ASSORTED
 * @details
 * A store operation with this memory order performs the release operation: prior writes to other
 * memory locations become visible to the threads that do a consume or an acquire on the same
 * location.
 */
inline void memory_fence_release() {
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
    std::atomic_thread_fence(std::memory_order_release);  // basically no-op in TSO
#else  // DISABLE_CXX11_IN_PUBLIC_HEADERS
#ifdef __x86_64
    prohibit_compiler_reorder();
#else  // __x86_64
    memory_fence_release_impl();
#endif  // __x86_64
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
}

/**
 * @brief Equivalent to std::atomic_thread_fence(std::memory_order_acq_rel).
 * @ingroup ASSORTED
 * @details
 * A load operation with this memory order performs the acquire operation on the affected memory
 * location and a store operation with this memory order performs the release operation.
 */
inline void memory_fence_acq_rel() {
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
    std::atomic_thread_fence(std::memory_order_acq_rel);  // basically no-op in TSO
#else  // DISABLE_CXX11_IN_PUBLIC_HEADERS
#ifdef __x86_64
    prohibit_compiler_reorder();
#else  // __x86_64
    memory_fence_acq_rel_impl();
#endif  // __x86_64
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
}

/**
 * @brief Equivalent to std::atomic_thread_fence(std::memory_order_consume).
 * @ingroup ASSORTED
 * @details
 * A load operation with this memory order performs a consume operation on the affected memory
 * location: prior writes to data-dependent memory locations made by the thread that did a release
 * operation become visible to this thread.
 */
inline void memory_fence_consume() {
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
    std::atomic_thread_fence(std::memory_order_consume);  // basically no-op in TSO
#else  // DISABLE_CXX11_IN_PUBLIC_HEADERS
#ifdef __x86_64
    prohibit_compiler_reorder();
#else  // __x86_64
    memory_fence_consume_impl();
#endif  // __x86_64
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
}

/**
 * @brief Equivalent to std::atomic_thread_fence(std::memory_order_seq_cst).
 * @ingroup ASSORTED
 * @details
 * Same as memory_order_acq_rel, plus a single total order exists in which all threads observe all
 * modifications in the same order.
 */
inline void memory_fence_seq_cst() {
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
    std::atomic_thread_fence(std::memory_order_seq_cst);  // compiled into mfence etc
#else  // DISABLE_CXX11_IN_PUBLIC_HEADERS
    // This one has to be really a mfence call even in x86. It's anyway expensive,
    // so a function call overhead doesn't matter.
    memory_fence_seq_cst_impl();
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
}

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_ATOMIC_FENCES_HPP_
