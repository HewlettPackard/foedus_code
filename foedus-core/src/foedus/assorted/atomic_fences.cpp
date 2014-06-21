/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/assorted/atomic_fences.hpp"

#include <atomic>
namespace foedus {
namespace assorted {
void memory_fence_acquire_impl() { std::atomic_thread_fence(std::memory_order_acquire); }
void memory_fence_release_impl() { std::atomic_thread_fence(std::memory_order_release); }
void memory_fence_acq_rel_impl() { std::atomic_thread_fence(std::memory_order_acq_rel); }
void memory_fence_consume_impl() { std::atomic_thread_fence(std::memory_order_consume); }
void memory_fence_seq_cst_impl() { std::atomic_thread_fence(std::memory_order_seq_cst); }
}  // namespace assorted
}  // namespace foedus
