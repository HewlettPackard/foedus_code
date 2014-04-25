/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/engine.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <cassert>
namespace foedus {
namespace log {
ThreadLogBuffer::ThreadLogBuffer(Engine* engine, thread::ThreadId thread_id)
    : engine_(engine), thread_id_(thread_id) {
    buffer_ = nullptr;
    buffer_size_ = 0;
}
ErrorStack ThreadLogBuffer::initialize_once() {
    memory::NumaCoreMemory *memory = engine_->get_memory_manager().get_core_memory(thread_id_);
    buffer_ = memory->get_log_buffer_memory();
    buffer_size_ = memory->get_log_buffer_size();
    buffer_size_safe_ = buffer_size_ - 64;
    return RET_OK;
}

ErrorStack ThreadLogBuffer::uninitialize_once() {
    buffer_ = nullptr;
    return RET_OK;
}

void ThreadLogBuffer::assert_consistent_offsets() const {
    assert(offset_head_ < buffer_size_
        && offset_durable_ < buffer_size_
        && offset_current_xct_begin_ < buffer_size_
        && offset_tail_ < buffer_size_);
    // because of wrap around, *at most* one of them does not hold
    int violation_count = 0;
    if (offset_head_ > offset_durable_) {
        ++violation_count;
    }
    if (offset_durable_ > offset_current_xct_begin_) {
        ++violation_count;
    }
    if (offset_current_xct_begin_ > offset_tail_) {
        ++violation_count;
    }
    assert(violation_count <= 1);
}

}  // namespace log
}  // namespace foedus
