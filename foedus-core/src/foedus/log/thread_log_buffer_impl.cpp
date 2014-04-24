/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/log/thread_log_buffer_impl.hpp>
namespace foedus {
namespace log {
ThreadLogBuffer::ThreadLogBuffer(Engine* engine, thread::ThreadId thread_id)
    : engine_(engine), thread_id_(thread_id) {
    buffer_ = nullptr;
    buffer_size_ = 0;
}
ErrorStack ThreadLogBuffer::initialize_once() {
    return RET_OK;
}

ErrorStack ThreadLogBuffer::uninitialize_once() {
    return RET_OK;
}

}  // namespace log
}  // namespace foedus
