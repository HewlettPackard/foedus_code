/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_THREAD_LOG_BUFFER_IMPL_HPP_
#define FOEDUS_LOG_THREAD_LOG_BUFFER_IMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/xct/epoch.hpp>
#include <stdint.h>
namespace foedus {
namespace log {
/**
 * @brief A thread-local log buffer.
 * @ingroup LOG
 * @details
 * This is a private implementation-details of \ref LOG, thus file name ends with _impl.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ThreadLogBuffer final : public DefaultInitializable {
 public:
    ThreadLogBuffer(Engine* engine, thread::ThreadId thread_id);
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    ThreadLogBuffer() = delete;
    ThreadLogBuffer(const ThreadLogBuffer &other) = delete;
    ThreadLogBuffer& operator=(const ThreadLogBuffer &other) = delete;

 private:
    Engine*                         engine_;
    thread::ThreadId                thread_id_;

    /**
     * The in-memory log buffer given to this thread.
     * This is a piece of NumaNodeMemory#thread_buffer_memory_.
     */
    char*                           buffer_;

    /** Size of the buffer assigned to tihs thread. */
    uint64_t                        buffer_size_;

    xct::Epoch                      previous_epoch_;
    xct::Epoch                      current_epoch_;

    uint64_t                        offset_head_;
    uint64_t                        offset_durable_;
    uint64_t                        offset_current_epoch_;
    uint64_t                        offset_tail_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_THREAD_LOG_BUFFER_IMPL_HPP_
