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

    void assert_consistent_offsets() const;

 private:
    Engine*                         engine_;
    thread::ThreadId                thread_id_;

    /**
     * @brief The in-memory log buffer given to this thread.
     * @details
     * This forms a circular buffer to which \e this thread (the owner of this buffer)
     * will append log entries, and from which log writer will read from head.
     * This is a piece of NumaNodeMemory#thread_buffer_memory_.
     */
    char*                           buffer_;

    /** Size of the buffer assigned to this thread. */
    uint64_t                        buffer_size_;

    /**
     * @brief buffer_size_ - 64.
     * @details
     * We always leave some \e hole between offset_tail_ and offset_head_
     * to avoid the case offset_tail_ == offset_head_ (log empty? or log full?).
     * One classic way to handle this case is to store \e count rather than offsets, but
     * it makes synchronization between log writer and this thread expensive.
     * Rather, we sacrifice a negligible space.
     */
    uint64_t                        buffer_size_safe_;

    xct::Epoch                      durable_epoch_;
    xct::Epoch                      current_epoch_;

    // values of these offsets must be in this order except wrap around.

    /**
     * @brief This marks the position upto which the log writer durably wrote out to log files.
     * @details
     */
    uint64_t                        offset_head_;

    /**
     * @brief This marks the position upto which the log writer durably wrote out to log files.
     * @details
     * Everything after this position must not be discarded because they are not yet durable.
     * When the log writer reads log entries after here, writes them to log file, and calls fsync,
     * this variable is advanced by the log writer.
     * This variable is read by this thread to check the end of the circular buffer.
     */
    uint64_t                        offset_durable_;

    /** Do we need this?? */
    uint64_t                        offset_current_epoch_;

    /**
     * @brief The beginning of logs for current transaction.
     * @details
     * Log writers can safely read log entries and write them to log files up to this place.
     * When the transaction commits, this value is advanced by the thread.
     * The only possible update pattern to this variable is \b advance by this thread.
     * Thus, the log writer can safely read this variable without any fence or lock
     * thanks to regular (either old value or new value, never garbage) read of 64-bit.
     */
    uint64_t                        offset_current_xct_begin_;

    /**
     * @brief The current cursor to which next log will be written.
     * @details
     * This is the location the current transaction of this thread is writing to \b before commit.
     * When the transaction commits, offset_current_xct_begin_ catches up with this.
     * When the transaction aborts, this value rolls back to offset_current_xct_begin_.
     * Only this thread reads/writes to this variable. No other threads access this.
     */
    uint64_t                        offset_tail_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_THREAD_LOG_BUFFER_IMPL_HPP_
