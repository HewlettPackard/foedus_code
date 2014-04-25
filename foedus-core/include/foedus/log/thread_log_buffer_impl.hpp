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
 *
 * @par Circular Log Buffer
 * This class forms a circular buffer used by log appender (Thread), log writer (Logger),
 * and log gleaner (LogGleaner). We maintain four offsets on the buffer.
 * <table>
 * <tr><th>Marker</th><th>Read by</th><th>Written by</th><th>Description</th></tr>
 * <tr><td> get_offset_head() </td><td>Thread</td><td>Thread, LogGleaner</td>
 *   <td> @copydoc get_offset_head() </td></tr>
 * <tr><td> get_offset_durable() </td><td>Thread, LogGleaner</td><td>Logger</td>
 *   <td> @copydoc get_offset_durable() </td></tr>
 * <tr><td> get_offset_current_xct_begin() </td><td>Logger</td><td>Thread</td>
 *   <td> @copydoc get_offset_current_xct_begin() </td></tr>
 * <tr><td> get_offset_tail() </td><td>Thread</td><td>Thread</td>
 *   <td> @copydoc get_offset_tail() </td></tr>
 * </table>
 */
class ThreadLogBuffer final : public DefaultInitializable {
 public:
    static uint64_t distance(uint64_t buffer_size, uint64_t from, uint64_t to) {
        assert(from < buffer_size);
        assert(to < buffer_size);
        if (to >= from) {
            return from - to;
        } else {
            return from + buffer_size - to;
        }
    }
    static void advance(uint64_t buffer_size, uint64_t *target, uint64_t advance) {
        assert(*target < buffer_size);
        assert(advance < buffer_size);
        *target += advance;
        if (*target >= buffer_size) {
            *target -= buffer_size;
        }
    }

    ThreadLogBuffer(Engine* engine, thread::ThreadId thread_id);
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    ThreadLogBuffer() = delete;
    ThreadLogBuffer(const ThreadLogBuffer &other) = delete;
    ThreadLogBuffer& operator=(const ThreadLogBuffer &other) = delete;

    void        assert_consistent_offsets() const;

    /**
     * @brief The in-memory log buffer given to this thread.
     * @details
     * This forms a circular buffer to which \e this thread (the owner of this buffer)
     * will append log entries, and from which log writer will read from head.
     * This is a piece of NumaNodeMemory#thread_buffer_memory_.
     */
    char*       get_buffer() { return buffer_; }

    /** Size of the buffer assigned to this thread. */
    uint64_t    get_buffer_size() const { return buffer_size_; }

    /**
     * @brief buffer_size_ - 64.
     * @details
     * We always leave some \e hole between offset_tail_ and offset_head_
     * to avoid the case offset_tail_ == offset_head_ (log empty? or log full?).
     * One classic way to handle this case is to store \e count rather than offsets, but
     * it makes synchronization between log writer and this thread expensive.
     * Rather, we sacrifice a negligible space.
     */
    uint64_t    get_buffer_size_safe() const { return buffer_size_safe_; }

    /**
     * @brief Reserves a space for a new (uncommitted) log entry at the tail.
     * @param[in] log_length byte size of the log. You have to give it beforehand.
     * @details
     * If the circular buffer's tail reaches the head, this method might block.
     * But it will be rare as we release a large region of buffer at each time.
     */
    char*       reserve_new_log(uint16_t log_length) {
        if (distance(buffer_size_, offset_tail_, offset_head_) + log_length >= buffer_size_safe_) {
            wait_for_space(log_length);
        }
        assert(distance(buffer_size_, offset_tail_, offset_head_) + log_length < buffer_size_safe_);
        char *buffer = buffer_ + offset_tail_;
        advance(buffer_size_, &offset_tail_, log_length);
        return buffer;
    }

    void       wait_for_space(uint16_t required_space);

    /**
     * @brief This marks the position where log entries start.
     * @details
     * This private log buffer is a circular buffer where the \e head is eaten by log gleaner.
     * However, log gleaner is okay to get behind, reading from log file instead (but slower).
     * Thus, offset_head_ is advanced either by log gleaner or this thread.
     * If the latter happens, log gleaner has to give up using in-memory logs and instead
     * read from log files.
     */
    uint64_t    get_offset_head() const { return offset_head_; }
    /** @copydoc get_offset_head() */
    void        set_offset_head(uint64_t value) { offset_head_ = value; }

    /**
     * @brief This marks the position upto which the log writer durably wrote out to log files.
     * @details
     * Everything after this position must not be discarded because they are not yet durable.
     * When the log writer reads log entries after here, writes them to log file, and calls fsync,
     * this variable is advanced by the log writer.
     * This variable is read by this thread to check the end of the circular buffer.
     */
    uint64_t    get_offset_durable() const { return offset_durable_; }
    /** @copydoc get_offset_durable() */
    void        set_offset_durable(uint64_t value) { offset_durable_ = value; }

    /**
     * @brief The beginning of logs for current transaction.
     * @details
     * Log writers can safely read log entries and write them to log files up to this place.
     * When the transaction commits, this value is advanced by the thread.
     * The only possible update pattern to this variable is \b advance by this thread.
     * Thus, the log writer can safely read this variable without any fence or lock
     * thanks to regular (either old value or new value, never garbage) read of 64-bit.
     */
    uint64_t    get_offset_current_xct_begin() const { return offset_current_xct_begin_; }
    /** @copydoc get_offset_current_xct_begin() */
    void        set_offset_current_xct_begin(uint64_t value) { offset_current_xct_begin_ = value; }

    /**
     * @brief The current cursor to which next log will be written.
     * @details
     * This is the location the current transaction of this thread is writing to \b before commit.
     * When the transaction commits, offset_current_xct_begin_ catches up with this.
     * When the transaction aborts, this value rolls back to offset_current_xct_begin_.
     * Only this thread reads/writes to this variable. No other threads access this.
     */
    uint64_t    get_offset_tail() const { return offset_tail_; }
    /** @copydoc get_offset_tail() */
    void        set_offset_tail(uint64_t value) { offset_tail_ = value; }

 private:
    Engine*                         engine_;
    thread::ThreadId                thread_id_;

    /** @copydoc get_buffer() */
    char*                           buffer_;
    /** @copydoc get_buffer_size() */
    uint64_t                        buffer_size_;
    /** @copydoc get_buffer_size_safe() */
    uint64_t                        buffer_size_safe_;

    xct::Epoch                      durable_epoch_;
    xct::Epoch                      current_epoch_;

    /** @copydoc get_offset_head() */
    uint64_t                        offset_head_;
    /** @copydoc get_offset_durable() */
    uint64_t                        offset_durable_;
    /** @copydoc get_offset_current_xct_begin() */
    uint64_t                        offset_current_xct_begin_;
    /** @copydoc get_offset_tail() */
    uint64_t                        offset_tail_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_THREAD_LOG_BUFFER_IMPL_HPP_
