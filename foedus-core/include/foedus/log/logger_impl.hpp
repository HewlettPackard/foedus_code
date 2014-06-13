/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOGGER_IMPL_HPP_
#define FOEDUS_LOG_LOGGER_IMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/fwd.hpp>
#include <foedus/log/epoch_history.hpp>
#include <foedus/log/log_id.hpp>
#include <foedus/fs/fwd.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/memory/aligned_memory.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/thread/stoppable_thread_impl.hpp>
#include <foedus/epoch.hpp>
#include <foedus/savepoint/fwd.hpp>
#include <stdint.h>
#include <iosfwd>
#include <string>
#include <vector>
namespace foedus {
namespace log {
/**
 * @brief A log writer that writes out buffered logs to stable storages.
 * @ingroup LOG
 * @details
 * This is a private implementation-details of \ref LOG, thus file name ends with _impl.
 * Do not include this header from a client program unless you know what you are doing.
 */
class Logger final : public DefaultInitializable {
 public:
    Logger(Engine* engine, LoggerId id, thread::ThreadGroupId numa_node, uint8_t in_node_ordinal,
           const fs::Path &log_folder,
           const std::vector< thread::ThreadId > &assigned_thread_ids) : engine_(engine),
           id_(id), numa_node_(numa_node), in_node_ordinal_(in_node_ordinal),
           log_folder_(log_folder), assigned_thread_ids_(assigned_thread_ids) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    Logger() = delete;
    Logger(const Logger &other) = delete;
    Logger& operator=(const Logger &other) = delete;

    /**
     * @brief Wakes up this logger if it is sleeping.
     */
    void        wakeup();

    /**
     * @brief Wakes up this logger if its durable_epoch has not reached the given epoch yet.
     * @details
     * If this logger's durable_epoch is already same or larger than the epoch, does nothing.
     * This method just wakes up the logger and immediately returns.
     */
    void        wakeup_for_durable_epoch(Epoch desired_durable_epoch);

    /** Returns this logger's durable epoch. */
    Epoch       get_durable_epoch() const { return durable_epoch_; }

    /** Called from log manager's copy_logger_states. */
    void        copy_logger_state(savepoint::Savepoint *new_savepoint);

    /** Append a new epoch history. */
    void        add_epoch_history(const EpochMarkerLogType& epoch_marker);

    std::string             to_string() const;
    friend std::ostream&    operator<<(std::ostream& o, const Logger& v);

 private:
    /**
     * @brief Main routine for logger_thread_.
     * @details
     * This method keeps writing out logs in assigned threads' private buffers.
     * When there are no logs in all the private buffers for a while, it goes into sleep.
     * This method exits when this object's uninitialize() is called.
     */
    void        handle_logger();
    /** handle_logger() keeps calling this with sleeps. */
    ErrorStack  handle_logger_once(bool *more_log_to_process);

    /**
     * Check if we can advance the durable epoch of this logger, invoking fsync BEFORE actually
     * advancing it in that case.
     */
    ErrorStack  update_durable_epoch();
    /**
     * Subroutine of update_durable_epoch to get the minimum durable epoch of assigned loggers.
     * This function is assured to be no-error and instantaneous.
     */
    Epoch calculate_min_durable_epoch();

    /**
     * Moves on to next file if the current file exceeds the configured max size.
     */
    ErrorStack  switch_file_if_required();

    /**
     * Adds a log entry to annotate the switch of epoch.
     * Individual log entries do not have epoch information, relying on this.
     */
    ErrorStack  log_epoch_switch(Epoch new_epoch);

    /**
     * Whenever we restart or switch to a new file, we call this method to write out an epoch marker
     * so that all log files start with an epoch marker.
     */
    ErrorStack  write_dummy_epoch_mark();

    /**
     * Writes out the given buffer upto the offset.
     * This method handles non-aligned starting/ending offsets by padding, and also handles wrap
     * around.
     */
    ErrorStack  write_log(ThreadLogBuffer* buffer, uint64_t upto_offset);

    fs::Path    construct_suffixed_log_path(LogFileOrdinal ordinal) const;

    /** Check invariants. This method should be wiped in NDEBUG. */
    void        assert_consistent();

    Engine* const                   engine_;
    const LoggerId                  id_;
    const thread::ThreadGroupId     numa_node_;
    const uint8_t                   in_node_ordinal_;
    const fs::Path                  log_folder_;
    const std::vector< thread::ThreadId > assigned_thread_ids_;

    thread::StoppableThread         logger_thread_;

    /**
     * @brief A local and very small aligned buffer to pad log entries to 4kb.
     * @details
     * The logger directly reads from the assigned threads' own buffer when it writes out to file
     * because we want to avoid doubling the overhead. As the buffer is exclusively assigned to
     * this logger, there is no risk to directly pass the thread's buffer \e except the case
     * where we are writing out less than 4kb, which happens on:
     *  \li at the beginning/end of an epoch log block
     *  \li when the logger is really catching up well (the thread has less than 4kb
     * commited-but-non-durable log)
     * In these cases, we need to pad it to 4kb. So, we copy the thread's buffer's content to this
     * buffer and fill the rest (at the end or at the beginning, or both).
     */
    memory::AlignedMemory           fill_buffer_;

    /**
     * @brief Upto what epoch the logger flushed logs in \b all buffers assigned to it.
     * @invariant durable_epoch_.is_valid()
     * @invariant global durable epoch <= durable_epoch_ < global current epoch
     * @details
     * Unlike buffer.logger_epoch, this value is continuously maintained by the logger, thus
     * no case of stale values. Actually, the global durable epoch does not advance until all
     * loggers' durable_epoch_ advance.
     * Hence, if some thread is idle or running a long transaction, this value could be larger
     * than buffer.logger_epoch_. Otherwise (when the worker thread is running normal), this value
     * is most likely smaller than buffer.logger_epoch_.
     */
    Epoch                           durable_epoch_;

    /**
     * @brief Upto what epoch the logger has put epoch marker in the log file.
     * @invariant marked_epoch_.is_valid()
     * @invariant marked_epoch_ <= durable_epoch_.one_more()
     * @details
     * Usually, this value is always same as durable_epoch_.one_more().
     * This value becomes smaller than that if the logger had no log to write out
     * when it advanced durable_epoch_. In that case, writing out an epoch marker is a waste
     * (eg when the system is idle for long time, there will be tons of empty epochs),
     * so we do not write out the epoch marker and let this value remain same.
     * When the logger later writes out a log, it checks this value and writes out an epoch mark.
     * @see no_log_epoch_
     */
    Epoch                           marked_epoch_;

    /** Whether so far this logger has not written out any log since previous epoch switch. */
    bool                            no_log_epoch_;

    /**
     * @brief Ordinal of the oldest active log file of this logger.
     * @invariant oldest_ordinal_ <= current_ordinal_
     */
    LogFileOrdinal                  oldest_ordinal_;
    /**
     * @brief Inclusive beginning of active region in the oldest log file.
     * @invariant oldest_file_offset_begin_ % kLogWriteUnitSize == 0 (because we pad)
     */
    uint64_t                        oldest_file_offset_begin_;
    /**
     * @brief Ordinal of the log file this logger is currently appending to.
     */
    LogFileOrdinal                  current_ordinal_;
    /**
     * @brief The log file this logger is currently appending to.
     */
    fs::DirectIoFile*               current_file_;
    /**
     * [log_folder_]/[id_]_[current_ordinal_].log.
     */
    fs::Path                        current_file_path_;

    /**
     * We called fsync on current file up to this offset.
     * @invariant current_file_durable_offset_ <= current_file->get_current_offset()
     * @invariant current_file_durable_offset_ % kLogWriteUnitSize == 0 (because we pad)
     */
    uint64_t                        current_file_durable_offset_;

    std::vector< thread::Thread* >  assigned_threads_;

    /**
     * Remembers all epoch switching in this logger.
     */
    std::vector< EpochHistory >     epoch_histories_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOGGER_IMPL_HPP_
