/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOGGER_IMPL_HPP_
#define FOEDUS_LOG_LOGGER_IMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/log_id.hpp>
#include <foedus/fs/fwd.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/thread/fwd.hpp>
#include <stdint.h>
#include <condition_variable>
#include <mutex>
#include <thread>
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
    Logger(Engine* engine, LoggerId id, const fs::Path &log_path,
           const std::vector< thread::ThreadId > &assigned_thread_ids);
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    Logger() = delete;
    Logger(const Logger &other) = delete;
    Logger& operator=(const Logger &other) = delete;

 private:
    /**
     * @brief Main routine for logger_thread_.
     * @details
     * This method keeps writing out logs in assigned threads' private buffers.
     * When there are no logs in all the private buffers for a while, it goes into sleep.
     * This method exits when this object's uninitialize() is called.
     */
    void        handle_logger();

    /**
     * Called from handle_logger when there is no log to process.
     */
    void        sleep_logger();

    fs::Path    construct_suffixed_log_path(LogFileOrdinal ordinal) const;

    Engine*                         engine_;
    LoggerId                        id_;
    thread::ThreadGroupId           numa_node_;
    const fs::Path                  log_path_;
    std::vector< thread::ThreadId > assigned_thread_ids_;

    std::mutex                      logger_mutex_;
    std::condition_variable         logger_stop_condition_;
    std::thread                     logger_thread_;
    bool                            logger_stop_requested_;
    bool                            logger_stopped_;

    memory::NumaNodeMemory*         node_memory_;
    char*                           logger_buffer_;
    uint64_t                        logger_buffer_size_;

    /**
     * @brief Ordinal of the oldest active log file of this logger.
     * @invariant oldest_ordinal_ <= current_ordinal_
     */
    LogFileOrdinal                  oldest_ordinal_;
    /**
     * @brief Inclusive beginning of active region in the oldest log file.
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
     * log_path_ + current_ordinal_.
     */
    fs::Path                        current_file_path_;
    /**
     * @brief Exclusive end of the current log file, or the size of the current file.
     */
    uint64_t                        current_file_offset_end_;

    std::vector< thread::ThreadPimpl* >  assigned_threads_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOGGER_IMPL_HPP_
