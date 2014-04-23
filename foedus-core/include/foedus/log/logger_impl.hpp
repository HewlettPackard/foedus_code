/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOGGER_IMPL_HPP_
#define FOEDUS_LOG_LOGGER_IMPL_HPP_
#include <foedus/engine.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/log_id.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/thread/thread_id.hpp>
#include <stdint.h>
#include <vector>
namespace foedus {
namespace log {
/**
 * @brief A log writer.
 * @ingroup LOG
 * @details
 * This is a private implementation-details of \ref LOG, thus file name ends with _impl.
 * Do not include this header from a client program unless you know what you are doing.
 */
class Logger final : public DefaultInitializable {
 public:
    Logger(Engine* engine, LoggerId id, const fs::Path &log_path,
           std::vector< thread::ThreadId > assigned_threads);
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    Logger(const Logger &other) = delete;
    Logger& operator=(const Logger &other) = delete;


 private:
    Engine*                         engine_;
    LoggerId                        id_;
    const fs::Path                  log_path_;
    std::vector< thread::ThreadId > assigned_threads_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOGGER_IMPL_HPP_
