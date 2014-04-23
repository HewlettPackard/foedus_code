/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/log/logger_impl.hpp>
#include <vector>
namespace foedus {
namespace log {

Logger::Logger(Engine* engine, LoggerId id,
               const fs::Path &log_path, std::vector< thread::ThreadId > assigned_threads)
    : engine_(engine), id_(id), log_path_(log_path), assigned_threads_(assigned_threads) {
}

ErrorStack Logger::initialize_once() {
    return RET_OK;
}

ErrorStack Logger::uninitialize_once() {
    return RET_OK;
}

}  // namespace log
}  // namespace foedus
