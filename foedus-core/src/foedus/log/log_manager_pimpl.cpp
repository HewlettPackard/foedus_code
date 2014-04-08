/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/log/log_manager_pimpl.hpp>
#include <foedus/log/log_options.hpp>
namespace foedus {
namespace log {
LogManagerPimpl::LogManagerPimpl(const LogOptions& options) : options_(options) {
}
ErrorStack LogManagerPimpl::initialize_once() {
    return RET_OK;
}

ErrorStack LogManagerPimpl::uninitialize_once() {
    return RET_OK;
}

}  // namespace log
}  // namespace foedus
