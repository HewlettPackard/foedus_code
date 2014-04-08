/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
#define FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
#include <foedus/initializable.hpp>
#include <foedus/log/fwd.hpp>
namespace foedus {
namespace log {
/**
 * @brief Pimpl object of LogManager.
 * @ingroup LOG
 * @details
 * A private pimpl object for LogManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class LogManagerPimpl : public DefaultInitializable {
 public:
    LogManagerPimpl() = delete;
    explicit LogManagerPimpl(const LogOptions& options);
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    const LogOptions& options_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
