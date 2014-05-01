/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
#define FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/fwd.hpp>
#include <foedus/thread/thread_id.hpp>
#include <foedus/xct/epoch.hpp>
#include <vector>
namespace foedus {
namespace log {
/**
 * @brief Pimpl object of LogManager.
 * @ingroup LOG
 * @details
 * A private pimpl object for LogManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class LogManagerPimpl CXX11_FINAL : public DefaultInitializable {
 public:
    LogManagerPimpl() = delete;
    explicit LogManagerPimpl(Engine* engine) : engine_(engine) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    Engine* const               engine_;

    thread::ThreadGroupId       groups_;

    /**
     * Log writers.
     */
    std::vector< Logger* >      loggers_;

    /**
     * All loggers flushed their logs upto this epoch.
     */
    xct::Epoch                  durable_epoch_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_MANAGER_PIMPL_HPP_
