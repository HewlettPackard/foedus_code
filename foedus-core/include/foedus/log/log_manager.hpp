/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_MANAGER_HPP_
#define FOEDUS_LOG_LOG_MANAGER_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/fwd.hpp>
namespace foedus {
namespace log {
/**
 * @brief Log Manager class that provides API to write/read transaction logs.
 * @ingroup LOG
 */
class LogManager CXX11_FINAL : public virtual Initializable {
 public:
    explicit LogManager(Engine* engine);
    ~LogManager();

    // Disable default constructors
    LogManager() CXX11_FUNC_DELETE;
    LogManager(const LogManager&) CXX11_FUNC_DELETE;
    LogManager& operator=(const LogManager&) CXX11_FUNC_DELETE;

    ErrorStack  initialize() CXX11_OVERRIDE;
    bool        is_initialized() const CXX11_OVERRIDE;
    ErrorStack  uninitialize() CXX11_OVERRIDE;

 private:
    LogManagerPimpl *pimpl_;
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_LOG_MANAGER_HPP_
