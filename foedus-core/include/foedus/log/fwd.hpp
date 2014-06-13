/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_FWD_HPP_
#define FOEDUS_LOG_FWD_HPP_
/**
 * @file foedus/log/fwd.hpp
 * @brief Forward declarations of classes in log manager package.
 * @ingroup LOG
 */
namespace foedus {
namespace log {
struct  BaseLogType;
struct  EngineLogType;
struct  EpochHistory;
struct  EpochMarkerLogType;
struct  FillerLogType;
struct  LogHeader;
class   LogManager;
class   LogManagerPimpl;
struct  LogOptions;
class   Logger;
struct  RecordLogType;
struct  StorageLogType;
class   ThreadLogBuffer;
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_FWD_HPP_
