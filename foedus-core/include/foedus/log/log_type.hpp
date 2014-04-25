/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_TYPE_HPP_
#define FOEDUS_LOG_LOG_TYPE_HPP_
// include all header files that define log types defined
#include <foedus/cxx11.hpp>
#include <foedus/log/log_types.hpp>
#include <foedus/storage/array/log_types.hpp>
namespace foedus {
namespace log {
/**
 * @defgroup LOGTYPE Log Types
 * @ingroup LOG
 * @brief Defines the content and \e apply logic of transactional operatrions.
 * @details
 * Each loggable operation defines a struct XxxLogType that has the following methods:
 * \li bluh
 * \li apply()
 *
 * @par No polymorphism
 * There is no base-class or interface for log types.
 * Because we read/write just a bunch of bytes and do reinterpret_cast, there is no dynamic
 * type information. We of course can't afford instantiating objects for each log entry.
 * Do not use any override in log type classes. You should even delete \b all constructors to avoid
 * misuse (see LOG_TYPE_NO_CONSTRUCT(clazz) ).
 *
 * @par Current List of LogType
 * See foedus::log::LogCode
 */

/**
 * @var LogCode
 * @brief A unique identifier of all log types.
 * @ingroup LOGTYPE
 * @details
 */
#define X(a, b, c) /** b: c. */ a = b,
enum LogCode {
    /** 0 is reserved as a non-existing log type. */
    LOG_TYPE_INVALID = 0,
#include <foedus/log/log_type.xmacro> // NOLINT
};
#undef X


/**
 * @brief Returns the names of ErrorCode enum defined in error_code.xmacro.
 * @ingroup LOGTYPE
 */
const char* get_log_type_name(LogCode code);

/**
 * @brief Returns the error messages corresponding to ErrorCode enum defined in error_code.xmacro.
 * @ingroup LOGTYPE
 */
void invoke_apply(LogCode code, void *buffer);

// A bit tricky to get "a" from a in C macro.
#define X_QUOTE(str) #str
#define X_EXPAND_AND_QUOTE(str) X_QUOTE(str)
#define X(a, b, c) case a: return X_EXPAND_AND_QUOTE(a);
inline const char* get_log_type_name(LogCode code) {
    switch (code) {
        case LOG_TYPE_INVALID: return "LOG_TYPE_INVALID";
#include <foedus/log/log_type.xmacro> // NOLINT
    }
    return "Unexpected log code";
}
#undef X
#undef X_EXPAND_AND_QUOTE
#undef X_QUOTE

#define X(a, b, c) case a: reinterpret_cast< c* >(buffer)->apply();
inline void invoke_apply(LogCode code, void *buffer) {
    switch (code) {
        case LOG_TYPE_INVALID: return;
#include <foedus/log/log_type.xmacro> // NOLINT
    }
    return;
}
#undef X

}  // namespace log
}  // namespace foedus

#endif  // FOEDUS_LOG_LOG_TYPE_HPP_
