/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_TYPE_HPP_
#define FOEDUS_LOG_LOG_TYPE_HPP_

// include all header files that forward-declare log types defined in the xmacro.
// don't include headers that really declare them. we just need foward-declarations here.
#include <foedus/log/fwd.hpp>
#include <foedus/storage/array/fwd.hpp>

namespace foedus {
namespace log {
/**
 * @defgroup LOGTYPE Log Types
 * @ingroup LOG
 * @brief Defines the content and \e apply logic of transactional operatrions.
 * @details
 * Each loggable operation defines a struct XxxLogType that has the following methods:
 *
 * \li "populate" method to populate all properties, but the method is not overridden and its
 * signature varies. This is just to have a uniform method name for readability.
 * \li void apply_engine(Engine*)     : For engine-wide operation.
 * \li void apply_storage(Storage*)   : For storage-wide operation.
 * \li void apply_record(Storage*, Record*)   : For record-wise operation.
 * \li is_engine_log()/is_storage_log()/is_record_log()
 * \li ostream operator, preferably in xml format without root element.
 *
 * For non-applicable apply-type, the implmentation class should abort.
 *
 * @par No polymorphism
 * There is polymorphism guaranteed for log types.
 * Because we read/write just a bunch of bytes and do reinterpret_cast, there is no dynamic
 * type information. We of course can't afford instantiating objects for each log entry.
 * Do not use any override in log type classes. You should even delete \b all constructors to avoid
 * misuse (see LOG_TYPE_NO_CONSTRUCT(clazz) ).
 * We do have base classes (EngineLogType, StorageLogType, and RecordLogType), but this is only
 * to reduce typing. No overridden methods.
 *
 * @par Current List of LogType
 * See foedus::log::LogCode
 *
 * @par log_type.hpp and log_type_invoke.hpp
 * This file defines only log codes and names, so quite compact even after preprocessing.
 * On the other hand, log_type_invoke.hpp defines more methods that need to include a few
 * more headers, so its size is quite big after proprocessing. Most places should need only
 * log_type.hpp. Include log_type_invoke.hpp only where we invoke apply/ostream etc.
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
 * @brief Returns the names of LogCode enum defined in log_type.xmacro.
 * @ingroup LOGTYPE
 */
const char* get_log_type_name(LogCode code);

// A bit tricky to get "a" from a in C macro.
#define X_QUOTE(str) #str
#define X_EXPAND_AND_QUOTE(str) X_QUOTE(str)
#define X(a, b, c) case a: return X_EXPAND_AND_QUOTE(a);
inline const char* get_log_type_name(LogCode code) {
    switch (code) {
        case LOG_TYPE_INVALID: return "LOG_TYPE_INVALID";
#include <foedus/log/log_type.xmacro> // NOLINT
        default: return "UNKNOWN";
    }
}
#undef X
#undef X_EXPAND_AND_QUOTE
#undef X_QUOTE

/**
 * @brief Returns LogCode for the log type defined in log_type.xmacro.
 * @ingroup LOGTYPE
 */
template <typename LOG_TYPE>
LogCode     get_log_code();

// give a template specialization for each log type class
#define X(a, b, c) template <> inline LogCode get_log_code< c >() { return a ; }
#include <foedus/log/log_type.xmacro> // NOLINT
#undef X

}  // namespace log
}  // namespace foedus

#endif  // FOEDUS_LOG_LOG_TYPE_HPP_
