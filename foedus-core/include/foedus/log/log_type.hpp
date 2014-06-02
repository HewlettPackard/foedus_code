/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_TYPE_HPP_
#define FOEDUS_LOG_LOG_TYPE_HPP_

// include all header files that forward-declare log types defined in the xmacro.
// don't include headers that really declare them. we just need foward-declarations here.
#include <foedus/log/fwd.hpp>
#include <foedus/storage/fwd.hpp>
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
 * \li One of the 3 apply methods as follows. These also populate xct_order in log if applicable
 * (remember, XctId or xct_order is finalized only at commit time, so populate() can't do it).
 * \li void apply_engine(const XctId&, Thread*)             : For engine-wide operation.
 * \li void apply_storage(const XctId&, Thread*, Storage*)  : For storage-wide operation.
 * \li void apply_record(const XctId&, Thread*, Storage*, Record*)   : For record-wise operation.
 * \li void assert_valid()  : For debugging (should have no cost in NDEBUG).
 * \li is_engine_log()/is_storage_log()/is_record_log()
 * \li ostream operator, preferably in xml format.
 *
 * For non-applicable apply-type, the implmentation class should abort.
 * Remember that these are all non-virtual methods. See the next section for more details.
 *
 * @par No polymorphism
 * There is polymorphism guaranteed for log types.
 * Because we read/write just a bunch of bytes and do reinterpret_cast, there is no dynamic
 * type information. We of course can't afford instantiating objects for each log entry, either.
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
 * Log code values must follow the convention.
 * Most significant 4 bits are used to denote the kind of the log:
 * \li 0x0000: record targetted logs
 * \li 0x1000: storage targetted logs
 * \li 0x2000: engine targetted logs
 * \li 0x3000: markers/fillers
 * \li ~0xF000 (reserved for future use)
 */
#define X(a, b, c) /** b: c. @copydoc c */ a = b,
enum LogCode {
    /** 0 is reserved as a non-existing log type. */
    LOG_TYPE_INVALID = 0,
#include <foedus/log/log_type.xmacro> // NOLINT
};
#undef X

/**
 * @var LogCodeKind
 * @brief Represents the kind of log types.
 * @ingroup LOGTYPE
 * @details
 * This is the most significant 4 bits of LogCode.
 */
enum LogCodeKind {
    /** record targetted logs */
    RECORD_LOGS = 0,
    /** storage targetted logs */
    STORAGE_LOGS = 1,
    /** engine targetted logs */
    ENGINE_LOGS = 2,
    /** markers/fillers */
    MARKER_LOGS = 3,
};

/**
 * @brief Returns the kind of the given log code.
 * @ingroup LOGTYPE
 */
inline LogCodeKind get_log_code_kind(LogCode code) {
    return static_cast<LogCodeKind>(code >> 12);
}

/**
 * @brief Returns if the LogCode value exists.
 * @ingroup LOGTYPE
 */
inline bool is_valid_log_type(LogCode code) {
    switch (code) {
#define X(a, b, c) case a: return true;
#include <foedus/log/log_type.xmacro> // NOLINT
#undef X
        default: return false;
    }
}

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
