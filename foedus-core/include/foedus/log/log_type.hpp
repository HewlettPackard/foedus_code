/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_LOG_LOG_TYPE_HPP_
#define FOEDUS_LOG_LOG_TYPE_HPP_

#include "foedus/cxx11.hpp"
// include all header files that forward-declare log types defined in the xmacro.
// don't include headers that really declare them. we just need foward-declarations here.
#include "foedus/log/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/array/fwd.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/sequential/fwd.hpp"

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
 * \li void apply_engine(Thread*)             : For engine-wide operation.
 * \li void apply_storage(Engine*, StorageId) : For storage-wide operation.
 * \li void apply_record(Thread*, StorageId, LockableXctId*, char*)   : For record-wise operation.
 * \li void assert_valid()  : For debugging (should have no cost in NDEBUG).
 * \li is_engine_log()/is_storage_log()/is_record_log()
 * \li ostream operator, preferably in xml format.
 *
 * For non-applicable apply-type, the implmentation class should abort.
 * Remember that these are all non-virtual methods. See the next section for more details.
 *
 * @par No polymorphism
 * There is no polymorphism guaranteed for log types.
 * Because we read/write just a bunch of bytes and do reinterpret_cast, there is no dynamic
 * type information. We of course can't afford instantiating objects for each log entry, either.
 * Do not use any override in log type classes. You should even delete \b all constructors to avoid
 * misuse (see LOG_TYPE_NO_CONSTRUCT(clazz) ).
 * We do have base classes (EngineLogType, StorageLogType, and RecordLogType), but this is only
 * to reduce typing. No virtual methods.
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
  kLogCodeInvalid = 0,
#include "foedus/log/log_type.xmacro" // NOLINT
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
  kRecordLogs = 0,
  /** storage targetted logs */
  kStorageLogs = 1,
  /** engine targetted logs */
  kEngineLogs = 2,
  /** markers/fillers */
  kMarkerLogs = 3,
};

/**
 * @brief Returns the kind of the given log code.
 * @ingroup LOGTYPE
 * @details
 * This is inlined here because it's called frequently.
 */
inline LogCodeKind get_log_code_kind(LogCode code) {
  return static_cast<LogCodeKind>(code >> 12);
}

/**
 * @brief Returns if the LogCode value exists.
 * @ingroup LOGTYPE
 * @details
 * This is inlined here because it's called frequently.
 */
inline bool is_valid_log_type(LogCode code) {
  switch (code) {
#define X(a, b, c) case a: return true;
#include "foedus/log/log_type.xmacro" // NOLINT
#undef X
    default: return false;
  }
}

/**
 * @brief Returns the names of LogCode enum defined in log_type.xmacro.
 * @ingroup LOGTYPE
 * @details
 * This is NOT inlined because this is used only in debugging situation.
 */
const char* get_log_type_name(LogCode code);

/**
 * @brief Returns LogCode for the log type defined in log_type.xmacro.
 * @ingroup LOGTYPE
 * @details
 * This is inlined below because it's called VERY frequently.
 * This method is declared as constexpr if C++11 is enabled, in which case there should
 * be really no overheads to call this method.
 */
template <typename LOG_TYPE>
CXX11_CONSTEXPR LogCode get_log_code();

// give a template specialization for each log type class
#define X(a, b, c) template <> inline CXX11_CONSTEXPR LogCode get_log_code< c >() { return a ; }
#include "foedus/log/log_type.xmacro" // NOLINT
#undef X

}  // namespace log
}  // namespace foedus

#endif  // FOEDUS_LOG_LOG_TYPE_HPP_
