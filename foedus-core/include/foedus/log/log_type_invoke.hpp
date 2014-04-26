/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_TYPE_INVOKE_HPP_
#define FOEDUS_LOG_LOG_TYPE_INVOKE_HPP_
#include <foedus/log/log_type.hpp>

// include all header files that declare log types defined in the xmacro.
// unlike log_type.hpp, we need full declarations here. so this file would be big.
#include <foedus/log/common_log_types.hpp>
#include <foedus/storage/array/array_log_types.hpp>

#include <cassert>
#include <iostream>
namespace foedus {
namespace log {

/**
 * @brief Invokes the apply logic for an engine-wide log type.
 * @ingroup LOGTYPE
 */
void invoke_apply_engine(void *log_buffer, Engine* engine);

/**
 * @brief Invokes the apply logic for a storage-wide log type.
 * @ingroup LOGTYPE
 */
void invoke_apply_storage(void *log_buffer, storage::Storage* storage);

/**
 * @brief Invokes the apply logic for a record-wise log type.
 * @ingroup LOGTYPE
 */
void invoke_apply_record(void *log_buffer, storage::Storage* storage, storage::Record* record);

/**
 * @brief Invokes the ostream operator for the given log type defined in log_type.xmacro.
 * @ingroup LOGTYPE
 * @details
 * This is only for debugging and analysis use, so does not have to be optimized.
 * This writes out an XML representation of the log entry.
 */
void invoke_ostream(void *buffer, std::ostream *ptr);

#define X(a, b, c) case a: return reinterpret_cast< c* >(buffer)->apply_engine(engine);
inline void invoke_apply_engine(void *buffer, Engine* engine) {
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
    LogCode code = static_cast<LogCode>(header->log_type_code_);
    switch (code) {
#include <foedus/log/log_type.xmacro> // NOLINT
        default:
            assert(false);
            return;
    }
}
#undef X

#define X(a, b, c) case a: reinterpret_cast< c* >(buffer)->apply_storage(storage); return;
inline void invoke_apply_storage(void *buffer, storage::Storage* storage) {
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
    LogCode code = static_cast<LogCode>(header->log_type_code_);
    switch (code) {
#include <foedus/log/log_type.xmacro> // NOLINT
        default:
            assert(false);
            return;
    }
}
#undef X

#define X(a, b, c) case a: reinterpret_cast< c* >(buffer)->apply_record(storage, record); return;
inline void invoke_apply_record(void *buffer,
                                      storage::Storage* storage, storage::Record* record) {
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
    LogCode code = static_cast<LogCode>(header->log_type_code_);
    switch (code) {
#include <foedus/log/log_type.xmacro> // NOLINT
        default:
            assert(false);
            return;
    }
}
#undef X

#define X(a, b, c) case a: o << *reinterpret_cast< c* >(buffer); break;
inline void invoke_ostream(void *buffer, std::ostream *ptr) {
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
    LogCode code = static_cast<LogCode>(header->log_type_code_);
    std::ostream &o = *ptr;
    o << "<" << get_log_type_name(code) << ">";
    o << reinterpret_cast<LogHeader*>(buffer);
    switch (code) {
        case LOG_TYPE_INVALID: break;
#include <foedus/log/log_type.xmacro> // NOLINT
    }
    o << "</" << get_log_type_name(code) << ">";
}
#undef X

}  // namespace log
}  // namespace foedus

#endif  // FOEDUS_LOG_LOG_TYPE_INVOKE_HPP_
