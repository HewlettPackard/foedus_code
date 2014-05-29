/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_TYPE_INVOKE_HPP_
#define FOEDUS_LOG_LOG_TYPE_INVOKE_HPP_
#include <foedus/assert_nd.hpp>
#include <foedus/log/log_type.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/xct/xct_id.hpp>

// include all header files that declare log types defined in the xmacro.
// unlike log_type.hpp, we need full declarations here. so this file would be big.
#include <foedus/log/common_log_types.hpp>
#include <foedus/storage/storage_log_types.hpp>
#include <foedus/storage/array/array_log_types.hpp>

#include <iostream>
namespace foedus {
namespace log {

/**
 * @brief Invokes the apply logic for an engine-wide log type.
 * @ingroup LOGTYPE
 */
void invoke_apply_engine(const xct::XctId &xct_id, void *log_buffer, thread::Thread* context);

/**
 * @brief Invokes the apply logic for a storage-wide log type.
 * @ingroup LOGTYPE
 */
void invoke_apply_storage(const xct::XctId &xct_id, void *log_buffer,
                          thread::Thread* context, storage::Storage* storage);

/**
 * @brief Invokes the apply logic for a record-wise log type.
 * @ingroup LOGTYPE
 * @details
 * This also unlocks the record by overwriting the record's owner_id.
 * @attention When the individual implementation of apply_record does it, it must be careful on
 * the memory order of unlock and data write. It must write data first, then unlock.
 * Otherwise the correctness is not guaranteed.
 * For that, apply_record() method must put memory_fence_release() between data and owner_id writes.
 */
void invoke_apply_record(const xct::XctId &xct_id, void *log_buffer,
                    thread::Thread* context, storage::Storage* storage, storage::Record* record);

/**
 * @brief Invokes the assertion logic of each log type.
 * @ingroup LOGTYPE
 */
void invoke_assert_valid(void *log_buffer);

/**
 * @brief Invokes the ostream operator for the given log type defined in log_type.xmacro.
 * @ingroup LOGTYPE
 * @details
 * This is only for debugging and analysis use, so does not have to be optimized.
 * This writes out an XML representation of the log entry.
 */
void invoke_ostream(void *buffer, std::ostream *ptr);

#define X(a, b, c) case a: return reinterpret_cast< c* >(buffer)->apply_engine(xct_id, context);
inline void invoke_apply_engine(const xct::XctId &xct_id, void *buffer, thread::Thread* context) {
    invoke_assert_valid(buffer);
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
    LogCode code = static_cast<LogCode>(header->log_type_code_);
    switch (code) {
#include <foedus/log/log_type.xmacro> // NOLINT
        default:
            ASSERT_ND(false);
            return;
    }
}
#undef X

#define X(a, b, c) case a: \
    reinterpret_cast< c* >(buffer)->apply_storage(xct_id, context, storage); return;
inline void invoke_apply_storage(const xct::XctId &xct_id, void *buffer,
                                 thread::Thread* context, storage::Storage* storage) {
    invoke_assert_valid(buffer);
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
    LogCode code = static_cast<LogCode>(header->log_type_code_);
    switch (code) {
#include <foedus/log/log_type.xmacro> // NOLINT
        default:
            ASSERT_ND(false);
            return;
    }
}
#undef X

#define X(a, b, c) case a: \
    reinterpret_cast< c* >(buffer)->apply_record(xct_id, context, storage, record); return;
inline void invoke_apply_record(const xct::XctId &xct_id, void *buffer,
                    thread::Thread* context, storage::Storage* storage, storage::Record* record) {
    invoke_assert_valid(buffer);
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
    LogCode code = static_cast<LogCode>(header->log_type_code_);
    switch (code) {
#include <foedus/log/log_type.xmacro> // NOLINT
        default:
            ASSERT_ND(false);
            return;
    }
}
#undef X

#ifdef NDEBUG
inline void invoke_assert_valid(void* /*buffer*/) {}
#else  // NDEBUG
#define X(a, b, c) case a: reinterpret_cast< c* >(buffer)->assert_valid(); return;
inline void invoke_assert_valid(void *buffer) {
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
    LogCode code = static_cast<LogCode>(header->log_type_code_);
    switch (code) {
#include <foedus/log/log_type.xmacro> // NOLINT
        default:
            ASSERT_ND(false);
            return;
    }
}
#undef X
#endif  // NDEBUG

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
