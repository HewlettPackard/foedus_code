/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_LOG_HEADER_HPP_
#define FOEDUS_LOG_LOG_HEADER_HPP_
#include <foedus/storage/storage_id.hpp>
#include <stdint.h>
namespace foedus {
namespace log {
/**
 * @brief A common header part for all log types.
 * @ingroup LOGTYPE
 * @details
 * Each log type should contain this as the first member.
 * This is 8-byte, so compiler won't do any reorder or filling.
 */
struct LogHeader {
    /**
     * Actually of LogCode defined in the X-Macro, but we want to make sure
     * the type size is 2 bytes. (and to avoid C++11 feature in public header).
     */
    uint16_t            log_type_code_;  // +2 => 2
    /**
     * Byte size of this log entry including this header itself and everything.
     * We so far support up to 64KB per log.
     */
    uint16_t            log_length_;     // +2 => 4
    /**
     * The storage this loggable operation mainly affects.
     * If this operation is agnostic to individual storages, zero.
     */
    storage::StorageId  storage_id_;     // +4 => 8
};
}  // namespace log
}  // namespace foedus

/**
 * @var LOG_TYPE_NO_CONSTRUCT(clazz)
 * @brief Macro to delete all constructors/destructors to prevent misuse for log type classes.
 * @ingroup LOGTYPE
 */
#define LOG_TYPE_NO_CONSTRUCT(clazz) \
    clazz() CXX11_FUNC_DELETE;\
    clazz(const clazz &other) CXX11_FUNC_DELETE;\
    ~clazz() CXX11_FUNC_DELETE;

#endif  // FOEDUS_LOG_LOG_HEADER_HPP_
