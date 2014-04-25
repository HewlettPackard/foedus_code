/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_COMMON_LOG_TYPES_HPP_
#define FOEDUS_LOG_COMMON_LOG_TYPES_HPP_
#include <foedus/error_stack.hpp>
#include <foedus/fwd.hpp>
#include <foedus/storage/fwd.hpp>
#include <foedus/storage/storage_id.hpp>
#include <iosfwd>

/**
 * @file foedus/log/common_log_types.hpp
 * @brief Declares common log types used in all packages.
 * @ingroup LOG
 */
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

    friend std::ostream& operator<<(std::ostream& o, const LogHeader& v);
};

/**
 * @var LOG_TYPE_NO_CONSTRUCT(clazz)
 * @brief Macro to delete all constructors/destructors to prevent misuse for log type classes.
 * @ingroup LOGTYPE
 */
#define LOG_TYPE_NO_CONSTRUCT(clazz) \
    clazz() CXX11_FUNC_DELETE;\
    clazz(const clazz &other) CXX11_FUNC_DELETE;\
    ~clazz() CXX11_FUNC_DELETE;

/**
 * @brief Base class for log type.
 * @ingroup LOGTYPE
 */
struct BaseLogType {
    LogHeader   header_;
};

/**
 * @brief Base class for log type of engine-wide operation.
 * @ingroup LOGTYPE
 */
struct EngineLogType : public BaseLogType {
    bool    is_engine_log()     const { return true; }
    bool    is_storage_log()    const { return false; }
    bool    is_record_log()     const { return false; }
    ErrorStack apply_storage(storage::Storage* /*storage*/) {
        return ERROR_STACK(ERROR_CODE_LOG_INVALID_APPLY_TYPE);
    }
    ErrorStack apply_record(storage::Storage* /*storage*/, storage::Record* /*record*/) {
        return ERROR_STACK(ERROR_CODE_LOG_INVALID_APPLY_TYPE);
    }
};
/**
 * @brief Base class for log type of storage-wide operation.
 * @ingroup LOGTYPE
 */
struct StorageLogType : public BaseLogType {
    LogHeader   header_;
    bool    is_engine_log()     const { return false; }
    bool    is_storage_log()    const { return true; }
    bool    is_record_log()     const { return false; }
    ErrorStack apply_engine(Engine* /*engine*/) {
        return ERROR_STACK(ERROR_CODE_LOG_INVALID_APPLY_TYPE);
    }
    ErrorStack apply_record(storage::Storage* /*storage*/, storage::Record* /*record*/) {
        return ERROR_STACK(ERROR_CODE_LOG_INVALID_APPLY_TYPE);
    }
};
/**
 * @brief Base class for log type of record-wise operation.
 * @ingroup LOGTYPE
 */
struct RecordLogType : public BaseLogType {
    LogHeader   header_;
    bool    is_engine_log()     const { return false; }
    bool    is_storage_log()    const { return false; }
    bool    is_record_log()     const { return true; }
    ErrorStack apply_engine(Engine* /*engine*/) {
        return ERROR_STACK(ERROR_CODE_LOG_INVALID_APPLY_TYPE);
    }
    ErrorStack apply_storage(storage::Storage* /*storage*/) {
        return ERROR_STACK(ERROR_CODE_LOG_INVALID_APPLY_TYPE);
    }
};

/**
 * @brief A dummy log type to fill up a sector in log files.
 * @ingroup LOG LOGTYPE
 * @details
 * As we do direct I/O, we must do file I/O in multiply of 4kb.
 * We pad the log buffer we are about to write with this log type.
 * Log gleaner simply skips this log.
 */
struct FillerLogType : public BaseLogType {
    /** Constant values. */
    enum Constants {
        /**
         * We always write to file in a multiply of this value, filling up the rest if needed.
         * 4kb Disk Sector (512b earlier, but nowadays 4kb especially on SSD).
         */
        LOG_WRITE_UNIT_SIZE = 1 << 12,
    };

    LOG_TYPE_NO_CONSTRUCT(FillerLogType)

    // this is a special log type where it is valid and skipped in every context
    bool    is_engine_log()     const { return true; }
    bool    is_storage_log()    const { return true; }
    bool    is_record_log()     const { return true; }
    ErrorStack apply_engine(Engine* /*engine*/) { return RET_OK; }
    ErrorStack apply_storage(storage::Storage* /*storage*/) { return RET_OK; }
    ErrorStack apply_record(storage::Storage* /*storage*/, storage::Record* /*record*/) {
        return RET_OK;
    }

    friend std::ostream& operator<<(std::ostream& o, const FillerLogType&) { return o; }
};
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_COMMON_LOG_TYPES_HPP_
