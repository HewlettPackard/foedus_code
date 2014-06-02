/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_UTIL_DUPM_LOG_HPP_
#define FOEDUS_UTIL_DUPM_LOG_HPP_
#include <foedus/epoch.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/log/fwd.hpp>
#include <stdint.h>
#include <iosfwd>
#include <vector>
namespace foedus {
namespace util {

// X-Macro for LogInconsistency
#define LOG_INCONSISTENCIES \
    X(INCOMPLETE_ENTRY_AT_END, "A log entry that is not fully stored in log file." \
         " This might be possible at the tail of log after non-graceful engine shutdown."\
         " Additional data: length of the incomplete log") \
    X(NON_ALIGNED_FILE_END, \
        "File size is not aligned to 4kb. This most likely comes with INCOMPLETE_ENTRY_AT_END.")\
    X(MISSING_LOG_LENGTH, "Log length is zero. A bug or corrupt log.") \
    X(MISSING_LOG_TYPE_CODE, "Log code not set or non-existing. A bug or software version issue."\
         " Additional data: log type code") \
    X(MISSING_STORAGE_ID, \
        "Storage ID is not set of storage/record type logs. A bug or corrupt log."\
        " Additional data: log type code") \
    X(INVALID_OLD_EPOCH, "old_epoch field of epoch marker is invalid")\
    X(INVALID_NEW_EPOCH, "new_epoch field of epoch marker is invalid")\
    X(EPOCH_MARKER_DOES_NOT_MATCH, "From field of epoch marker is inconsistent")
/**
 * Represents one inconsistency found in log files.
 */
struct LogInconsistency {
    enum InconsistencyType {
        CONSISTENT = 0,
#define X(a, b) /** b */ a,
LOG_INCONSISTENCIES
#undef X
    };
    static const char* type_to_string(InconsistencyType type) {
        switch (type) {
            case CONSISTENT: return "CONSISTENT";
#define X_QUOTE(str) #str
#define X_EXPAND_AND_QUOTE(str) X_QUOTE(str)
#define X(a, b) case a: return X_EXPAND_AND_QUOTE(a);
LOG_INCONSISTENCIES
#undef X
#undef X_EXPAND_AND_QUOTE
#undef X_QUOTE
            default:
                return "UNKNOWN";
        }
    }
    static const char* type_to_description(InconsistencyType type) {
        switch (type) {
            case CONSISTENT: return "not an error";
#define X(a, b) case a: return b;
LOG_INCONSISTENCIES
#undef X
            default:
                return "UNKNOWN";
        }
    }
    LogInconsistency() : type_(CONSISTENT), file_index_(0), offset_(0), additional_data_(0) {}
    LogInconsistency(InconsistencyType type, uint32_t file_index, uint64_t offset,
                     uint64_t additional_data = 0) : type_(type), file_index_(file_index),
                     offset_(offset), additional_data_(additional_data) {}

    /** Type of inconsistency. */
    InconsistencyType   type_;

    /** Index in DumpLog::files_ */
    uint32_t            file_index_;

    /** starting byte offset. */
    uint64_t            offset_;

    /** Any inconsistency-specific information. */
    uint64_t            additional_data_;

    friend std::ostream& operator<<(std::ostream& o, const LogInconsistency& v);
};

struct DumpLog {
    enum Verbosity {
        BRIEF = 0,
        NORMAL = 1,
        DETAIL = 2,
    };

    DumpLog() {
        verbose_ = BRIEF;
        limit_ = -1;
        from_epoch_ = INVALID_EPOCH;
        to_epoch_ = INVALID_EPOCH;
        result_processed_logs_ = 0;
        result_limit_reached_ = false;
        result_cur_epoch_ = INVALID_EPOCH;
        result_first_epoch_ = INVALID_EPOCH;
        result_last_epoch_ = INVALID_EPOCH;
    }

    Verbosity                           verbose_;
    int32_t                             limit_;
    Epoch                               from_epoch_;
    Epoch                               to_epoch_;
    std::vector< foedus::fs::Path >     files_;

    /** When this reaches limit_, we stop processing. */
    uint64_t                            result_processed_logs_;
    /** Might become true only when limit_ is set. */
    bool                                result_limit_reached_;
    Epoch                               result_cur_epoch_;
    Epoch                               result_first_epoch_;
    Epoch                               result_last_epoch_;
    std::vector< LogInconsistency >     result_inconsistencies_;

    /** main routine of foedus_dump_log utility */
    int dump_to_stdout();

    struct ParserCallback {
        ParserCallback() {}
        virtual ~ParserCallback() {}
        virtual void process(log::LogHeader *entry, uint64_t offset) = 0;
    };
    void parse_log_file(uint32_t file_index, ParserCallback* callback);
};

}  // namespace util
}  // namespace foedus
#endif  // FOEDUS_UTIL_DUPM_LOG_HPP_
