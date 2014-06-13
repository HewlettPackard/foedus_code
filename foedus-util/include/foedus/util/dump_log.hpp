/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_UTIL_DUPM_LOG_HPP_
#define FOEDUS_UTIL_DUPM_LOG_HPP_
#include <foedus/epoch.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/log/fwd.hpp>
#include <foedus/log/common_log_types.hpp>
#include <stdint.h>
#include <iosfwd>
#include <vector>
namespace foedus {
namespace util {

// X-Macro for LogInconsistency
#define LOG_INCONSISTENCIES \
    X(kIncompleteEntryAtEnd, "A log entry that is not fully stored in log file." \
         " This might be possible at the tail of log after non-graceful engine shutdown.") \
    X(kNonAlignedFileEnd, \
        "File size is not aligned to 4kb. This most likely comes with kIncompleteEntryAtEnd.")\
    X(kMissingLogLength, "Log length is zero. A bug or corrupt log. Stopped reading this file.") \
    X(kMissingLogTypeCode, "Log code not set or non-existing. A bug or software version issue.") \
    X(kMissingStorageId, \
        "Storage ID is not set of storage/record type logs. A bug or corrupt log.") \
    X(kInvalidOldExpoch, "old_epoch field of epoch marker is invalid")\
    X(kInvaligNewEpoch, "new_epoch field of epoch marker is invalid")\
    X(kNoEpochMarkerAtBeginning, "The log file does not start with epoch marker.")\
    X(kEpochMarkerDoesNotMatch, "From field of epoch marker is inconsistent")\
    X(kEpochMarkerIncorrectOffset, "Offset field of epoch marker is wrong")\
    X(kTooManyInconsistencies, "Too many inconsistencies found.")
/**
 * Represents one inconsistency found in log files.
 */
struct LogInconsistency {
    enum InconsistencyType {
        kConsistent = 0,
#define X(a, b) /** b */ a,
LOG_INCONSISTENCIES
#undef X
    };
    static const char* type_to_string(InconsistencyType type) {
        switch (type) {
            case kConsistent: return "kConsistent";
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
            case kConsistent: return "not an error";
#define X(a, b) case a: return b;
LOG_INCONSISTENCIES
#undef X
            default:
                return "UNKNOWN";
        }
    }
    LogInconsistency(InconsistencyType type = kConsistent, uint32_t file_index = 0,
                     uint64_t offset = 0) : type_(type), file_index_(file_index), offset_(offset) {
        header_.log_length_ = 0;
        header_.log_type_code_ = 0;
        header_.storage_id_ = 0;
    }
    LogInconsistency(InconsistencyType type, uint32_t file_index, uint64_t offset,
                     const log::LogHeader &header)
        : type_(type), file_index_(file_index), offset_(offset), header_(header) {}

    /** Type of inconsistency. */
    InconsistencyType   type_;

    /** Index in DumpLog::files_ */
    uint32_t            file_index_;

    /** starting byte offset. */
    uint64_t            offset_;

    /** Header of . */
    log::LogHeader      header_;

    friend std::ostream& operator<<(std::ostream& o, const LogInconsistency& v);
};

struct DumpLog {
    enum Verbosity {
        kBrief = 0,
        kNormal = 1,
        kDetail = 2,
    };

    DumpLog() {
        verbose_ = kBrief;
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
