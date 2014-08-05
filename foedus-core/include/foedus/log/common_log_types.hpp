/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_LOG_COMMON_LOG_TYPES_HPP_
#define FOEDUS_LOG_COMMON_LOG_TYPES_HPP_
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/epoch.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

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
 * This is 16-byte, so compiler won't do any reorder or filling.
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
   * @todo all log types are 8-byte aligned, so this can store /8, allowing a 512KB log.
   */
  uint16_t            log_length_;     // +2 => 4
  /**
   * The storage this loggable operation mainly affects.
   * If this operation is agnostic to individual storages, zero.
   */
  storage::StorageId  storage_id_;     // +4 => 8

  /**
   * Epoch and in-epoch ordinal of this log.
   * Unlike other fields, xct_id is set at commit time because we have no idea what the
   * epoch and the in-epoch ordinal will be until that time.
   * Basically all logs have this information, but FillerLogType does not have it so that
   * it fits in 8 bytes.
   */
  xct::XctId          xct_id_;         // +8 => 16

  /** Convenience method to cast into LogCode. */
  LogCode get_type() const ALWAYS_INLINE { return static_cast<LogCode>(log_type_code_); }
  /** Convenience method to get LogCodeKind. */
  LogCodeKind get_kind() const ALWAYS_INLINE { return get_log_code_kind(get_type()); }

  /** Another convenience method to see if the type code is non-zero and exists. */
  bool is_valid_type() const { return is_valid_log_type(get_type()); }

  /** Because of the special case of FillerLogType, we must use this method to set xct_id */
  inline void set_xct_id(xct::XctId new_xct_id) {
    if (log_length_ >= 16) {
      xct_id_ = new_xct_id;
    } else {
      // So far only log type that omits xct_id is FillerLogType.
      ASSERT_ND(get_type() == kLogCodeFiller);
    }
  }

  friend std::ostream& operator<<(std::ostream& o, const LogHeader& v);
};

/**
 * @def LOG_TYPE_NO_CONSTRUCT(clazz)
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
STATIC_SIZE_CHECK(sizeof(BaseLogType), 16)

/**
 * @brief Base class for log type of engine-wide operation.
 * @ingroup LOGTYPE
 */
struct EngineLogType : public BaseLogType {
  bool    is_engine_log()     const { return true; }
  bool    is_storage_log()    const { return false; }
  bool    is_record_log()     const { return false; }
  void apply_storage(thread::Thread* /*context*/, storage::Storage* /*storage*/) {
    ASSERT_ND(false);
  }
  void apply_record(
    thread::Thread* /*context*/,
    storage::Storage* /*storage*/,
    xct::XctId* /*owner_id*/,
    char* /*payload*/) {
    ASSERT_ND(false);
  }
  /**
   * @brief Verifies the log contains essential fields set.
   */
  void assert_valid_generic() const ALWAYS_INLINE {
    ASSERT_ND(header_.is_valid_type());
    ASSERT_ND(header_.log_length_ != 0);
    ASSERT_ND(header_.log_length_ % 8 == 0);  // must be 8 byte aligned
    ASSERT_ND(header_.storage_id_ == 0);
  }
};
STATIC_SIZE_CHECK(sizeof(EngineLogType), 16)

/**
 * @brief Base class for log type of storage-wide operation.
 * @ingroup LOGTYPE
 */
struct StorageLogType : public BaseLogType {
  bool    is_engine_log()     const { return false; }
  bool    is_storage_log()    const { return true; }
  bool    is_record_log()     const { return false; }
  void apply_engine(thread::Thread* /*context*/) {
    ASSERT_ND(false);
  }
  void apply_record(
    thread::Thread* /*context*/,
    storage::Storage* /*storage*/,
    xct::XctId* /*owner_id*/,
    char* /*payload*/) {
    ASSERT_ND(false);
  }
  /**
   * @brief Verifies the log contains essential fields set.
   */
  void assert_valid_generic() ALWAYS_INLINE {
    ASSERT_ND(header_.is_valid_type());
    ASSERT_ND(header_.log_length_ != 0);
    ASSERT_ND(header_.log_length_ % 8 == 0);  // must be 8 byte aligned
    ASSERT_ND(header_.storage_id_ > 0);
  }
};
STATIC_SIZE_CHECK(sizeof(StorageLogType), 16)

/**
 * @brief Base class for log type of record-wise operation.
 * @ingroup LOGTYPE
 */
struct RecordLogType : public BaseLogType {
  bool    is_engine_log()     const { return false; }
  bool    is_storage_log()    const { return false; }
  bool    is_record_log()     const { return true; }
  void apply_engine(thread::Thread* /*context*/) {
    ASSERT_ND(false);
  }
  void apply_storage(thread::Thread* /*context*/, storage::Storage* /*storage*/) {
    ASSERT_ND(false);
  }
  /**
   * @brief Verifies the log contains essential fields set.
   */
  void assert_valid_generic() const ALWAYS_INLINE {
    ASSERT_ND(header_.is_valid_type());
    ASSERT_ND(header_.log_length_ != 0);
    ASSERT_ND(header_.log_length_ % 8 == 0);  // must be 8 byte aligned
    ASSERT_ND(header_.storage_id_ > 0);
  }
};
STATIC_SIZE_CHECK(sizeof(RecordLogType), 16)

/**
 * @brief A dummy log type to fill up a sector in log files.
 * @ingroup LOG LOGTYPE
 * @details
 * As we do direct I/O, we must do file I/O in multiply of 4kb.
 * We pad the log buffer we are about to write with this log type.
 * Log gleaner simply skips this log.
 * This is the only log type whose size might be smaller than sizeof(FillerLogType).
 * This happens because the common log header is 16 bytes but we have to make this log type
 * 8 bytes able to fill every gap. For this reason, the xct_id_ property of this log
 * must not be used.
 */
struct FillerLogType : public BaseLogType {
  /** Constant values. */
  enum Constants {
    /**
     * We always write to file in a multiply of this value, filling up the rest if needed.
     * 4kb Disk Sector (512b earlier, but nowadays 4kb especially on SSD).
     */
    kLogWriteUnitSize = 1 << 12,
  };

  LOG_TYPE_NO_CONSTRUCT(FillerLogType)

  // this is a special log type where it is valid and skipped in every context
  bool    is_engine_log()     const { return true; }
  bool    is_storage_log()    const { return true; }
  bool    is_record_log()     const { return true; }
  void    apply_engine(thread::Thread* /*context*/) {}
  void    apply_storage(thread::Thread* /*context*/, storage::Storage* /*storage*/) {}
  void    apply_record(
    thread::Thread* /*context*/,
    storage::Storage* /*storage*/,
    xct::XctId* /*owner_id*/,
    char* /*payload*/) {}

  /** Populate this log to fill up the specified byte size. */
  void    populate(uint64_t size);

  void    assert_valid() const ALWAYS_INLINE {
    ASSERT_ND(header_.get_type() == kLogCodeFiller);
    ASSERT_ND(header_.log_length_ >= 8);  // note: it CAN be only 8, not 16 (sizeof FillerLogType)
    ASSERT_ND(header_.log_length_ % 8 == 0);
    ASSERT_ND(header_.storage_id_ == 0);
  }

  friend std::ostream& operator<<(std::ostream& o, const FillerLogType &v);
};
STATIC_SIZE_CHECK(sizeof(FillerLogType), 16)
// NOTE: As a class, it's 16 bytes. However, it might be only 8 bytes in actual log.
// In that case, xct_id is omitted.

/**
 * @brief A log type to declare a switch of epoch in a logger or the engine.
 * @ingroup LOG LOGTYPE
 * @details
 * Each logger puts this marker when it switches epoch. When applied, this just adds
 * epoch switch history which is maintained until related logs are gleaned and garbage collected.
 *
 * The epoch switch history is used to efficiently identify the beginning of each epoch in each
 * logger. This is useful for example when we take samples from each epoch.
 *
 * Now that we contain XctId in every log, it's not necessary to put this marker.
 * However, having this log makes a few things easier; the epoch history management for
 * seeking to beginning of a specific epoch and several sanity checks.
 * So, we still keep this log. It's anyway only occadionally written, so no harm.
 * Every log file starts with an epoch mark for this reason, too.
 */
struct EpochMarkerLogType : public EngineLogType {
  LOG_TYPE_NO_CONSTRUCT(EpochMarkerLogType)

  void    apply_engine(thread::Thread* context);

  /** Epoch before this switch. */
  Epoch   old_epoch_;  // +4  => 20
  /** Epoch after this switch. */
  Epoch   new_epoch_;  // +4  => 24

  /** Numa node of the logger that produced this log. */
  uint8_t     logger_numa_node_;  // +1 => 25
  /** Ordinal of the logger in the numa node. */
  uint8_t     logger_in_node_ordinal_;  // +1 => 26
  /** Unique ID of the logger. */
  uint16_t    logger_id_;  // +2 => 28

  /** Ordinal of log files (eg "log.0", "log.1"). */
  uint32_t    log_file_ordinal_;  // +4 => 32

  /**
   * Byte offset of the epoch mark log itself in the log. We can put this value in this log
   * because logger knows the current offset of its own file when it writes out an epoch mark.
   */
  uint64_t    log_file_offset_;  // +8 => 40

  void    populate(Epoch old_epoch, Epoch new_epoch,
           uint8_t logger_numa_node, uint8_t logger_in_node_ordinal,
           uint16_t logger_id, uint32_t log_file_ordinal, uint64_t log_file_offset);
  void    assert_valid() const ALWAYS_INLINE { assert_valid_generic(); }

  friend std::ostream& operator<<(std::ostream& o, const EpochMarkerLogType &v);
};
STATIC_SIZE_CHECK(sizeof(EpochMarkerLogType), 40)

}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_COMMON_LOG_TYPES_HPP_
