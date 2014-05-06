/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_LOG_TYPES_HPP_
#define FOEDUS_STORAGE_ARRAY_LOG_TYPES_HPP_
#include <foedus/log/common_log_types.hpp>
#include <foedus/log/log_type.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/storage/record.hpp>
#include <foedus/storage/storage_id.hpp>
#include <foedus/storage/array/fwd.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/storage/array/array_id.hpp>
#include <foedus/assert_nd.hpp>
#include <foedus/compiler.hpp>
#include <stdint.h>
#include <iosfwd>
#include <cstring>
/**
 * @file foedus/storage/array/array_log_types.hpp
 * @brief Declares all log types used in this storage type.
 * @ingroup ARRAY
 */
namespace foedus {
namespace storage {
namespace array {
/**
 * @brief Log type of array-storage's overwrite operation.
 * @ingroup ARRAY LOGTYPE
 * @details
 * This is the only modification operation in array.
 * It simply invokes memcpy to the payload.
 */
struct OverwriteLogType : public log::RecordLogType {
    LOG_TYPE_NO_CONSTRUCT(OverwriteLogType)
    ArrayOffset     offset_;            // +8 => 16
    uint16_t        payload_offset_;    // +2 => 18
    uint16_t        payload_count_;     // +2 => 20
    char            data_[4];           // +4 => 24

    static uint16_t calculate_log_length(uint16_t payload_count) ALWAYS_INLINE {
        // we pad to 8 bytes for efficiency (so far not for regular register access)
        return assorted::align8(20 + payload_count);
    }

    void            populate(StorageId storage_id, ArrayOffset offset, const void *payload,
                        uint16_t payload_offset, uint16_t payload_count) ALWAYS_INLINE {
        header_.log_type_code_ = log::get_log_code<OverwriteLogType>();
        header_.log_length_ = calculate_log_length(payload_count);
        header_.storage_id_ = storage_id;
        offset_ = offset;
        payload_offset_ = payload_offset;
        payload_count_ = payload_count;
        std::memcpy(data_, payload, payload_count);
    }
    void            apply_record(Storage* storage, Record* record) ALWAYS_INLINE {
        ASSERT_ND(payload_count_ < DATA_SIZE);
        ASSERT_ND(dynamic_cast<ArrayStorage*>(storage));
        std::memcpy(record->payload_ + payload_offset_, data_, payload_count_);
    }

    friend std::ostream& operator<<(std::ostream& o, const OverwriteLogType& v);
};
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_LOG_TYPES_HPP_
