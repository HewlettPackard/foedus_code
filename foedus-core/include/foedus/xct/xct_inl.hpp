/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_INL_HPP_
#define FOEDUS_XCT_XCT_INL_HPP_
#include <foedus/compiler.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/storage/record.hpp>
#include <foedus/xct/xct.hpp>
#include <foedus/xct/xct_access.hpp>
#include <iosfwd>
/**
 * @file foedus/xct/xct_inl.hpp
 * @brief Inline functions of Xct.
 * @ingroup XCT
 */
namespace foedus {
namespace xct {

inline ErrorCode Xct::add_to_read_set(storage::Record* record) {
    if (UNLIKELY(read_set_size_ >= max_read_set_size_)) {
        return ERROR_CODE_XCT_READ_SET_OVERFLOW;
    }

    read_set_[read_set_size_].observed_owner_id_ = record->owner_id_;
    read_set_[read_set_size_].record_ = record;
    ++read_set_size_;
    return ERROR_CODE_OK;
}
inline ErrorCode Xct::add_to_write_set(storage::Record* record) {
    if (UNLIKELY(write_set_size_ >= max_write_set_size_)) {
        return ERROR_CODE_XCT_WRITE_SET_OVERFLOW;
    }

    write_set_[write_set_size_].observed_owner_id_ = record->owner_id_;
    write_set_[write_set_size_].record_ = record;
    ++read_set_size_;
    return ERROR_CODE_OK;
}

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_INL_HPP_
