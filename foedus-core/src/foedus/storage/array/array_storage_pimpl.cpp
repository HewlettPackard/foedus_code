/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/record.hpp>
#include <foedus/storage/array/array_id.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/storage/array/array_storage_pimpl.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <glog/logging.h>
#include <string>
#include <vector>
namespace foedus {
namespace storage {
namespace array {
ArrayStoragePimpl::ArrayStoragePimpl(Engine* engine, StorageId id, const std::string &name,
    uint16_t payload_size, ArrayOffset array_size, DualPagePointer root_page, bool create)
    : engine_(engine), id_(id), name_(name), payload_size_(payload_size),
        payload_size_aligned_(assorted::align8(payload_size)),
        array_size_(array_size), root_page_(root_page), create_(create) {
}

ErrorStack ArrayStoragePimpl::initialize_once() {
    if (create_) {
        CHECK_ERROR(create());
    } else {
        LOG(INFO) << "Loaded an array-storage " << id_ << "(" << name_ << ")";
    }
    return RET_OK;
}

ErrorStack ArrayStoragePimpl::uninitialize_once() {
    return RET_OK;
}

ErrorStack ArrayStoragePimpl::create() {
    LOG(INFO) << "Newly creating an array-storage " << id_ << "(" << name_ << ")";
    uint64_t total_payloads = (payload_size_aligned_ + RECORD_OVERHEAD) * array_size_;
    LOG(INFO) << "total_payloads=" << total_payloads << "B (" << (total_payloads >> 20) << "MB)";

    // so, how many leaf pages do we need?
    uint64_t page_data_size = PAGE_SIZE - HEADER_SIZE;
    uint64_t leaf_pages = assorted::int_div_ceil(total_payloads, page_data_size);
    LOG(INFO) << "We need " << leaf_pages << " leaf pages";

    // interior nodes
    uint64_t total_pages = leaf_pages;
    std::vector<uint64_t> pages;
    pages.push_back(leaf_pages);
    while (pages.back() != 1) {
        uint64_t next_level_pages = assorted::int_div_ceil(pages.back(), INTERIOR_FANOUT);
        LOG(INFO) << "Level-" << pages.size() << " would have " << next_level_pages << " pages";
        pages.push_back(next_level_pages);
        total_pages += next_level_pages;
    }

    LOG(INFO) << "In total, we need " << total_pages << " pages";

    return RET_OK;
}

ErrorStack ArrayStoragePimpl::get_record(ArrayOffset offset, void *payload) {
    assert(is_initialized());
    return RET_OK;
}
ErrorStack ArrayStoragePimpl::get_record_part(ArrayOffset offset, void *payload,
                            uint16_t payload_offset, uint16_t payload_count) {
    assert(is_initialized());
    return RET_OK;
}
ErrorStack ArrayStoragePimpl::overwrite_record(ArrayOffset offset, const void *payload) {
    assert(is_initialized());
    return RET_OK;
}
ErrorStack ArrayStoragePimpl::overwrite_record_part(ArrayOffset offset, const void *payload,
                            uint16_t payload_offset, uint16_t payload_count) {
    assert(is_initialized());
    return RET_OK;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
