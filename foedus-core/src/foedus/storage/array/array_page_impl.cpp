/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/array/array_page_impl.hpp>
#include <cstring>
namespace foedus {
namespace storage {
namespace array {
void ArrayPage::initialize_data_page(StorageId storage_id, uint32_t payload_size,
                                            uint8_t node_height, const ArrayRange& array_range) {
    std::memset(this, 0, PAGE_SIZE);
    storage_id_ = storage_id;
    payload_size_ = payload_size;
    node_height_ = node_height;
    array_range_ = array_range;
}
}  // namespace array
}  // namespace storage
}  // namespace foedus
