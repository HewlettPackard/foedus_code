/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/epoch.hpp>
#include <foedus/storage/array/array_page_impl.hpp>
#include <cstring>
namespace foedus {
namespace storage {
namespace array {
void ArrayPage::initialize_data_page(Epoch initial_epoch, StorageId storage_id,
                    uint16_t payload_size, uint8_t node_height, const ArrayRange& array_range) {
    std::memset(this, 0, PAGE_SIZE);
    storage_id_ = storage_id;
    payload_size_ = payload_size;
    node_height_ = node_height;
    array_range_ = array_range;
    if (is_leaf()) {
        uint16_t records = get_leaf_record_count();
        for (uint16_t i = 0; i < records; ++i) {
            get_leaf_record(i)->owner_id_.data_.components.epoch_int = initial_epoch.value();
        }
    } else {
        /* TODO(Hideaki)
        for (uint16_t i = 0; i < INTERIOR_FANOUT; ++i) {
            get_interior_record(i)->pointer_.volatile_pointer_.components. = initial_epoch;
        }
        */
    }
}
}  // namespace array
}  // namespace storage
}  // namespace foedus
