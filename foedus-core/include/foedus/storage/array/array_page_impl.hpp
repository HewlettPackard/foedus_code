/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_PAGE_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_PAGE_HPP_
#include <foedus/storage/record.hpp>
#include <foedus/storage/storage_id.hpp>
#include <foedus/storage/array/array_id.hpp>
#include <foedus/xct/xct_id.hpp>
#include <stdint.h>
#include <cassert>
namespace foedus {
namespace storage {
namespace array {

/**
 * @brief Represents one data page in \ref ARRAY.
 * @ingroup ARRAY
 * @details
 * This is a private implementation-details of \ref ARRAY, thus file name ends with _impl.
 * Do not include this header from a client program unless you know what you are doing.
 * @attention Do NOT instantiate this object or derive from this class.
 * A page is always reinterpret-ed from a pooled memory region. No meaningful RTTI.
 */
class ArrayPage final {
 public:
    /**
     * Non-leaf node contains pointers to leaf child nodes.
     */
    struct InteriorRecord {
        DualPagePointer     pointer_;
    };
    static_assert(sizeof(InteriorRecord) == INTERIOR_SIZE, "INTERIOR_SIZE is incorrect");

    /**
     * Data union for either leaf (dynamic size) or interior (fixed size).
     */
    union Data {
        char            leaf_data[1];
        InteriorRecord  interior_data[INTERIOR_FANOUT];
    };

    // A page object is never explicitly instantiated. You must reinterpret_cast.
    ArrayPage() = delete;
    ArrayPage(const ArrayPage& other) = delete;
    ArrayPage& operator=(const ArrayPage& other) = delete;

    // simple accessors
    StorageId           get_storage_id()    const   { return storage_id_; }
    uint16_t            get_payload_size()  const   { return payload_size_; }
    bool                is_leaf()           const   { return node_height_ == 0; }
    uint8_t             get_node_height()   const   { return node_height_; }
    const ArrayRange&   get_array_range()   const   { return array_range_; }
    Checksum            get_checksum()      const   { return checksum_; }
    void                set_checksum(Checksum checksum)     { checksum_ = checksum; }

    /** Called only when this page is initialized. */
    void                initialize_data_page(StorageId storage_id, uint16_t payload_size,
                                              uint8_t node_height, const ArrayRange& array_range);

    // Record accesses
    const Record*   get_leaf_record(uint16_t record) const {
        return const_cast<ArrayPage*>(this)->get_leaf_record(record);
    }
    Record*         get_leaf_record(uint16_t record) {
        assert(is_leaf());
        assert(HEADER_SIZE + (record + 1) * (RECORD_OVERHEAD + payload_size_) <= PAGE_SIZE);
        return reinterpret_cast<Record*>(data_.leaf_data
            + record * (RECORD_OVERHEAD + payload_size_));
    }
    const InteriorRecord*   get_interior_record(uint16_t record) const {
        return const_cast<ArrayPage*>(this)->get_interior_record(record);
    }
    InteriorRecord*         get_interior_record(uint16_t record) {
        assert(!is_leaf());
        assert(record < INTERIOR_FANOUT);
        return data_.interior_data + record;
    }

 private:
    /** ID of the array storage. */
    StorageId           storage_id_;    // +4 -> 4

    /** Byte size of one record in this array storage without internal overheads. */
    uint16_t            payload_size_;  // +2 -> 6

    /** Height of this node, counting up from 0 (leaf). */
    uint8_t             node_height_;   // +1 -> 7

    uint8_t             reserved1_;     // +1 -> 8

    /** The offset range this node is in charge of. */
    ArrayRange          array_range_;   // +16 -> 24

    // All variables up to here are immutable after the array storage is created.

    /**
     * Checksum of the content of this page to detect corrupted pages.
     * \b Changes only when we save it to media. No synchronization needed to access.
     */
    Checksum            checksum_;      // +8 -> 32

    /** Dynamic records in this page. */
    Data                data_;
};
static_assert(sizeof(ArrayPage) <= PAGE_SIZE, "sizeof(ArrayPage) exceeds PAGE_SIZE");
static_assert(sizeof(ArrayPage) - sizeof(ArrayPage::Data) == HEADER_SIZE, "HEADER_SIZE is wrong");

}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_PAGE_HPP_
