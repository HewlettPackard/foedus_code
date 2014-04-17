/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/storage/storage.hpp>
#include <foedus/storage/storage_id.hpp>
#include <foedus/storage/array/array_id.hpp>
#include <foedus/storage/array/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <string>
#include <vector>
namespace foedus {
namespace storage {
namespace array {
/**
 * @brief Pimpl object of ArrayStorage.
 * @ingroup ARRAY
 * @details
 * A private pimpl object for ArrayStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ArrayStoragePimpl final : public DefaultInitializable {
 public:
    ArrayStoragePimpl() = delete;
    ArrayStoragePimpl(Engine* engine, ArrayStorage* holder,
                      StorageId id, const std::string &name, uint16_t payload_size,
        ArrayOffset array_size, DualPagePointer root_page_pointer, bool create);

    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    ErrorStack  create(thread::Thread* context);

    ErrorStack  get_record(thread::Thread* context, ArrayOffset offset,
                                void *payload, uint16_t payload_offset, uint16_t payload_count);
    ErrorStack  overwrite_record(thread::Thread* context, ArrayOffset offset,
                        const void *payload, uint16_t payload_offset, uint16_t payload_count);

    ErrorStack  lookup(thread::Thread* context, ArrayOffset offset, ArrayPage** out);

    Engine* const           engine_;
    ArrayStorage* const     holder_;
    const StorageId         id_;
    const std::string       name_;
    const uint16_t          payload_size_;
    const uint16_t          payload_size_aligned_;
    const ArrayOffset       array_size_;
    /**
     * Number of pages in each level. index=level.
     */
    std::vector<uint64_t>   pages_;
    /**
     * The offset interval a single page represents in each level. index=level.
     * So, offset_intervals_[0] is the number of records in a leaf page.
     */
    std::vector<uint64_t>   offset_intervals_;
    /** Number of levels. */
    uint8_t                 levels_;

    DualPagePointer         root_page_pointer_;

    /**
     * Root page is assured to be never evicted.
     * So, we can access the root_page_ without going through caching module.
     */
    ArrayPage*              root_page_;

    bool                    exist_;

    /** auxiliary. same as engine_->get_memory_manager().get_page_pool(). */
    memory::PagePool*       pool_;
    /** auxiliary. same as reinterpret_cast<ArrayPage*>(pool->get_base_address()). */
    ArrayPage*              base_address_;
};
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
