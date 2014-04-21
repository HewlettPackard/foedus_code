/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/record.hpp>
#include <foedus/storage/array/array_id.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/storage/array/array_storage_pimpl.hpp>
#include <foedus/storage/array/array_page_impl.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <foedus/memory/memory_id.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/memory/page_pool.hpp>
#include <foedus/engine.hpp>
#include <foedus/xct/xct.hpp>
#include <foedus/xct/xct_inl.hpp>
#include <glog/logging.h>
#include <cstring>
#include <string>
#include <vector>
namespace foedus {
namespace storage {
namespace array {

/**
 * Calculate leaf/interior pages we need.
 * @return index=level.
 */
std::vector<uint64_t> calculate_required_pages(uint64_t array_size, uint16_t payload) {
    uint64_t records_per_page = DATA_SIZE / (payload + RECORD_OVERHEAD);

    // so, how many leaf pages do we need?
    uint64_t leaf_pages = assorted::int_div_ceil(array_size, records_per_page);
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
    return pages;
}

ArrayStoragePimpl::ArrayStoragePimpl(Engine* engine, ArrayStorage* holder, StorageId id,
    const std::string &name,
    uint16_t payload_size, ArrayOffset array_size, DualPagePointer root_page_pointer, bool create)
    : engine_(engine), holder_(holder), id_(id), name_(name), payload_size_(payload_size),
        payload_size_aligned_(assorted::align8(payload_size)), array_size_(array_size),
        root_page_pointer_(root_page_pointer), root_page_(nullptr), exist_(!create) {
    pages_ = calculate_required_pages(array_size_, payload_size_aligned_);
    levels_ = pages_.size();
    offset_intervals_.push_back(DATA_SIZE / (payload_size_aligned_ + RECORD_OVERHEAD));
    for (uint8_t level = 1; level < levels_; ++level) {
        offset_intervals_.push_back(offset_intervals_[level - 1] * INTERIOR_FANOUT);
    }
}

ErrorStack ArrayStoragePimpl::initialize_once() {
    LOG(INFO) << "Initializing an array-storage " << id_ << "(" << name_ << ") exists=" << exist_
        << " levels=" << static_cast<int>(levels_);
    for (uint8_t level = 0; level < levels_; ++level) {
        LOG(INFO) << "Level-" << static_cast<int>(level) << " pages=" << pages_[level]
            << " interval=" << offset_intervals_[level];
    }

    if (exist_) {
        // initialize root_page_
    }
    resolver_ = engine_->get_memory_manager().get_page_pool()->get_resolver();
    return RET_OK;
}

void release_pages_recursive(memory::PageResolver *resolver, memory::NumaCoreMemory* memory,
                             ArrayPage* page, memory::PagePoolOffset offset) {
    if (!page->is_leaf()) {
        for (uint16_t i = 0; i < INTERIOR_FANOUT; ++i) {
            DualPagePointer &child_pointer = page->get_interior_record(i)->pointer_;
            memory::PagePoolOffset child_offset = child_pointer.volatile_pointer_.components.offset;
            if (child_offset) {
                // then recurse
                ArrayPage* child_page = reinterpret_cast<ArrayPage*>(
                    resolver->resolve_offset(child_offset));
                release_pages_recursive(resolver, memory, child_page, child_offset);
                child_pointer.volatile_pointer_.components.offset = 0;
            }
        }
    }
    memory->release_free_page(offset);
}

ErrorStack ArrayStoragePimpl::uninitialize_once() {
    LOG(INFO) << "Uninitializing an array-storage " << id_ << "(" << name_ << ") exists=" << exist_;
    if (root_page_) {
        LOG(INFO) << "Releasing all in-memory pages...";
        // We don't care which core to return this memory. Just pick the first.
        memory::NumaCoreMemory* memory = engine_->get_memory_manager().get_core_memory(0);
        release_pages_recursive(&resolver_, memory, root_page_,
                                root_page_pointer_.volatile_pointer_.components.offset);
        root_page_ = nullptr;
        root_page_pointer_.volatile_pointer_.components.offset = 0;
    }
    return RET_OK;
}


ErrorStack ArrayStoragePimpl::create(thread::Thread* context) {
    if (exist_) {
        LOG(ERROR) << "This array-storage already exists: " << id_ << "(" << name_ << ")";
        return ERROR_STACK(ERROR_CODE_STR_ALREADY_EXISTS);
    }

    LOG(INFO) << "Newly creating an array-storage " << id_ << "(" << name_ << ")";

    // TODO(Hideaki) This part must handle the case where RAM < Array Size
    // So far, we just do assert(offset) after memory->grab_free_page().
    memory::NumaCoreMemory *memory = context->get_thread_memory();

    // we create from left, keeping cursors on each level.
    // first, create the left-most in each level
    // All of the following, index=level
    std::vector<ArrayPage*> current_pages;
    std::vector<memory::PagePoolOffset> current_pages_offset;
    std::vector<uint16_t> current_records;
    for (uint8_t level = 0; level < levels_; ++level) {
        memory::PagePoolOffset offset = memory->grab_free_page();
        assert(offset);
        ArrayPage* page = reinterpret_cast<ArrayPage*>(resolver_.resolve_offset(offset));

        ArrayRange range(0, offset_intervals_[level]);
        if (range.end_ > array_size_) {
            assert(level == levels_ - 1);
            range.end_ = array_size_;
        }
        page->initialize_data_page(id_, payload_size_, level, range);

        current_pages.push_back(page);
        current_pages_offset.push_back(offset);
        if (level == 0) {
            current_records.push_back(0);
        } else {
            current_records.push_back(1);
            DualPagePointer& child_pointer = page->get_interior_record(0)->pointer_;
            child_pointer.snapshot_page_id_ = 0;
            child_pointer.volatile_pointer_.components.mod_count = 0;
            child_pointer.volatile_pointer_.components.offset = current_pages_offset[level - 1];
        }
    }
    assert(current_pages.size() == levels_);
    assert(current_pages_offset.size() == levels_);
    assert(current_records.size() == levels_);

    // then moves on to right
    for (uint64_t leaf = 1; leaf < pages_[0]; ++leaf) {
        memory::PagePoolOffset offset = memory->grab_free_page();
        assert(offset);
        ArrayPage* page = reinterpret_cast<ArrayPage*>(resolver_.resolve_offset(offset));

        ArrayRange range(current_pages[0]->get_array_range().end_,
                         current_pages[0]->get_array_range().end_ + offset_intervals_[0]);
        if (range.end_ > array_size_) {
            range.end_ = array_size_;
        }
        page->initialize_data_page(id_, payload_size_, 0, range);
        current_pages[0] = page;
        current_pages_offset[0] = offset;
        // current_records[0] is always 0

        // push it up to parent, potentially up to root
        for (uint8_t level = 1; level < levels_; ++level) {
            if (current_records[level] == INTERIOR_FANOUT) {
                VLOG(2) << "leaf=" << leaf << ", interior level=" << static_cast<int>(level);
                memory::PagePoolOffset interior_offset = memory->grab_free_page();
                assert(interior_offset);
                ArrayPage* interior_page = reinterpret_cast<ArrayPage*>(
                    resolver_.resolve_offset(interior_offset));
                ArrayRange interior_range(current_pages[level]->get_array_range().end_,
                         current_pages[level]->get_array_range().end_ + offset_intervals_[level]);
                if (range.end_ > array_size_) {
                    range.end_ = array_size_;
                }
                interior_page->initialize_data_page(id_, payload_size_, level, interior_range);

                DualPagePointer& child_pointer = interior_page->get_interior_record(0)->pointer_;
                child_pointer.snapshot_page_id_ = 0;
                child_pointer.volatile_pointer_.components.mod_count = 0;
                child_pointer.volatile_pointer_.components.offset = current_pages_offset[level - 1];
                current_pages[level] = interior_page;
                current_pages_offset[level] = interior_offset;
                current_records[level] = 1;
                // also inserts this to parent
            } else {
                DualPagePointer& child_pointer = current_pages[level]->get_interior_record(
                    current_records[level])->pointer_;
                child_pointer.snapshot_page_id_ = 0;
                child_pointer.volatile_pointer_.components.mod_count = 0;
                child_pointer.volatile_pointer_.components.offset = current_pages_offset[level - 1];
                ++current_records[level];
                break;
            }
        }
    }

    root_page_pointer_.snapshot_page_id_ = 0;
    root_page_pointer_.volatile_pointer_.components.mod_count = 0;
    root_page_pointer_.volatile_pointer_.components.offset = current_pages_offset[levels_ - 1];
    root_page_ = current_pages[levels_ - 1];
    LOG(INFO) << "Newly created an array-storage " << id_ << "(" << name_ << ")";
    exist_ = true;
    engine_->get_storage_manager().register_storage(holder_);
    return RET_OK;
}

ErrorStack ArrayStoragePimpl::get_record(thread::Thread* context, ArrayOffset offset,
                            void *payload, uint16_t payload_offset, uint16_t payload_count) {
    assert(is_initialized());
    assert(offset < array_size_);
    assert(payload_offset + payload_count <= payload_size_);
    ArrayPage* page = nullptr;
    CHECK_ERROR(lookup(context, offset, &page));
    assert(page);
    assert(page->is_leaf());
    assert(page->get_array_range().contains(offset));
    ArrayOffset index = offset - page->get_array_range().begin_;
    Record *record = page->get_leaf_record(index);
    // TODO(Hideaki) Handle too-many-read-set error
    context->get_current_xct().add_to_read_set(record);
    std::memcpy(payload, record->payload_ + payload_offset, payload_count);
    return RET_OK;
}
ErrorStack ArrayStoragePimpl::overwrite_record(thread::Thread* context,
        ArrayOffset offset, const void *payload, uint16_t payload_offset, uint16_t payload_count) {
    assert(is_initialized());
    assert(offset < array_size_);
    assert(payload_offset + payload_count <= payload_size_);
    ArrayPage* page = nullptr;
    CHECK_ERROR(lookup(context, offset, &page));
    assert(page);
    assert(page->is_leaf());
    assert(page->get_array_range().contains(offset));
    ArrayOffset index = offset - page->get_array_range().begin_;
    Record *record = page->get_leaf_record(index);
    // TODO(Hideaki) Handle too-many-write-set error
    context->get_current_xct().add_to_write_set(record);
    // TODO(Hideaki) Add to private log
    std::memcpy(record->payload_ + payload_offset, payload, payload_count);
    return RET_OK;
}

inline ErrorStack ArrayStoragePimpl::lookup(thread::Thread* context, ArrayOffset offset,
                                            ArrayPage** out) {
    assert(is_initialized());
    assert(offset < array_size_);
    assert(out);
    ArrayPage* current_page = root_page_;
    while (!current_page->is_leaf()) {
        assert(current_page->get_array_range().contains(offset));
        uint64_t diff = offset - current_page->get_array_range().begin_;
        uint16_t record = diff / offset_intervals_[current_page->get_node_height() - 1];
        DualPagePointer& pointer = current_page->get_interior_record(record)->pointer_;
        // TODO(Hideaki) Add to Node-set (?)
        if (pointer.volatile_pointer_.components.offset == 0) {
            // TODO(Hideaki) Read the page from cache.
            return ERROR_STACK(ERROR_CODE_NOTIMPLEMENTED);
        } else {
            current_page = reinterpret_cast<ArrayPage*>(
                resolver_.resolve_offset(pointer.volatile_pointer_.components.offset));
        }
    }
    assert(current_page->get_array_range().contains(offset));
    *out = current_page;
    return RET_OK;
}


}  // namespace array
}  // namespace storage
}  // namespace foedus
