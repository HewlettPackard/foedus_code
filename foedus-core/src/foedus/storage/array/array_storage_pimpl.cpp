/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/storage/record.hpp>
#include <foedus/storage/array/array_log_types.hpp>
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
#include <foedus/xct/xct.hpp>
#include <foedus/xct/xct_inl.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <foedus/log/thread_log_buffer_impl.hpp>
#include <foedus/log/log_type.hpp>
#include <foedus/engine.hpp>
#include <glog/logging.h>
#include <string>
#include <vector>
namespace foedus {
namespace storage {
namespace array {

// Defines ArrayStorage methods so that we can inline implementation calls
bool        ArrayStorage::is_initialized()   const  { return pimpl_->is_initialized(); }
bool        ArrayStorage::exists()           const  { return pimpl_->exist_; }
uint16_t    ArrayStorage::get_payload_size() const  { return pimpl_->payload_size_; }
ArrayOffset ArrayStorage::get_array_size()   const  { return pimpl_->array_size_; }
StorageId   ArrayStorage::get_id()           const  { return pimpl_->id_; }
const std::string& ArrayStorage::get_name()  const  { return pimpl_->name_; }
ErrorStack ArrayStorage::get_record(thread::Thread* context, ArrayOffset offset,
                    void *payload, uint16_t payload_offset, uint16_t payload_count) {
    return pimpl_->get_record(context, offset, payload, payload_offset, payload_count);
}

template <typename T>
ErrorStack ArrayStorage::get_record_primitive(thread::Thread* context, ArrayOffset offset,
                    T *payload, uint16_t payload_offset) {
    return pimpl_->get_record_primitive<T>(context, offset, payload, payload_offset);
}

ErrorStack ArrayStorage::overwrite_record(thread::Thread* context, ArrayOffset offset,
            const void *payload, uint16_t payload_offset, uint16_t payload_count) {
    return pimpl_->overwrite_record(context, offset, payload, payload_offset, payload_count);
}

template <typename T>
ErrorStack ArrayStorage::overwrite_record_primitive(thread::Thread* context, ArrayOffset offset,
                    T payload, uint16_t payload_offset) {
    return pimpl_->overwrite_record_primitive<T>(context, offset, payload, payload_offset);
}

template <typename T>
ErrorStack ArrayStorage::increment_record(thread::Thread* context, ArrayOffset offset,
                    T* value, uint16_t payload_offset) {
    return pimpl_->increment_record<T>(context, offset, value, payload_offset);
}

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
    ASSERT_ND(id > 0);
    ASSERT_ND(name.size() > 0);
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

    Epoch initial_epoch = engine_->get_xct_manager().get_current_global_epoch();
    LOG(INFO) << "Newly creating an array-storage " << id_ << "(" << name_ << ") as epoch="
        << initial_epoch;

    // TODO(Hideaki) This part must handle the case where RAM < Array Size
    // So far, we just do ASSERT_ND(offset) after memory->grab_free_page().
    memory::NumaCoreMemory *memory = context->get_thread_memory();

    // we create from left, keeping cursors on each level.
    // first, create the left-most in each level
    // All of the following, index=level
    std::vector<ArrayPage*> current_pages;
    std::vector<memory::PagePoolOffset> current_pages_offset;
    std::vector<uint16_t> current_records;
    for (uint8_t level = 0; level < levels_; ++level) {
        memory::PagePoolOffset offset = memory->grab_free_page();
        ASSERT_ND(offset);
        ArrayPage* page = reinterpret_cast<ArrayPage*>(resolver_.resolve_offset(offset));

        ArrayRange range(0, offset_intervals_[level]);
        if (range.end_ > array_size_) {
            ASSERT_ND(level == levels_ - 1);
            range.end_ = array_size_;
        }
        page->initialize_data_page(initial_epoch, id_, payload_size_, level, range);

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
    ASSERT_ND(current_pages.size() == levels_);
    ASSERT_ND(current_pages_offset.size() == levels_);
    ASSERT_ND(current_records.size() == levels_);

    // then moves on to right
    for (uint64_t leaf = 1; leaf < pages_[0]; ++leaf) {
        memory::PagePoolOffset offset = memory->grab_free_page();
        ASSERT_ND(offset);
        ArrayPage* page = reinterpret_cast<ArrayPage*>(resolver_.resolve_offset(offset));

        ArrayRange range(current_pages[0]->get_array_range().end_,
                         current_pages[0]->get_array_range().end_ + offset_intervals_[0]);
        if (range.end_ > array_size_) {
            range.end_ = array_size_;
        }
        page->initialize_data_page(initial_epoch, id_, payload_size_, 0, range);
        current_pages[0] = page;
        current_pages_offset[0] = offset;
        // current_records[0] is always 0

        // push it up to parent, potentially up to root
        for (uint8_t level = 1; level < levels_; ++level) {
            if (current_records[level] == INTERIOR_FANOUT) {
                VLOG(2) << "leaf=" << leaf << ", interior level=" << static_cast<int>(level);
                memory::PagePoolOffset interior_offset = memory->grab_free_page();
                ASSERT_ND(interior_offset);
                ArrayPage* interior_page = reinterpret_cast<ArrayPage*>(
                    resolver_.resolve_offset(interior_offset));
                ArrayRange interior_range(current_pages[level]->get_array_range().end_,
                         current_pages[level]->get_array_range().end_ + offset_intervals_[level]);
                if (range.end_ > array_size_) {
                    range.end_ = array_size_;
                }
                interior_page->initialize_data_page(
                    initial_epoch, id_, payload_size_, level, interior_range);

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

inline ErrorStack ArrayStoragePimpl::locate_record(
    thread::Thread* context, ArrayOffset offset, Record** out) {
    ASSERT_ND(is_initialized());
    ASSERT_ND(offset < array_size_);
    ArrayPage* page = nullptr;
    CHECK_ERROR(lookup(context, offset, &page));
    ASSERT_ND(page);
    ASSERT_ND(page->is_leaf());
    ASSERT_ND(page->get_array_range().contains(offset));
    ArrayOffset index = offset - page->get_array_range().begin_;
    *out = page->get_leaf_record(index);
    return RET_OK;
}

inline ErrorStack ArrayStoragePimpl::get_record(thread::Thread* context, ArrayOffset offset,
                    void *payload, uint16_t payload_offset, uint16_t payload_count) {
    ASSERT_ND(payload_offset + payload_count <= payload_size_);
    Record *record = nullptr;
    CHECK_ERROR(locate_record(context, offset, &record));
    CHECK_ERROR_CODE(context->get_current_xct().read_record(holder_, record,
                                                        payload, payload_offset, payload_count));
    return RET_OK;
}

template <typename T>
ErrorStack ArrayStoragePimpl::get_record_primitive(thread::Thread* context, ArrayOffset offset,
                    T *payload, uint16_t payload_offset) {
    ASSERT_ND(payload_offset + sizeof(T) <= payload_size_);
    Record *record = nullptr;
    CHECK_ERROR(locate_record(context, offset, &record));
    CHECK_ERROR_CODE(context->get_current_xct().read_record_primitive<T>(holder_, record,
                                                                      payload, payload_offset));
    return RET_OK;
}

inline ErrorStack ArrayStoragePimpl::overwrite_record(thread::Thread* context, ArrayOffset offset,
            const void *payload, uint16_t payload_offset, uint16_t payload_count) {
    ASSERT_ND(payload_offset + payload_count <= payload_size_);
    Record *record = nullptr;
    CHECK_ERROR(locate_record(context, offset, &record));

    // write out log
    uint16_t log_length = OverwriteLogType::calculate_log_length(payload_count);
    OverwriteLogType* log_entry = reinterpret_cast<OverwriteLogType*>(
        context->get_thread_log_buffer().reserve_new_log(log_length));
    log_entry->populate(id_, offset, payload, payload_offset, payload_count);
    CHECK_ERROR_CODE(context->get_current_xct().add_to_write_set(holder_, record, log_entry));
    return RET_OK;
}

template <typename T>
ErrorStack ArrayStoragePimpl::overwrite_record_primitive(
            thread::Thread* context, ArrayOffset offset, T payload, uint16_t payload_offset) {
    ASSERT_ND(payload_offset + sizeof(T) <= payload_size_);
    Record *record = nullptr;
    CHECK_ERROR(locate_record(context, offset, &record));

    // write out log
    uint16_t log_length = OverwriteLogType::calculate_log_length(sizeof(T));
    OverwriteLogType* log_entry = reinterpret_cast<OverwriteLogType*>(
        context->get_thread_log_buffer().reserve_new_log(log_length));
    log_entry->populate_primitive<T>(id_, offset, payload, payload_offset);
    CHECK_ERROR_CODE(context->get_current_xct().add_to_write_set(holder_, record, log_entry));
    return RET_OK;
}

template <typename T>
ErrorStack ArrayStoragePimpl::increment_record(
            thread::Thread* context, ArrayOffset offset, T* value, uint16_t payload_offset) {
    ASSERT_ND(payload_offset + sizeof(T) <= payload_size_);
    Record *record = nullptr;
    CHECK_ERROR(locate_record(context, offset, &record));

    // this is get_record + overwrite_record
    T old_value;
    CHECK_ERROR_CODE(context->get_current_xct().read_record_primitive<T>(
        holder_, record, &old_value, payload_offset));
    *value += old_value;
    uint16_t log_length = OverwriteLogType::calculate_log_length(sizeof(T));
    OverwriteLogType* log_entry = reinterpret_cast<OverwriteLogType*>(
        context->get_thread_log_buffer().reserve_new_log(log_length));
    log_entry->populate_primitive<T>(id_, offset, *value, payload_offset);
    CHECK_ERROR_CODE(context->get_current_xct().add_to_write_set(holder_, record, log_entry));
    return RET_OK;
}

inline ErrorStack ArrayStoragePimpl::lookup(thread::Thread* /*context*/, ArrayOffset offset,
                                            ArrayPage** out) {
    ASSERT_ND(is_initialized());
    ASSERT_ND(offset < array_size_);
    ASSERT_ND(out);
    ArrayPage* current_page = root_page_;
    while (!current_page->is_leaf()) {
        ASSERT_ND(current_page->get_array_range().contains(offset));
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
    ASSERT_ND(current_page->get_array_range().contains(offset));
    *out = current_page;
    return RET_OK;
}


// Explicit instantiations for each type
// @cond DOXYGEN_IGNORE
#define EXPLICIT_INSTANTIATION_GET(x) template ErrorStack ArrayStorage::get_record_primitive< x > \
    (thread::Thread* context, ArrayOffset offset, x *payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET);

#define EXPLICIT_INSTANTIATION_OV(x) template ErrorStack\
    ArrayStorage::overwrite_record_primitive< x > \
    (thread::Thread* context, ArrayOffset offset, x payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_OV);

#define EXPLICIT_INSTANTIATION_INC(x) template ErrorStack ArrayStorage::increment_record< x > \
    (thread::Thread* context, ArrayOffset offset, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_INC);

#define EXPLICIT_INSTANTIATION_GET_IMPL(x) template ErrorStack \
    ArrayStoragePimpl::get_record_primitive< x > \
    (thread::Thread* context, ArrayOffset offset, x *payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET_IMPL);

#define EXPLICIT_INSTANTIATION_GET_OV_IMPL(x) template ErrorStack \
    ArrayStoragePimpl::overwrite_record_primitive< x > \
    (thread::Thread* context, ArrayOffset offset, x payload, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET_OV_IMPL);

#define EXPLICIT_INSTANTIATION_GET_INC_IMPL(x) template ErrorStack \
    ArrayStoragePimpl::increment_record< x > \
    (thread::Thread* context, ArrayOffset offset, x* value, uint16_t payload_offset)
INSTANTIATE_ALL_NUMERIC_TYPES(EXPLICIT_INSTANTIATION_GET_INC_IMPL);
// @endcond

}  // namespace array
}  // namespace storage
}  // namespace foedus
