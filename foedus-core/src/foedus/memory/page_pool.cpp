/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/memory/page_pool.hpp>
#include <foedus/memory/page_pool_pimpl.hpp>
#include <cassert>
#include <cstring>
namespace foedus {
namespace memory {
void PagePoolOffsetChunk::push_back(const PagePoolOffset* begin, const PagePoolOffset* end) {
    uint32_t count = end - begin;
    assert(size_ + count <= MAX_SIZE);
    std::memcpy(chunk_ + size_, begin, count * sizeof(PagePoolOffset));
    size_ += count;
}
void PagePoolOffsetChunk::move_to(PagePoolOffset* destination, uint32_t count) {
    assert(size_ >= count);
    std::memcpy(destination, chunk_ + (size_ - count), count * sizeof(PagePoolOffset));
    size_ -= count;
}

PagePool::PagePool(Engine* engine) {
    pimpl_ = new PagePoolPimpl(engine);
}
PagePool::~PagePool() {
    delete pimpl_;
    pimpl_ = nullptr;
}

ErrorStack  PagePool::initialize() { return pimpl_->initialize(); }
bool        PagePool::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  PagePool::uninitialize() { return pimpl_->uninitialize(); }

ErrorCode   PagePool::grab(uint32_t desired_grab_count, PagePoolOffsetChunk* chunk) {
    return pimpl_->grab(desired_grab_count, chunk);
}
void        PagePool::release(uint32_t desired_release_count, PagePoolOffsetChunk *chunk) {
    pimpl_->release(desired_release_count, chunk);
}
PagePoolOffset PagePool::resolve_page(storage::Page *page) const {
    return pimpl_->resolve_page(page);
}
storage::Page* PagePool::resolve_offset(PagePoolOffset offset) const {
    return pimpl_->resolve_offset(offset);
}
storage::Page* PagePool::get_base_address() {
    return pimpl_->pool_base_;
}

}  // namespace memory
}  // namespace foedus
