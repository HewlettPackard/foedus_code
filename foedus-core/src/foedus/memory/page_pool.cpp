/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/memory/page_pool.hpp>
#include <foedus/memory/page_pool_pimpl.hpp>
namespace foedus {
namespace memory {
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

ErrorStack  PagePool::grab(uint32_t desired_grab_count, PagePoolOffsetChunk* chunk) {
    return pimpl_->grab(desired_grab_count, chunk);
}
void        PagePool::release(uint32_t desired_release_count, PagePoolOffsetChunk *chunk) {
    pimpl_->release(desired_release_count, chunk);
}

}  // namespace memory
}  // namespace foedus
