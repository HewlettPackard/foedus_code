/*
 * Copyright (c), Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/memory/aligned_memory.hpp>
#include <cstdlib>

namespace foedus { namespace memory {

AlignedMemory::AlignedMemory(size_t size, size_t alignment)
    : size_(size), alignment_(alignment) {
    block_ = NULL;
    if (size_ % alignment != 0) {
        size_ = ((size_ / alignment) + 1) * alignment;
    }
    ::posix_memalign(&block_, alignment, size_);
}

AlignedMemory::AlignedMemory(AlignedMemory &&other) {
    size_ = other.size_;
    alignment_ = other.alignment_;
    block_ = other.block_;
    other.block_ = NULL;
}

AlignedMemory::~AlignedMemory() {
    if (block_ != NULL) {
        ::free(block_);
        block_ = NULL;
    }
}

}  // namespace memory
}  // namespace foedus
