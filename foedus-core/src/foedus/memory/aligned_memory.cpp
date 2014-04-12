/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/memory/aligned_memory.hpp>
#include <numa.h>
#include <cassert>
#include <cstdlib>
#include <ostream>

namespace foedus {
namespace memory {
AlignedMemory::AlignedMemory(size_t size, size_t alignment,
                             AllocType alloc_type, int numa_node) noexcept
    : size_(size), alignment_(alignment), alloc_type_(alloc_type), numa_node_(numa_node) {
    block_ = nullptr;
    if (size_ % alignment != 0) {
        size_ = ((size_ / alignment) + 1) * alignment;
    }
    switch (alloc_type_) {
        case POSIX_MEMALIGN:
            ::posix_memalign(&block_, alignment, size_);
            break;
        case NUMA_ALLOC_INTERLEAVED:
            block_ = ::numa_alloc_interleaved(size_);
            break;
        case NUMA_ALLOC_ONNODE:
            block_ = ::numa_alloc_onnode(size_, numa_node);
            break;
        default:
            assert(false);
    }

    ::memset(block_, 0, size_);  // see class comment for why we do this immediately
}

AlignedMemory::AlignedMemory(AlignedMemory &&other) noexcept : block_(nullptr) {
    *this = std::move(other);
}
AlignedMemory& AlignedMemory::operator=(AlignedMemory &&other) noexcept {
    release_block();
    size_ = other.size_;
    alignment_ = other.alignment_;
    alloc_type_ = other.alloc_type_;
    block_ = other.block_;
    other.block_ = nullptr;
    return *this;
}

void AlignedMemory::release_block() {
    if (block_ != nullptr) {
        switch (alloc_type_) {
            case POSIX_MEMALIGN:
                ::free(block_);
                break;
            case NUMA_ALLOC_INTERLEAVED:
            case NUMA_ALLOC_ONNODE:
                ::numa_free(block_, size_);
                break;
            default:
                assert(false);
        }
        block_ = nullptr;
    }
}

std::ostream& operator<<(std::ostream& o, const AlignedMemory& v) {
    o << "AlignedMemory" << std::endl;
    o << "  is_null = " << v.is_null() << std::endl;
    o << "  size = " << v.get_size() << std::endl;
    o << "  alignment = " << v.get_alignment() << std::endl;
    o << "  alloc_type_ = " << v.get_alloc_type() << std::endl;
    o << "  numa_node_ = " << v.get_numa_node() << std::endl;
    return o;
}

}  // namespace memory
}  // namespace foedus

