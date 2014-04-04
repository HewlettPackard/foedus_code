/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_ALIGNED_MEMORY_HPP_
#define FOEDUS_MEMORY_ALIGNED_MEMORY_HPP_

#include <foedus/cxx11.hpp>
#include <cstddef>
#include <iosfwd>
namespace foedus {
namespace memory {
/**
 * @brief Represents one memory block aligned to actual OS/hardware pages.
 * @ingroup MEMORY
 * @details
 * This class is used to allocate and hold memory blocks for objects that must be aligned.
 * There are a few cases where objects must be on aligned memory.
 *   \li When the memory might be used for Direct I/O (O_DIRECT).
 *   \li When the object needs alignment for \e regular or \e atomic memory access.
 * This object is \e moveable if C++11 is enabled.
 * @see http://pubs.opengroup.org/onlinepubs/007904975/functions/posix_memalign.html
 * @see http://msdn.microsoft.com/en-us/library/windows/desktop/aa366887(v=vs.85).aspx
 */
class AlignedMemory {
 public:
    /**
     * Allocate an aligned memory of given size and alignment.
     * @param[in] size Byte size of the memory block
     * @param[in] alignment Alignment bytes of the memory block
     * @attention When memory allocation fails for some reason (eg Out-Of-Memory), this constructor
     * does NOT fail nor throws an exception. Instead, it sets the block_ NULL.
     * So, the caller is responsible for checking it after construction.
     */
    AlignedMemory(size_t size, size_t alignment);

    /** non-move copy constructor is disabled as it causes ownership issue. */
    AlignedMemory(const AlignedMemory &other) CXX11_FUNC_DELETE;

    /** non-move assign operator is disabled as it causes ownership issue. */
    AlignedMemory* operator=(const AlignedMemory &other) CXX11_FUNC_DELETE;

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
    /**
     * Move constructor that steals the memory block from other.
     */
    AlignedMemory(AlignedMemory &&other);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

    /** Automatically releases the memory. */
    ~AlignedMemory();

    /** Returns the memory block. */
    void*   get_block() const { return block_; }
    /** Returns if this object doesn't hold a valid memory block. */
    bool    is_null() const { return block_ == NULL; }
    /** Returns the byte size of the memory block. */
    size_t  get_size() const { return size_; }
    /** Returns the alignment of the memory block. */
    size_t  get_alignment() const { return alignment_; }

    friend std::ostream&    operator<<(std::ostream& o, const AlignedMemory& v);

 private:
    /** Byte size of the memory block. */
    size_t  size_;
    /** Alignment of the memory block. */
    size_t  alignment_;
    /** Allocated memory block. */
    void*   block_;
};

}  // namespace memory
}  // namespace foedus

#endif  // FOEDUS_MEMORY_ALIGNED_MEMORY_HPP_
