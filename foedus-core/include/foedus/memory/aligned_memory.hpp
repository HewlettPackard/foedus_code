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
 *
 * @par Moveable/Copiable
 * This object is \e moveable if C++11 is enabled. This object is \b NOT copiable, either via
 * constructor or assignment operator because it causes an issue on ownership of the memory.
 * Use move semantics to pass this object around.
 *
 * @section WHY Why aligned memory
 * There are a few cases where objects must be on aligned memory.
 *   \li When the memory might be used for Direct I/O (O_DIRECT).
 *   \li When the object needs alignment for \e regular or \e atomic memory access.
 * @see http://pubs.opengroup.org/onlinepubs/007904975/functions/posix_memalign.html
 * @see http://msdn.microsoft.com/en-us/library/windows/desktop/aa366887(v=vs.85).aspx
 *
 * @section THP Transparent Hugepage
 * We do not explicitly use hugepages (2MB/1GB OS page sizes) in our program, but we \b do allocate
 * memories in a way Linux is strongly advised to use transparent hugepages (THP).
 * posix_memalign() or numa_alloc_interleaved() with big allocation size is a clear hint for Linux
 * to use THP. You don't need mmap or madvise with this strong hint.
 *
 * After allocating the memory, we zero-clear the memory for two reasons.
 *  \li to avoid bugs caused by unintialized data access
 *  \li to immediately finalize memory allocation, which (with big allocation size) strongly advises
 * Linux to use THP.
 *
 * To check if THP is actually used, check /proc/meminfo before/after the engine start-up.
 * AnonHugePages tells it. We at least confirmed that THP is used in Fedora 19/20.
 * @see https://lwn.net/Articles/423584/
 *
 * @todo Support Windows' VirtualAlloc() and VirtualFree().
 */
class AlignedMemory CXX11_FINAL {
 public:
    /**
     * @brief Type of new/delete operation for the block.
     * @details
     * So far we allow posix_memalign and numa_alloc.
     * numa_alloc implicitly aligns the allocated memory, but we can't specify alignment size.
     * Usually it's 4096 bytes aligned, thus always enough for our usage.
     * @see http://stackoverflow.com/questions/8154162/numa-aware-cache-aligned-memory-allocation
     */
    enum AllocType {
        /** posix_memalign() and free(). */
        POSIX_MEMALIGN = 0,
        /** numa_alloc_interleaved() and numa_free(). Implicit 4096 bytes alignment. */
        NUMA_ALLOC_INTERLEAVED,
        /** numa_alloc_onnode() and numa_free(). Implicit 4096 bytes alignment.  */
        NUMA_ALLOC_ONNODE,
        /**
         * Usual new()/delete(). We currently don't use this for aligned memory allocation,
         * but may be the best for portability. But, this is not a strong hint for Linux to use
         * THP. hmm.
         */
        // NORMAL,
        /** Windows's VirtualAlloc() and VirtualFree(). */
        // VIRTUAL_ALLOC,
    };

    /** Empty constructor which allocates nothing. */
    AlignedMemory() CXX11_NOEXCEPT : size_(0), alignment_(0), alloc_type_(POSIX_MEMALIGN),
        numa_node_(0), block_(CXX11_NULLPTR) {}

    /**
     * Allocate an aligned memory of given size and alignment.
     * @param[in] size Byte size of the memory block. Actual allocation is at least of this size.
     * @param[in] alignment Alignment bytes of the memory block. Must be power of two.
     * Ignored for NUMA_ALLOC_ONNODE and NUMA_ALLOC_INTERLEAVED.
     * @param[in] alloc_type specifies type of new/delete
     * @param[in] numa_node if alloc_type_ is NUMA_ALLOC_ONNODE, the NUMA node to allocate at.
     * Otherwise ignored.
     * @attention When memory allocation fails for some reason (eg Out-Of-Memory), this constructor
     * does NOT fail nor throws an exception. Instead, it sets the block_ NULL.
     * So, the caller is responsible for checking it after construction.
     */
    AlignedMemory(size_t size, size_t alignment,
        AllocType alloc_type, int numa_node) CXX11_NOEXCEPT;

    // Disable default constructors
    AlignedMemory(const AlignedMemory &other) CXX11_FUNC_DELETE;
    AlignedMemory& operator=(const AlignedMemory &other) CXX11_FUNC_DELETE;

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
    /**
     * Move constructor that steals the memory block from other.
     */
    AlignedMemory(AlignedMemory &&other) noexcept;
    /**
     * Move assignment operator that steals the memory block from other.
     */
    AlignedMemory& operator=(AlignedMemory &&other) noexcept;
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

    /** Automatically releases the memory. */
    ~AlignedMemory() { release_block(); }

    /** Returns the memory block. */
    void*       get_block() const { return block_; }
    /** Returns if this object doesn't hold a valid memory block. */
    bool        is_null() const { return block_ == CXX11_NULLPTR; }
    /** Returns the byte size of the memory block. */
    size_t      get_size() const { return size_; }
    /** Returns the alignment of the memory block. */
    size_t      get_alignment() const { return alignment_; }
    /** Returns type of new/delete operation for the block. */
    AllocType   get_alloc_type() const { return alloc_type_; }
    /** If alloc_type_ is NUMA_ALLOC_ONNODE, returns the NUMA node this memory was allocated at. */
    int         get_numa_node() const { return numa_node_; }

    /** Releases the memory block. */
    void        release_block();

    friend std::ostream&    operator<<(std::ostream& o, const AlignedMemory& v);

 private:
    /** Byte size of the memory block. */
    size_t      size_;
    /** Alignment of the memory block. */
    size_t      alignment_;
    /** type of new/delete operation for the block .*/
    AllocType   alloc_type_;
    /** if alloc_type_ is NUMA_ALLOC_ONNODE, the NUMA node this memory was allocated at. */
    int         numa_node_;
    /** Allocated memory block. */
    void*       block_;
};

}  // namespace memory
}  // namespace foedus

#endif  // FOEDUS_MEMORY_ALIGNED_MEMORY_HPP_
