/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/memory/memory_options.hpp>
namespace foedus {
namespace memory {
MemoryOptions::MemoryOptions() {
    use_numa_alloc_ = true;
    interleave_numa_alloc_ = false;
    page_pool_size_mb_ = DEFAULT_PAGE_POOL_SIZE_MB;
}

ErrorStack MemoryOptions::load(tinyxml2::XMLElement* element) {
    EXTERNALIZE_LOAD_ELEMENT(element, use_numa_alloc_);
    EXTERNALIZE_LOAD_ELEMENT(element, interleave_numa_alloc_);
    EXTERNALIZE_LOAD_ELEMENT(element, page_pool_size_mb_);
    return RET_OK;
}

ErrorStack MemoryOptions::save(tinyxml2::XMLElement* element) const {
    CHECK_ERROR(insert_comment(element, "Set of options for memory manager"));

    EXTERNALIZE_SAVE_ELEMENT(element, use_numa_alloc_,
        "Whether to use ::numa_alloc_interleaved()/::numa_alloc_onnode() to allocate memories"
        " in NumaCoreMemory and NumaNodeMemory.\n"
        " If false, we use usual posix_memalign() instead.\n"
        " If everything works correctly, ::numa_alloc_interleaved()/::numa_alloc_onnode()\n"
        " should result in much better performance because each thread should access only\n"
        " the memories allocated for the NUMA node. Default is true..");
    EXTERNALIZE_SAVE_ELEMENT(element, interleave_numa_alloc_,
        "Whether to use ::numa_alloc_interleaved() instead of ::numa_alloc_onnode()\n"
        " If everything works correctly, numa_alloc_onnode() should result in much better\n"
        " performance because interleaving just wastes memory if it is very rare to access other\n"
        " node's memory. Default is false.\n"
        " If use_numa_alloc_ is false, this configuration has no meaning.");
    EXTERNALIZE_SAVE_ELEMENT(element, page_pool_size_mb_, "Total size of the page pool in MB");
    return RET_OK;
}

}  // namespace memory
}  // namespace foedus
