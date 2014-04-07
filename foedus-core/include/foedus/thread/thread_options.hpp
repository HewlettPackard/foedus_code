/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_OPTIONS_HPP_
#define FOEDUS_THREAD_THREAD_OPTIONS_HPP_
#include <foedus/thread/thread_id.hpp>
#include <cstdint>
#include <iosfwd>
namespace foedus {
namespace thread {
/**
 * @brief Set of options about threads and thread-groups.
 * @ingroup THREAD
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct ThreadOptions {
    /**
     * Constructs option values with default values.
     */
    ThreadOptions();

    friend std::ostream& operator<<(std::ostream& o, const ThreadOptions& v);

    /**
     * Number of ThreadGroup in the engine.
     * Default value is hardware NUMA node count (::numa_num_configured_nodes()).
     */
    thread_group_id         group_count_;

    /**
     * Number of Thread in each ThreadGroup.
     * Default value is hardware NUMA core count;
     * ::numa_num_configured_cpus() / ::numa_num_configured_nodes().
     */
    thread_local_ordinal    thread_count_per_group_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_OPTIONS_HPP_
