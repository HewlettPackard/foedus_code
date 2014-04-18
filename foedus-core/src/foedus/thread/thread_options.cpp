/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/thread/thread_options.hpp>
#include <numa.h>
#include <ostream>
namespace foedus {
namespace thread {
ThreadOptions::ThreadOptions() {
    group_count_ = ::numa_num_configured_nodes();
    if (group_count_ == 0) {
        group_count_ = 1;
    }

    int total_cores = numa_num_configured_cpus();
    if (total_cores == 0) {
        total_cores = 1;
    }
    thread_count_per_group_ = total_cores / group_count_;
}

std::ostream& operator<<(std::ostream& o, const ThreadOptions& v) {
    o << "  <ThreadOptions>" << std::endl;
    EXTERNALIZE_WRITE(group_count_);
    EXTERNALIZE_WRITE(thread_count_per_group_);
    o << "  </ThreadOptions>" << std::endl;
    return o;
}

}  // namespace thread
}  // namespace foedus
