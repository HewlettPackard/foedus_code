/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/thread/thread_options.hpp>
#include <numa.h>
#include <glog/logging.h>
#include <thread>
namespace foedus {
namespace thread {
ThreadOptions::ThreadOptions() {
    group_count_ = ::numa_num_configured_nodes();
    LOG(INFO) << "ThreadOptions(): numa_num_configured_nodes=" << group_count_;
    if (group_count_ == 0) {
        LOG(WARNING) << "ThreadOptions(): numa_num_configured_nodes returned 0. Using 1 instead";
        group_count_ = 1;
    }

    int total_cores = numa_num_configured_cpus();
    LOG(INFO) << "ThreadOptions(): numa_num_configured_cpus=" << total_cores;
    if (total_cores == 0) {
        LOG(WARNING) << "ThreadOptions(): numa_num_configured_cpus() returned 0. Using 1 instead";
        total_cores = 1;
    }
    thread_count_per_group_ = total_cores / group_count_;
}
}  // namespace thread
}  // namespace foedus

std::ostream& operator<<(std::ostream& o, const foedus::thread::ThreadOptions& v) {
    o << "Thread options:" << std::endl;
    o << "  group_count_=" << static_cast<int>(v.group_count_) << std::endl;
    o << "  thread_count_per_group_=" << static_cast<int>(v.thread_count_per_group_) << std::endl;
    return o;
}
