/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/debugging/stop_watch.hpp>
#include <chrono>
namespace foedus {
namespace debugging {

uint64_t get_now_nanosec() {
    std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

void StopWatch::start() {
    started_ = get_now_nanosec();
    stopped_ = started_;
}

uint64_t StopWatch::stop() {
    stopped_ = get_now_nanosec();
    return elapsed_ns();
}

}  // namespace debugging
}  // namespace foedus
