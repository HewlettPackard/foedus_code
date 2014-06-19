/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_DEBUGGING_STOP_WATCH_HPP_
#define FOEDUS_DEBUGGING_STOP_WATCH_HPP_
#include <stdint.h>
namespace foedus {
namespace debugging {
/**
 * @brief A high-resolution stop watch.
 * @ingroup DEBUGGING
 * @details
 * Stop-watch has some overhead, so use instantiate this too frequently
 * (per sec-ms: fine. per us: mmm. per ns: oh god).
 */
class StopWatch {
 public:
  StopWatch() : started_(0), stopped_(0) { start(); }

  /** Take current time tick. */
  void        start();

  /** Take another current time tick. Returns elapsed nanosec. */
  uint64_t    stop();

  uint64_t    elapsed_ns() const {
    return stopped_ - started_;
  }
  double      elapsed_us() const {
    return static_cast<double>(stopped_ - started_) / 1000.0;
  }
  double      elapsed_ms() const {
    return static_cast<double>(stopped_ - started_) / 1000000.0;
  }
  double      elapsed_sec() const {
    return static_cast<double>(stopped_ - started_) / 1000000000.0;
  }

 private:
  uint64_t started_;
  uint64_t stopped_;
};

}  // namespace debugging
}  // namespace foedus

#endif  // FOEDUS_DEBUGGING_STOP_WATCH_HPP_
