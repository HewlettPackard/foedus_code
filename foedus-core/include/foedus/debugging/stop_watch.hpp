/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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

  uint64_t    peek_elapsed_ns() const;

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
