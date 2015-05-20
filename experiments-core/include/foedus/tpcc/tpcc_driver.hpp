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
#ifndef FOEDUS_TPCC_TPCC_DRIVER_HPP_
#define FOEDUS_TPCC_TPCC_DRIVER_HPP_

#include <stdint.h>

#include <iosfwd>
#include <string>
#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/tpcc/fwd.hpp"
#include "foedus/tpcc/tpcc.hpp"

namespace foedus {
namespace tpcc {

/**
 * @brief Main class for TPCC benchmark.
 */
class TpccDriver {
 public:
  enum Constants {
    kMaxWorkers = 1024,
  };
  struct WorkerResult {
    uint32_t id_;
    uint64_t processed_;
    uint64_t user_requested_aborts_;
    uint64_t race_aborts_;
    uint64_t largereadset_aborts_;
    uint64_t unexpected_aborts_;
    uint64_t snapshot_cache_hits_;
    uint64_t snapshot_cache_misses_;
    friend std::ostream& operator<<(std::ostream& o, const WorkerResult& v);
  };
  struct Result {
    Result()
      : duration_sec_(0),
        worker_count_(0),
        processed_(0),
        user_requested_aborts_(0),
        race_aborts_(0),
        largereadset_aborts_(0),
        unexpected_aborts_(0),
        snapshot_cache_hits_(0),
        snapshot_cache_misses_(0) {}
    double   duration_sec_;
    uint32_t worker_count_;
    uint64_t processed_;
    uint64_t user_requested_aborts_;
    uint64_t race_aborts_;
    uint64_t largereadset_aborts_;
    uint64_t unexpected_aborts_;
    uint64_t snapshot_cache_hits_;
    uint64_t snapshot_cache_misses_;
    WorkerResult workers_[kMaxWorkers];
    std::vector<std::string> papi_results_;
    friend std::ostream& operator<<(std::ostream& o, const Result& v);
  };
  explicit TpccDriver(Engine* engine) : engine_(engine) {
  }

  Result run();

 private:
  Engine* const engine_;

  /** inclusive beginning of responsible wid. index=thread ordinal */
  std::vector<Wid>              from_wids_;
  /** exclusive end of responsible wid. index=thread ordinal */
  std::vector<Wid>              to_wids_;

  /** inclusive beginning of responsible iid. index=thread ordinal */
  std::vector<Iid>              from_iids_;
  /** exclusive end of responsible iid. index=thread ordinal */
  std::vector<Iid>              to_iids_;

  char ctime_buffer_[64];

  void assign_wids();
  void assign_iids();
};

int driver_main(int argc, char **argv);

/**
 * What this method does is VERY hacky.
 * This does manual binary replication because linux currently lacks user/kernel
 * text replication to NUMA nodes.
 */
void replicate_binaries(EngineOptions* options);

}  // namespace tpcc
}  // namespace foedus
#endif  // FOEDUS_TPCC_TPCC_DRIVER_HPP_
