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

#ifndef FOEDUS_YCSB_YCSB_HPP_
#define FOEDUS_YCSB_YCSB_HPP_

#include <stdint.h>

#include <atomic>
#include <cstring>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/fwd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/soc/shared_rendezvous.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"

namespace foedus {
namespace ycsb {

/* Number of bytes in each field */
const uint32_t kFieldLength = 100;

/* Number of fields per record */
const uint32_t kFields = 10;

/* Record size */
const uint64_t kRecordSize = kFieldLength * kFields;

// TODO: make it a cmd argument
const uint64_t kInitialUserTableSize= 10000;

const uint32_t kMaxWorkers = 1024;

// The YCSB paper didn't say key length, assume it's 64-bit for simplicity
typedef uint64_t YcsbKey;

struct YcsbRecord {
  char data_[kFieldLength][kFields];
  YcsbRecord(char value);
  YcsbRecord() {}
};

/**
 * Channel between the driver process/thread and clients process/thread.
 * If the driver spawns client processes, this is allocated in shared memory.
 * This ``channel'' controls/synchoronizes worker threads.
 */
struct YcsbClientChannel {
  void initialize() {
    start_rendezvous_.initialize();
    exit_nodes_.store(0);
    stop_flag_.store(false);
  }
  void uninitialize() {
    start_rendezvous_.uninitialize();
  }
  soc::SharedRendezvous start_rendezvous_;
  std::atomic<uint16_t> exit_nodes_;
  std::atomic<bool> stop_flag_;
};

extern YcsbKey next_key;

int driver_main(int argc, char **argv);
ErrorStack ycsb_load_task(const proc::ProcArguments& args);
ErrorStack ycsb_client_task(const proc::ProcArguments& args);
YcsbClientChannel* get_channel(Engine* engine);

class YcsbLoadTask {
 public:
  YcsbLoadTask() {}
  ErrorStack run(thread::Thread* context);
 private:
  assorted::UniformRandom rnd_;
};

class YcsbDriver {
 public:
  YcsbDriver(Engine* engine) : engine_(engine) {}
  ErrorStack run();

 private:
  Engine* engine_;
};

struct YcsbWorkload {
  YcsbWorkload(
    char desc,
    uint8_t read_percent,
    uint8_t update_percent,
    uint8_t insert_percent,
    uint8_t scan_percent)
    : desc_(desc),
      read_percent_(read_percent),
      update_percent_(update_percent),
      insert_percent_(insert_percent),
      scan_percent_(scan_percent) {}

  YcsbWorkload() {}

  char desc_;
  // Cumulative percentage of r/u/i/s. From read...scan the percentages
  // accumulates, e.g., r=5, u=12 => we'll have 12-5=7% of updates in total.
  uint8_t read_percent_;
  uint8_t update_percent_;
  uint8_t insert_percent_;
  uint8_t scan_percent_;
};

class YcsbClientTask {
 public:
  struct Inputs {
    uint32_t worker_id_;
    YcsbWorkload workload_;
    Inputs() {}
  };

  // Result of each worker
  struct Outputs {
    uint32_t id_;
    uint64_t processed_;
    uint64_t user_requested_aborts_;
    uint64_t race_aborts_;
    uint64_t largereadset_aborts_;
    uint64_t unexpected_aborts_;
    uint64_t snapshot_cache_hits_;
    uint64_t snapshot_cache_misses_;
    friend std::ostream& operator<<(std::ostream& o, const Outputs& v);
  };

  YcsbClientTask(const Inputs *inputs)
    : worker_id_(inputs->worker_id_),
      workload_(inputs->workload_) {}

  ErrorStack run(thread::Thread* context);

  bool is_stop_requested() const {
    assorted::memory_fence_acquire();
    return channel_->stop_flag_.load();
  }

 private:
  thread::Thread* context_;
  uint32_t worker_id_;
  YcsbWorkload workload_;

  Engine* engine_;
  xct::XctManager* xct_manager_;
  storage::masstree::MasstreeStorage user_table_;
  YcsbClientChannel *channel_;
  assorted::UniformRandom rnd_;  // TODO: add zipfian etc.

  ErrorStack do_xct(YcsbWorkload workload_desc);
  ErrorStack do_read(YcsbKey key);
  ErrorStack do_update(YcsbKey key);
  ErrorStack do_insert(YcsbKey key);
  ErrorStack do_scan(YcsbKey start_key, uint64_t nrecs);
};

// Total, summary of all workers
struct YcsbResult {
  YcsbResult()
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
  YcsbClientTask::Outputs workers_[kMaxWorkers];
  std::vector<std::string> papi_results_;
  friend std::ostream& operator<<(std::ostream& o, const YcsbResult& v);
};

}  // namespace ycsb
}  // namespace foedus

#endif
