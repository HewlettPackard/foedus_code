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
#include "foedus/storage/hash/hash_storage.hpp"
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

const int kKeyPrefixLength = 4; // "user" without \0
const assorted::FixedString<kKeyPrefixLength> kKeyPrefix("user");

const int32_t kKeyMaxLength = kKeyPrefixLength + 32; // 4 bytes of char "user" + 32 chars for numbers
class YcsbKey {
 private:
  char data_[kKeyMaxLength];
  uint32_t size_;

 public:
  YcsbKey();
  YcsbKey next(uint32_t worker_id, uint32_t* local_counter);
  YcsbKey build(uint32_t high_bits, uint32_t low_bits);

  const char *ptr() {
    return data_;
  }

  uint32_t size() {
    return size_;
  }
};

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
class YcsbClientTask;
struct YcsbClientChannel {
  void initialize() {
    start_rendezvous_.initialize();
    exit_nodes_.store(0);
    stop_flag_.store(false);
    nr_workers_ = 0;
    workers_mutex_.initialize();
  }
  void uninitialize() {
    start_rendezvous_.uninitialize();
    workers_mutex_.uninitialize();
  }
  uint32_t peek_local_key_counter(uint32_t worker_id);

  soc::SharedRendezvous start_rendezvous_;
  std::atomic<uint16_t> exit_nodes_;
  std::atomic<bool> stop_flag_;
  uint32_t nr_workers_;
  soc::SharedMutex workers_mutex_;
  std::vector<YcsbClientTask> workers;
};

int driver_main(int argc, char **argv);
ErrorStack ycsb_load_task(const proc::ProcArguments& args);
ErrorStack ycsb_client_task(const proc::ProcArguments& args);
YcsbClientChannel* get_channel(Engine* engine);
int64_t max_scan_length();

struct YcsbWorkload {
  YcsbWorkload(
    char desc,
    uint8_t insert_percent,
    uint8_t read_percent,
    uint8_t update_percent,
    uint8_t scan_percent)
    : desc_(desc),
      insert_percent_(insert_percent),
      read_percent_(read_percent),
      update_percent_(update_percent),
      scan_percent_(scan_percent) {}

  YcsbWorkload() {}

  char desc_;
  // Cumulative percentage of i/r/u/s. From insert...scan the percentages
  // accumulates, e.g., i=5, r=12 => we'll have 12-5=7% of reads in total.
  uint8_t insert_percent_;
  uint8_t read_percent_;
  uint8_t update_percent_;
  uint8_t scan_percent_;
};

class YcsbLoadTask {
 public:
  YcsbLoadTask() {}
  ErrorStack run(thread::Thread* context, const uint32_t nr_workers,
    std::pair<uint32_t, uint32_t>* start_key_pair);
 private:
  assorted::UniformRandom rnd_;
};

class YcsbClientTask {
 public:
  enum Constants {
    kRandomSeed = 123456,
  };

  struct Inputs {
    uint32_t worker_id_;
    YcsbWorkload workload_;
    bool read_all_fields_;
    uint32_t local_key_counter_;
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

  YcsbClientTask(const Inputs& inputs, Outputs* outputs)
    : worker_id_(inputs.worker_id_),
      workload_(inputs.workload_),
      read_all_fields_(inputs.read_all_fields_),
      outputs_(outputs),
      local_key_counter_(inputs.local_key_counter_),
      rnd_(kRandomSeed + inputs.worker_id_) {}

  ErrorStack run(thread::Thread* context);

  bool is_stop_requested() const {
    assorted::memory_fence_acquire();
    return channel_->stop_flag_.load();
  }

  uint32_t local_key_counter() {return local_key_counter_; }  // Not accurate!
  uint32_t get_worker_id() const { return worker_id_; }

 private:
  thread::Thread* context_;
  uint32_t worker_id_;
  YcsbWorkload workload_;
  bool read_all_fields_;
  Outputs* outputs_;
  uint32_t local_key_counter_;
  YcsbKey key_arena_; // Don't use this from other threads!

  Engine* engine_;
  xct::XctManager* xct_manager_;
#ifdef YCSB_HASH_STORAGE
  storage::hash::HashStorage user_table_;
#else
  storage::masstree::MasstreeStorage user_table_;
#endif
  YcsbClientChannel *channel_;
  assorted::UniformRandom rnd_;  // TODO: add zipfian etc.

  YcsbKey next_insert_key() {
    return key_arena_.next(worker_id_, &local_key_counter_);
  };

  YcsbKey build_key(uint32_t high_bits, uint32_t low_bits) {
    return key_arena_.build(high_bits, low_bits);
  }

  uint32_t get_user_requested_aborts() const { return outputs_->user_requested_aborts_; }
  uint32_t increment_user_requested_aborts() { return ++outputs_->user_requested_aborts_; }
  uint32_t get_race_aborts() const { return outputs_->race_aborts_; }
  uint32_t increment_race_aborts() { return ++outputs_->race_aborts_; }
  uint32_t get_unexpected_aborts() const { return outputs_->unexpected_aborts_; }
  uint32_t increment_unexpected_aborts() { return ++outputs_->unexpected_aborts_; }
  uint32_t get_largereadset_aborts() const { return outputs_->largereadset_aborts_; }
  uint32_t increment_largereadset_aborts() { return ++outputs_->largereadset_aborts_; }

  ErrorStack do_xct(YcsbWorkload workload_desc);
  ErrorCode do_read(YcsbKey key);
  ErrorCode do_update(YcsbKey key);
  ErrorCode do_insert(YcsbKey key);
#ifndef YCSB_HASH_STORAGE
  ErrorCode do_scan(YcsbKey start_key, uint64_t nrecs);
#endif
};

class YcsbDriver {
 public:
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
  // Total, summary of all workers
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

  YcsbDriver(Engine* engine) : engine_(engine) {}
  ErrorStack run();

 private:
  Engine* engine_;
};
}  // namespace ycsb
}  // namespace foedus

#endif
