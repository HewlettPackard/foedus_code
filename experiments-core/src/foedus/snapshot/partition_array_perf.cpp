/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
/**
 * @file foedus/snapshot/partition_array_perf.cpp
 * @brief Measures the performance of array partitioner (both sorting and partitioning)
 * @author kimurhid
 * @details
 */
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/log_buffer.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_partitioner_impl.hpp"
#include "foedus/storage/array/array_storage.hpp"

namespace foedus {
namespace snapshot {

DEFINE_bool(profile, false, "Whether to profile the execution with gperftools.");
DEFINE_bool(papi, false, "Whether to profile with PAPI.");
// usually, partitioning is negligible compared to sorting. so the default is sorting only
DEFINE_bool(run_partition, false, "Whether to test partitioning.");
DEFINE_bool(run_sort, true, "Whether to test sorting.");

const uint64_t kRecords = 1 << 24;
const uint16_t kPayloadSize = 1 << 6;

void make_dummy_partitions(
  Engine* engine,
  storage::StorageId id,
  storage::PartitionerMetadata* metadata);
void populate_logs(storage::StorageId id, char* buffer, BufferPosition* positions, uint64_t* size);

ErrorStack execute(
  Engine* engine,
  storage::StorageId id,
  double* elapsed_ms,
  std::vector<std::string>* papi_results) {
  storage::PartitionerMetadata* metadata = storage::PartitionerMetadata::get_metadata(engine, id);
  make_dummy_partitions(engine, id, metadata);

  LOG(INFO) << "Allocating memories...";
  debugging::StopWatch alloc_watch;
  memory::AlignedMemory::AllocType kAlloc = memory::AlignedMemory::kNumaAllocOnnode;
  memory::AlignedMemory work_memory(kRecords * 32ULL, 1U << 21, kAlloc, 0);
  memory::AlignedMemory positions_memory(sizeof(BufferPosition) * kRecords, 1U << 12, kAlloc, 0);
  memory::AlignedMemory out_memory(sizeof(BufferPosition) * kRecords, 1U << 12, kAlloc, 0);
  memory::AlignedMemory partitions_memory(sizeof(uint8_t) * kRecords, 1U << 12, kAlloc, 0);
  memory::AlignedMemory log_memory(kRecords * kPayloadSize * 2ULL, 1U << 21, kAlloc, 0);
  alloc_watch.stop();
  LOG(INFO) << "Allocated memories in " << alloc_watch.elapsed_ms() << "ms";

  LOG(INFO) << "Populating logs to process...";
  debugging::StopWatch log_watch;
  char* log_buffer = reinterpret_cast<char*>(log_memory.get_block());
  uint64_t log_size = 0;
  BufferPosition* log_positions = reinterpret_cast<BufferPosition*>(positions_memory.get_block());
  populate_logs(id, log_buffer, log_positions, &log_size);
  log_watch.stop();
  LOG(INFO) << "Populated logs to process in " << log_watch.elapsed_ms() << "ms";

  if (FLAGS_profile) {
    COERCE_ERROR(engine->get_debug()->start_profile("partition_experiment.prof"));
    engine->get_debug()->start_papi_counters();
  }

  LOG(INFO) << "experiment's main part has started";
  debugging::StopWatch watch;

  storage::Partitioner partitioner_base(engine, id);
  ASSERT_ND(partitioner_base.is_valid());
  storage::array::ArrayPartitioner partitioner(&partitioner_base);
  LogBuffer buf(log_buffer);
  if (FLAGS_run_partition) {
    LOG(INFO) << "running partitioning...";
    storage::Partitioner::PartitionBatchArguments partition_args = {
      0,
      buf,
      log_positions,
      kRecords,
      reinterpret_cast<uint8_t*>(partitions_memory.get_block())};
    partitioner.partition_batch(partition_args);
  }

  if (FLAGS_run_sort) {
    LOG(INFO) << "running sorting...";
    uint32_t written_count;
    storage::Partitioner::SortBatchArguments sort_args = {
      buf,
      log_positions,
      kRecords,
      8,
      8,
      &work_memory,
      Epoch(1),
      reinterpret_cast<BufferPosition*>(out_memory.get_block()),
      &written_count};
    partitioner.sort_batch(sort_args);
  }

  watch.stop();
  *elapsed_ms = watch.elapsed_ms();
  LOG(INFO) << "experiment's main part has ended. Took " << *elapsed_ms << "ms";

  if (FLAGS_profile) {
    engine->get_debug()->stop_profile();
    engine->get_debug()->stop_papi_counters();
    if (FLAGS_papi) {
      *papi_results = debugging::DebuggingSupports::describe_papi_counters(
        engine->get_debug()->get_papi_counters());
    }
  }

  return kRetOk;
}

void make_dummy_partitions(
  Engine* engine,
  storage::StorageId id,
  storage::PartitionerMetadata* metadata) {
  soc::SharedMutexScope scope(&metadata->mutex_);  // protect this metadata
  ASSERT_ND(!metadata->valid_);
  metadata->allocate_data(
    engine,
    &scope,
    sizeof(storage::array::ArrayPartitionerData));
  storage::array::ArrayStorage target(engine, id);
  storage::array::ArrayPartitionerData* data
    = reinterpret_cast<storage::array::ArrayPartitionerData*>(metadata->locate_data(engine));
  data->partitionable_ = true;
  data->array_levels_ = target.get_levels();
  data->array_size_ = kRecords;
  data->bucket_size_ = kRecords / 16;
  for (uint64_t i = 0; i < 16U; ++i) {
    data->bucket_owners_[i] = i;
  }
  metadata->valid_ = true;
}

void populate_logs(storage::StorageId id, char* buffer, BufferPosition* positions, uint64_t* size) {
  uint64_t cur = 0;
  char payload[kPayloadSize];
  std::memset(payload, 0, kPayloadSize);
  assorted::UniformRandom r(1234);
  for (uint64_t i = 0; i < kRecords; ++i) {
    positions[i] = cur / 8;
    storage::array::ArrayOverwriteLogType* log
      = reinterpret_cast<storage::array::ArrayOverwriteLogType*>(buffer + cur);
    std::memcpy(payload, &i, sizeof(i));
    uint64_t offset = r.next_uint32() % kRecords;
    log->populate(id, offset, payload, 0, kPayloadSize);
    log->header_.xct_id_.set(1, 1);
    cur += log->header_.log_length_;
  }
  *size = cur;
}

int main_impl(int argc, char **argv) {
  gflags::SetUsageMessage("partition_array_perf");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fs::Path folder("/dev/shm/foedus_partition");
  if (fs::exists(folder)) {
    fs::remove_all(folder);
  }
  if (!fs::create_directories(folder)) {
    std::cerr << "Couldn't create " << folder << ". err=" << assorted::os_error();
    return 1;
  }
  EngineOptions options;

  fs::Path savepoint_path(folder);
  savepoint_path /= "savepoint.xml";
  options.savepoint_.savepoint_path_.assign(savepoint_path.string());
  ASSERT_ND(!fs::exists(savepoint_path));

  options.snapshot_.folder_path_pattern_ = "/dev/shm/foedus_partition/snapshot/node_$NODE$";
  options.snapshot_.snapshot_interval_milliseconds_ = 100000000U;
  options.log_.folder_path_pattern_ = "/dev/shm/foedus_partition/log/node_$NODE$/logger_$LOGGER$";
  options.log_.loggers_per_node_ = 1;
  options.log_.flush_at_shutdown_ = false;
  options.thread_.group_count_ = 1;
  options.thread_.thread_count_per_group_ = 1;

  options.debugging_.debug_log_min_threshold_ = debugging::DebuggingOptions::kDebugLogInfo;
  options.debugging_.verbose_modules_ = "";
  options.debugging_.verbose_log_level_ = -1;

  double elapsed_ms = 0;
  std::vector<std::string> papi_results;
  {
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      Epoch commit_epoch;
      storage::array::ArrayMetadata meta("aaa", kRecords, kPayloadSize);
      storage::array::ArrayStorage target;
      COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &target, &commit_epoch));
      ASSERT_ND(target.exists());

      std::cout << "started!" << std::endl;
      COERCE_ERROR(execute(&engine, target.get_id(), &elapsed_ms, &papi_results));
      COERCE_ERROR(engine.uninitialize());
    }
  }

  std::cout << "elapsed time:" << elapsed_ms << "ms" << std::endl;
  std::cout << (kRecords / 1000) / elapsed_ms << " M logs/sec/core" << std::endl;
  if (FLAGS_profile) {
    if (FLAGS_papi) {
      std::cout << "PAPI results:" << std::endl;
      for (uint16_t i = 0; i < papi_results.size(); ++i) {
        std::cout << "  " << papi_results[i] << std::endl;
      }
    }
    std::cout << "Check out the prof: "
      << "pprof --pdf <process name> partition_experiment.prof > prof.pdf" << std::endl;
  }
  return 0;
}

}  // namespace snapshot
}  // namespace foedus

int main(int argc, char **argv) {
  return foedus::snapshot::main_impl(argc, argv);
}
