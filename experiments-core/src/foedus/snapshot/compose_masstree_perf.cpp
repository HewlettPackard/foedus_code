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
/**
 * @file foedus/snapshot/compose_masstree_perf.cpp
 * @brief Measures the performance of masstree composer
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
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/snapshot/fwd.hpp"
#include "foedus/snapshot/log_buffer.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"
#include "foedus/soc/shared_mutex.hpp"
#include "foedus/storage/composer.hpp"
#include "foedus/storage/partitioner.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_partitioner_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace snapshot {

DEFINE_bool(profile, false, "Whether to profile the execution with gperftools.");
DEFINE_bool(papi, false, "Whether to profile with PAPI.");

const uint64_t kRecords = 1 << 22;
const uint32_t kPayloadSize = 1 << 6;
const uint32_t kSnapshotId = 1;

void make_dummy_partitions(Engine* engine, storage::PartitionerMetadata* metadata);
void populate_logs(storage::StorageId id, char* buffer, uint64_t* size);

ErrorStack execute(
  Engine* engine,
  storage::StorageId id,
  double* elapsed_ms,
  std::vector<std::string>* papi_results) {
  storage::PartitionerMetadata* metadata = storage::PartitionerMetadata::get_metadata(engine, id);
  make_dummy_partitions(engine, metadata);

  storage::Composer composer(engine, id);
  cache::SnapshotFileSet dummy_files(engine);
  CHECK_ERROR(dummy_files.initialize());

  LOG(INFO) << "Allocating memories...";
  debugging::StopWatch alloc_watch;
  memory::AlignedMemory::AllocType kAlloc = memory::AlignedMemory::kNumaAllocOnnode;
  memory::AlignedMemory work_memory(1ULL << 23, 1U << 21, kAlloc, 0);
  memory::AlignedMemory root_page_memory(1ULL << 12, 1U << 12, kAlloc, 0);
  uint64_t full_size = kRecords * kPayloadSize * 2ULL;
  memory::AlignedMemory page_memory(full_size, 1U << 21, kAlloc, 0);
  memory::AlignedMemory intermediate_memory(1ULL << 24, 1U << 21, kAlloc, 0);
  memory::AlignedMemory log_memory(full_size, 1U << 21, kAlloc, 0);
  alloc_watch.stop();
  LOG(INFO) << "Allocated memories in " << alloc_watch.elapsed_ms() << "ms";

  LOG(INFO) << "Populating logs to process...";
  debugging::StopWatch log_watch;
  char* log_buffer = reinterpret_cast<char*>(log_memory.get_block());
  uint64_t log_size = 0;
  populate_logs(id, log_buffer, &log_size);
  InMemorySortedBuffer buffer(log_buffer, log_size);
  uint16_t key_len = sizeof(storage::masstree::KeySlice);
  buffer.set_current_block(id, kRecords, 0, log_size, key_len, key_len);
  log_watch.stop();
  LOG(INFO) << "Populated logs to process in " << log_watch.elapsed_ms() << "ms";

  SnapshotWriter writer(engine, 0, kSnapshotId, &page_memory, &intermediate_memory);
  CHECK_ERROR(writer.open());

  storage::Page* root_page = reinterpret_cast<storage::Page*>(root_page_memory.get_block());
  snapshot::SortedBuffer* log_masstree[1];
  log_masstree[0] = &buffer;
  storage::Composer::ComposeArguments args = {
    &writer,
    &dummy_files,
    log_masstree,
    1,
    &work_memory,
    Epoch(1),
    root_page
  };

  if (FLAGS_profile) {
    COERCE_ERROR(engine->get_debug()->start_profile("compose_experiment.prof"));
    engine->get_debug()->start_papi_counters();
  }

  LOG(INFO) << "experiment's main part has started";
  debugging::StopWatch watch;
  CHECK_ERROR(composer.compose(args));
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

  writer.close();
  CHECK_ERROR(dummy_files.uninitialize());
  return kRetOk;
}

void make_dummy_partitions(Engine* engine, storage::PartitionerMetadata* metadata) {
  soc::SharedMutexScope scope(&metadata->mutex_);  // protect this metadata
  ASSERT_ND(!metadata->valid_);
  metadata->allocate_data(
    engine,
    &scope,
    sizeof(storage::masstree::MasstreePartitionerData));
  storage::masstree::MasstreePartitionerData* data
    = reinterpret_cast<storage::masstree::MasstreePartitionerData*>(metadata->locate_data(engine));
  data->partition_count_ = 16;
  for (uint64_t i = 0; i < 16U; ++i) {
    data->low_keys_[i] = (1ULL << 60) * i;
    data->partitions_[i] = i;
  }
  metadata->valid_ = true;
}

void populate_logs(storage::StorageId id, char* buffer, uint64_t* size) {
  uint64_t cur = 0;
  char key[8];
  char payload[kPayloadSize];
  std::memset(payload, 0, kPayloadSize);
  for (uint64_t i = 0; i < kRecords; ++i) {
    storage::masstree::MasstreeInsertLogType* log
      = reinterpret_cast<storage::masstree::MasstreeInsertLogType*>(buffer + cur);
    std::memcpy(payload, &i, sizeof(i));
    assorted::write_bigendian<uint64_t>(i, key);
    log->populate(id, key, 8, payload, kPayloadSize);
    log->header_.xct_id_.set(1, 1);
    cur += log->header_.log_length_;
  }
  *size = cur;
}

int main_impl(int argc, char **argv) {
  gflags::SetUsageMessage("compose_masstree_perf");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fs::Path folder("/dev/shm/foedus_compose");
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

  options.snapshot_.folder_path_pattern_ = "/dev/shm/foedus_compose/snapshot/node_$NODE$";
  options.snapshot_.snapshot_interval_milliseconds_ = 100000000U;
  options.log_.folder_path_pattern_ = "/dev/shm/foedus_compose/log/node_$NODE$/logger_$LOGGER$";
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
      storage::masstree::MasstreeMetadata meta("aaa");
      storage::masstree::MasstreeStorage target;
      COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &target, &commit_epoch));
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
      << "pprof --pdf <process name> compose_experiment.prof > prof.pdf" << std::endl;
  }
  return 0;
}

}  // namespace snapshot
}  // namespace foedus

int main(int argc, char **argv) {
  return foedus::snapshot::main_impl(argc, argv);
}
