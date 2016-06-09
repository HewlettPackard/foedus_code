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
 * @brief A simplistic test to verify the NVDIMM feature.
 * @details
 * Consists of two runs.
 * The first run populates a storage with one record, then intentionally gets stack.
 * We kill the machine in the meantime.
 * Then, after reboot, the second run reads the popolated storage.
 *
 * @see http://linux.hpe.com/nvdimm/
 * @see http://linux.hpe.com/nvdimm/LinuxSDKReadme.htm
 */

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace nvdimm {

// Note: Create this folder upfront AND set permission to read-writable for yourself.
// Otherwise you need to run this program as sudo:
// sudo rm -rf /mnt/pmem0/foedus_test
// sudo mkdir -p /mnt/pmem0/foedus_test
// sudo chmod 777 /mnt/pmem0/foedus_test
const fs::Path kMountPoint(std::string("/mnt/pmem0/foedus_test"));

const storage::StorageName kStorageName("aaa");

ErrorStack the_task(const proc::ProcArguments& args) {
  std::cout << "==================================================" << std::endl;
  std::cout << "=====   NOW the task starts!" << std::endl;
  std::cout << "==================================================" << std::endl << std::flush;
  auto* engine = args.engine_;
  auto* str_manager = engine->get_storage_manager();
  auto* xct_manager = engine->get_xct_manager();
  constexpr uint16_t kRecords = 16;
  storage::array::ArrayStorage the_storage(engine, kStorageName);
  Epoch commit_epoch;
  if (the_storage.exists()) {
    std::cout << "Ok, the storage exists. This is after recovery." << std::endl;

    WRAP_ERROR_CODE(xct_manager->begin_xct(args.context_, xct::kSerializable));
    for (uint16_t i = 0; i < kRecords; ++i) {
      uint64_t data = 0;
      WRAP_ERROR_CODE(the_storage.get_record_primitive<uint64_t>(args.context_, i, &data, 0));
      std::cout << "Record-" << i << "=" << data << std::endl;
    }
    WRAP_ERROR_CODE(xct_manager->precommit_xct(args.context_, &commit_epoch));
    std::cout << "Read the record. done!" << std::endl;
  } else {
    std::cout << "the storage doesn't exist. This must be the initial run." << std::endl;
    storage::array::ArrayMetadata meta(kStorageName, sizeof(uint64_t), 1024);
    CHECK_ERROR(str_manager->create_array(&meta, &the_storage, &commit_epoch));
    ASSERT_ND(the_storage.exists());
    std::cout << "Created the storage. making it durable..." << std::endl;
    // WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));

    std::cout << "Made the storage durable. Populating it..." << std::endl;
    WRAP_ERROR_CODE(xct_manager->begin_xct(args.context_, xct::kSerializable));
    for (uint16_t i = 0; i < kRecords; ++i) {
      WRAP_ERROR_CODE(the_storage.overwrite_record_primitive<uint64_t>(
        args.context_,
        i,
        i + 42U,
        0));
    }
    WRAP_ERROR_CODE(xct_manager->precommit_xct(args.context_, &commit_epoch));

    std::cout << "Populated the storage. making it durable..." << std::endl;
    WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));

    std::cout << "Made it durable. Now we go into sleep. Kill the machine or Ctrl-C now!"
      << std::endl << std::flush;
    std::this_thread::sleep_for(std::chrono::seconds(60000));
    std::cerr << "Wait, why are we here!" << std::endl;
  }

  return kRetOk;
}

int main_impl(int /*argc*/, char **/*argv*/) {
  std::cout << "hey yo. starting the test program..." << std::endl << std::flush;
  EngineOptions options;
  options.thread_.group_count_ = 1;
  options.thread_.thread_count_per_group_ = 1;
  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogWarning;

  // "tiny" options
  options.log_.log_buffer_kb_ = 1 << 8;
  options.memory_.page_pool_size_mb_per_node_ = 4;
  options.memory_.private_page_pool_initial_grab_ = 32;
  options.cache_.snapshot_cache_size_mb_per_node_ = 2;
  options.cache_.private_snapshot_cache_initial_grab_ = 32;
  options.snapshot_.snapshot_interval_milliseconds_ = 1 << 26;  // never
  options.snapshot_.log_mapper_io_buffer_mb_ = 2;
  options.snapshot_.log_reducer_buffer_mb_ = 2;
  options.snapshot_.log_reducer_dump_io_buffer_mb_ = 2;
  options.snapshot_.snapshot_writer_page_pool_size_mb_ = 2;
  options.snapshot_.snapshot_writer_intermediate_pool_size_mb_ = 2;
  options.storage_.max_storages_ = 128;

  // Savepoint/xlog/snapshot must be persistent.
  fs::Path log_folder = kMountPoint;
  log_folder /= "foedus_xlogs/node_$NODE$/logger_$LOGGER$";
  options.log_.folder_path_pattern_.assign(log_folder.string());

  fs::Path snapshot_folder = kMountPoint;
  snapshot_folder /= "foedus_snapshots/node_$NODE$";
  options.snapshot_.folder_path_pattern_.assign(snapshot_folder.string());

  fs::Path savepoint_file = kMountPoint;
  savepoint_file /= "foedus_savepoints.xml";
  options.savepoint_.savepoint_path_.assign(savepoint_file.string());

  // Debug log doesn't have to be persistent
  options.debugging_.debug_log_dir_.assign(std::string("/dev/shm/foedus_debuglog/"));

  Engine engine(options);
  engine.get_proc_manager()->pre_register("the_task", the_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    ErrorStack ret = engine.get_thread_pool()->impersonate_synchronous("the_task");
    COERCE_ERROR(ret);
    COERCE_ERROR(engine.uninitialize());
  }

  return 0;
}

}  // namespace nvdimm
}  // namespace foedus

int main(int argc, char **argv) {
  return foedus::nvdimm::main_impl(argc, argv);
}
