/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
/**
 * @file foedus/storage/array/readonly_experiment.cpp
 * @brief Read-only uniform-random accesses on array storage
 * @author kimurhid
 * @date 2014/04/14
 * @details
 * This is the first experiment to see the speed-of-light, where we use the simplest
 * data structure, array, and run read-only queries. This is supposed to run VERY fast.
 * Actually, we observed more than 400 MQPS in a desktop machine if the payload is small (16 bytes).
 *
 * @section ENVIRONMENTS Environments
 * At least 1GB of available RAM.
 *
 * @section OTHER Other notes
 * No special steps to build/run this expriment. This is self-contained.
 *
 * @section RESULTS Latest Results
 * foedus_results/20140414_kimurhid_array_readonly
 * foedus_results/20140619_kimurhid_array_readonly
 *
 * @todo kPayload/kDurationMicro/kRecords are so far hard-coded constants, not program arguments.
 */
#include <unistd.h>
#include <sys/mman.h>

#include <atomic>
#include <iostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace array {


const uint16_t kPayload = 16;  // = 128;
const uint64_t kDurationMicro = 10000000;
const uint32_t kRecords = 1 << 19;  // 1 << 20;
const uint32_t kRecordsMask = 0x7FFFF;  // 0xFFFFF;

bool start_req = false;
bool stop_req = false;
ArrayStorage *storage = nullptr;

class ReadTask : public thread::ImpersonateTask {
 public:
  ReadTask() {}
  ErrorStack run(thread::Thread* context) {
    Engine *engine = context->get_engine();
    const xct::IsolationLevel isolation = xct::kDirtyReadPreferVolatile;
    // const xct::IsolationLevel isolation = xct::kSerializable;
    CHECK_ERROR(engine->get_xct_manager().begin_xct(context, isolation));
    Epoch commit_epoch;

    // pre-calculate random numbers to get rid of random number generation as bottleneck
    random_.set_current_seed(context->get_thread_id());
    CHECK_ERROR(
      context->get_thread_memory()->get_node_memory()->allocate_numa_memory(
        kRandomCount * sizeof(uint32_t), &numbers_));
    random_.fill_memory(&numbers_);
    const uint32_t *randoms = reinterpret_cast<const uint32_t*>(numbers_.get_block());
    while (!start_req) {
      std::atomic_thread_fence(std::memory_order_acquire);
    }

    ArrayStorage *array = dynamic_cast<ArrayStorage*>(storage);
    char buf[kPayload];
    processed_ = 0;
    while (true) {
      uint64_t id = randoms[processed_ & 0xFFFF] & kRecordsMask;
      WRAP_ERROR_CODE(array->get_record(context, id, buf, 0, kPayload));
      ++processed_;
      if ((processed_ & 0xFFFF) == 0) {
        CHECK_ERROR(engine->get_xct_manager().precommit_xct(context, &commit_epoch));
        CHECK_ERROR(engine->get_xct_manager().begin_xct(context, isolation));
        std::atomic_thread_fence(std::memory_order_acquire);
        if (stop_req) {
          break;
        }
      }
    }

    CHECK_ERROR(engine->get_xct_manager().precommit_xct(context, &commit_epoch));
    numbers_.release_block();
    std::cout << "I'm done! " << context->get_thread_id()
      << ", processed=" << processed_ << std::endl;
    return kRetOk;
  }

  memory::AlignedMemory numbers_;
  assorted::UniformRandom random_;
  uint64_t processed_;
  const uint32_t kRandomCount = 1 << 19;
  const uint32_t kRandomCountMod = 0x7FFFF;
};

int main_impl(int argc, char **argv) {
  bool profile = false;
  if (argc >= 2 && std::string(argv[1]) == "--profile") {
    profile = true;
    std::cout << "Profiling..." << std::endl;
  }
  fs::remove_all(fs::Path("logs"));
  fs::remove_all(fs::Path("snapshots"));
  fs::remove(fs::Path("savepoint.xml"));
  EngineOptions options;
  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogWarning;
  const int kThreads = options.thread_.group_count_ * options.thread_.thread_count_per_group_;
  {
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      Epoch commit_epoch;
      ArrayMetadata meta("aaa", kPayload, kRecords);
      COERCE_ERROR(engine.get_storage_manager().create_array(&meta, &storage, &commit_epoch));

      typedef ReadTask* TaskPtr;
      TaskPtr* tasks = new TaskPtr[kThreads];
      thread::ImpersonateSession sessions[kThreads];
      for (int i = 0; i < kThreads; ++i) {
        tasks[i] = new ReadTask();
        sessions[i] = engine.get_thread_pool().impersonate(tasks[i]);
        if (!sessions[i].is_valid()) {
          COERCE_ERROR(sessions[i].invalid_cause_);
        }
      }
      ::usleep(1000000);
      start_req = true;
      std::atomic_thread_fence(std::memory_order_release);
      if (profile) {
        COERCE_ERROR(engine.get_debug().start_profile("readonly_experiment.prof"));
        engine.get_debug().start_papi_counters();
      }
      std::cout << "all started!" << std::endl;
      ::usleep(kDurationMicro);
      stop_req = true;
      std::atomic_thread_fence(std::memory_order_release);

      uint64_t total = 0;
      std::atomic_thread_fence(std::memory_order_acquire);
      for (int i = 0; i < kThreads; ++i) {
        total += tasks[i]->processed_;
      }
      if (profile) {
        engine.get_debug().stop_profile();
        engine.get_debug().stop_papi_counters();
      }

      for (int i = 0; i < kThreads; ++i) {
        std::cout << "session: result[" << i << "]="
          << sessions[i].get_result() << std::endl;
        delete tasks[i];
      }

      auto papi_results = debugging::DebuggingSupports::describe_papi_counters(
        engine.get_debug().get_papi_counters());
      for (uint16_t i = 0; i < papi_results.size(); ++i) {
        std::cout << papi_results[i] << std::endl;
      }
      delete[] tasks;
      std::cout << "total=" << total << ", MQPS="
        << (static_cast<double>(total)/kDurationMicro) << std::endl;
      COERCE_ERROR(engine.uninitialize());
    }
  }

  return 0;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus

int main(int argc, char **argv) {
  return foedus::storage::array::main_impl(argc, argv);
}
