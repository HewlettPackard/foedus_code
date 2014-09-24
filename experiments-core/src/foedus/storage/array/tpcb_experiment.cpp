/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
/**
 * @file foedus/storage/array/tpcb_experiment.cpp
 * @brief TPC-B experiment on array storage
 * @author kimurhid
 * @date 2014/05/07
 * @details
 * This is the second experiment, which implements TPC-B.
 *
 * @section ENVIRONMENTS Environments
 * At least 24GB of available RAM.
 * At least 16 cores or more.
 *
 * @section OTHER Other notes
 * No special steps to build/run this expriment. This is self-contained.
 * The implementation is equivalent to test_array_tpcb.cpp, but more optimized, and without
 * verification feature.
 *
 * @section RESULTS Latest Results
 * 20140521 12M tps
 * NOTE: tpch_experiment_seq is more appropriate now that we have sequential for history.
 */
#include <atomic>
#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_rendezvous.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace array {

/** number of branches (TPS scaling factor). */
int kBranches  =   100;

int kTotalThreads = -1;

/** number of log writers per numa node */
const int kLoggersPerNode = 8;

/** number of tellers in 1 branch. */
const int kTellers   =   10;
/** number of accounts in 1 branch. */
const int kAccounts  =   100000;
const int kAccountsPerTeller = kAccounts / kTellers;

/** number of histories in TOTAL. */
const int kHistories =   200000000;

const uint64_t kDurationMicro = 5000000;

static_assert(kAccounts % kTellers == 0, "kAccounts must be multiply of kTellers");

struct BranchData {
  int64_t     branch_balance_;
  char        other_data_[104];  // just to make it at least 100 bytes
};

struct TellerData {
  uint64_t    branch_id_;
  int64_t     teller_balance_;
  char        other_data_[96];  // just to make it at least 100 bytes
};

struct AccountData {
  uint64_t    branch_id_;
  int64_t     account_balance_;
  char        other_data_[96];  // just to make it at least 100 bytes
};

struct HistoryData {
  uint64_t    account_id_;
  uint64_t    teller_id_;
  uint64_t    branch_id_;
  int64_t     amount_;
  char        other_data_[16];  // just to make it at least 50 bytes
};

struct ExperimentControlBlock {
  void initialize() {
    start_rendezvous_.initialize();
    stop_requested_ = false;
  }
  void uninitialize() {
    start_rendezvous_.uninitialize();
  }
  soc::SharedRendezvous start_rendezvous_;
  bool                  stop_requested_;
};

ErrorStack verify_task(
  thread::Thread* context,
  const void* /*input_buffer*/,
  uint32_t /*input_len*/,
  void* /*output_buffer*/,
  uint32_t /*output_buffer_size*/,
  uint32_t* /*output_used*/) {
  StorageManager* st = context->get_engine()->get_storage_manager();
  CHECK_ERROR(st->get_array("branches").verify_single_thread(context));
  CHECK_ERROR(st->get_array("tellers").verify_single_thread(context));
  CHECK_ERROR(st->get_array("accounts").verify_single_thread(context));
  CHECK_ERROR(st->get_array("histories").verify_single_thread(context));
  return kRetOk;
}

class RunTpcbTask {
 public:
  ErrorStack run(thread::Thread* context) {
    ExperimentControlBlock* control = reinterpret_cast<ExperimentControlBlock*>(
      context->get_engine()->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
    // pre-calculate random numbers to get rid of random number generation as bottleneck
    random_.set_current_seed(context->get_thread_id());
    CHECK_ERROR(
      context->get_thread_memory()->get_node_memory()->allocate_numa_memory(
        kRandomCount * sizeof(uint32_t), &numbers_));
    random_.fill_memory(&numbers_);
    const uint32_t *randoms = reinterpret_cast<const uint32_t*>(numbers_.get_block());
    std::memset(tmp_history_.other_data_, 0, sizeof(tmp_history_.other_data_));

    StorageManager* st = context->get_engine()->get_storage_manager();
    branches_ = st->get_array("branches");
    tellers_ = st->get_array("tellers");
    accounts_ = st->get_array("accounts");
    histories_ = st->get_array("histories");

    control->start_rendezvous_.wait();

    processed_ = 0;
    xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
    while (true) {
      uint64_t account_id = randoms[processed_ & 0xFFFF] % (kBranches * kAccounts);
      uint64_t teller_id = account_id / kAccountsPerTeller;
      uint64_t branch_id = account_id / kAccounts;
      uint64_t history_id = processed_ * kTotalThreads + context->get_thread_global_ordinal();
      if (history_id >= kHistories) {
        std::cerr << "Full histories" << std::endl;
        return kRetOk;
      }
      int64_t  amount = static_cast<int64_t>(random_.uniform_within(0, 1999999)) - 1000000;
      int successive_aborts = 0;
      while (!control->stop_requested_) {
        ErrorCode result_code = try_transaction(context,
          branch_id, teller_id, account_id, history_id, amount);
        if (result_code == kErrorCodeOk) {
          break;
        } else if (result_code == kErrorCodeXctRaceAbort) {
          // abort and retry
          if (context->is_running_xct()) {
            CHECK_ERROR(xct_manager->abort_xct(context));
          }
          if ((++successive_aborts & 0xFF) == 0) {
            std::cerr << "Thread-" << context->get_thread_id() << " having "
              << successive_aborts << " aborts in a row" << std::endl;
            assorted::memory_fence_acquire();
          }
        } else {
          COERCE_ERROR(ERROR_STACK(result_code));
        }
      }
      ++processed_;
      if ((processed_ & 0xFF) == 0) {
        assorted::memory_fence_acquire();
        if (control->stop_requested_) {
          break;
        }
      }
    }
    std::cout << "I'm done! " << context->get_thread_id()
      << ", processed=" << processed_ << std::endl;
    return kRetOk;
  }

  ErrorCode try_transaction(thread::Thread* context, uint64_t branch_id, uint64_t teller_id,
    uint64_t account_id, uint64_t history_id, int64_t amount) {
    xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
    CHECK_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

    CHECK_ERROR_CODE(branches_.increment_record_oneshot<int64_t>(
      context,
      branch_id,
      amount,
      0));

    CHECK_ERROR_CODE(tellers_.increment_record_oneshot<int64_t>(
      context,
      teller_id,
      amount,
      sizeof(uint64_t)));

    CHECK_ERROR_CODE(accounts_.increment_record_oneshot<int64_t>(
      context,
      account_id,
      amount,
      sizeof(uint64_t)));

    tmp_history_.account_id_ = account_id;
    tmp_history_.branch_id_ = branch_id;
    tmp_history_.teller_id_ = teller_id;
    tmp_history_.amount_ = amount;
    CHECK_ERROR_CODE(histories_.overwrite_record(context, history_id, &tmp_history_));

    Epoch commit_epoch;
    CHECK_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
    return kErrorCodeOk;
  }

  uint64_t get_processed() const { return processed_; }

 private:
  memory::AlignedMemory numbers_;
  assorted::UniformRandom random_;
  uint64_t processed_;
  const uint32_t kRandomCount = 1 << 16;

  HistoryData tmp_history_;
  ArrayStorage branches_;
  ArrayStorage tellers_;
  ArrayStorage accounts_;
  ArrayStorage histories_;
};

ErrorStack run_task(
  thread::Thread* context,
  const void* /*input_buffer*/,
  uint32_t /*input_len*/,
  void* output_buffer,
  uint32_t output_buffer_size,
  uint32_t* output_used) {
  RunTpcbTask task;
  CHECK_ERROR(task.run(context));
  ASSERT_ND(output_buffer_size >= sizeof(uint64_t));
  *output_used = sizeof(uint64_t);
  *reinterpret_cast<uint64_t*>(output_buffer) = task.get_processed();
  return kRetOk;
}

int main_impl(int argc, char **argv) {
  bool profile = false;
  if (argc >= 2 && std::string(argv[1]) == "--profile") {
    profile = true;
    std::cout << "Profiling..." << std::endl;
  }
  fs::Path folder("/dev/shm/tpcb_array_expr");
  if (fs::exists(folder)) {
    fs::remove_all(folder);
  }
  if (!fs::create_directories(folder)) {
    std::cerr << "Couldn't create " << folder << ". err="
      << assorted::os_error() << std::endl;
    return 1;
  }

  EngineOptions options;

  fs::Path savepoint_path(folder);
  savepoint_path /= "savepoint.xml";
  options.savepoint_.savepoint_path_.assign(savepoint_path.string());
  ASSERT_ND(!fs::exists(savepoint_path));

  std::cout << "NUMA node count=" << static_cast<int>(options.thread_.group_count_) << std::endl;
  options.snapshot_.folder_path_pattern_ = "/dev/shm/tpcb_array_expr/snapshot/node_$NODE$";
  options.log_.folder_path_pattern_ = "/dev/shm/tpcb_array_expr/log/node_$NODE$/logger_$LOGGER$";
  options.log_.loggers_per_node_ = kLoggersPerNode;
  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogInfo;
    // = debugging::DebuggingOptions::kDebugLogWarning;
  options.debugging_.verbose_modules_ = "";
  options.debugging_.verbose_log_level_ = -1;
  options.log_.log_buffer_kb_ = 1 << 20;  // 256MB * 16 cores = 4 GB. nothing.
  options.log_.log_file_size_mb_ = 1 << 10;
  options.memory_.page_pool_size_mb_per_node_ = 1 << 13;  // 8GB per node = 16GB
  kTotalThreads = options.thread_.group_count_ * options.thread_.thread_count_per_group_;

  {
    Engine engine(options);
    engine.get_proc_manager()->pre_register("run_task", run_task);
    engine.get_proc_manager()->pre_register("verify_task", verify_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      StorageManager* str_manager = engine.get_storage_manager();
      std::cout << "Creating TPC-B tables... " << std::endl;
      Epoch ep;
      ArrayMetadata branch_meta("branches", sizeof(BranchData), kBranches);
      COERCE_ERROR(str_manager->create_storage(&branch_meta, &ep));
      std::cout << "Created branches " << std::endl;
      ArrayMetadata teller_meta("tellers", sizeof(TellerData), kBranches * kTellers);
      COERCE_ERROR(str_manager->create_storage(&teller_meta, &ep));
      std::cout << "Created tellers " << std::endl;
      ArrayMetadata account_meta("accounts", sizeof(AccountData), kBranches * kAccounts);
      COERCE_ERROR(str_manager->create_storage(&account_meta, &ep));
      std::cout << "Created accounts " << std::endl;
      ArrayMetadata history_meta("histories", sizeof(HistoryData), kHistories);
      COERCE_ERROR(str_manager->create_storage(&history_meta, &ep));
      std::cout << "Created all!" << std::endl;

      COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify_task"));

      ExperimentControlBlock* control = reinterpret_cast<ExperimentControlBlock*>(
        engine.get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
      control->initialize();

      std::vector< thread::ImpersonateSession > sessions;
      for (int i = 0; i < kTotalThreads; ++i) {
        thread::ImpersonateSession session;
        bool ret = engine.get_thread_pool()->impersonate("run_task", nullptr, 0, &session);
        ASSERT_ND(ret);
        sessions.emplace_back(std::move(session));
      }

      // make sure all threads are done with random number generation
      std::this_thread::sleep_for(std::chrono::seconds(1));
      if (profile) {
        COERCE_ERROR(engine.get_debug()->start_profile("tpcb_experiment.prof"));
      }
      control->start_rendezvous_.signal();  // GO!
      std::cout << "Started!" << std::endl;
      std::this_thread::sleep_for(std::chrono::microseconds(kDurationMicro));
      std::cout << "Experiment ended." << std::endl;

      assorted::memory_fence_release();
      control->stop_requested_ = true;
      assorted::memory_fence_release();
      if (profile) {
        engine.get_debug()->stop_profile();
      }
      uint64_t total = 0;
      for (int i = 0; i < kTotalThreads; ++i) {
        std::cout << "session: result[" << i << "]=" << sessions[i].get_result() << std::endl;
        uint64_t processed;
        sessions[i].get_output(&processed);
        total += processed;
        sessions[i].release();
      }
      std::cout << "total=" << total << ", MTPS="
        << (static_cast<double>(total)/kDurationMicro) << std::endl;
      std::cout << "Shutting down..." << std::endl;
      COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify_task"));
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
