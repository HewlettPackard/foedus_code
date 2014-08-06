/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
/**
 * @file foedus/storage/sequential/tpcb_experiment_seq.cpp
 * @brief TPC-B experiment on array storage with sequential storage for history
 * @author kimurhid
 * @date 2014/07/07
 * @details
 * This is just a slightly modified version of array/tpcb_experiment.cpp.
 * The only difference is that this one uses sequential storage for history table.
 *
 * @section RESULTS Latest Results
 * 20140701 14M tps (up from 12M of array storage even for history)
 * Also, it was 5M tps before the sequential storage optimization to avoid contentious CAS.
 * 20140803 16.5M tps (now uses the increment_record_oneshot() method.)
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
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace sequential {

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

const uint64_t kDurationMicro = 5000000;

static_assert(kAccounts % kTellers == 0, "kAccounts must be multiply of kTellers");

struct BranchData {
  int64_t     branch_balance_;
  char        other_data_[96];  // just to make it at least 100 bytes
};

struct TellerData {
  uint64_t    branch_id_;
  int64_t     teller_balance_;
  char        other_data_[88];  // just to make it at least 100 bytes
};

struct AccountData {
  uint64_t    branch_id_;
  int64_t     account_balance_;
  char        other_data_[88];  // just to make it at least 100 bytes
};

struct HistoryData {
  uint64_t    account_id_;
  uint64_t    teller_id_;
  uint64_t    branch_id_;
  int64_t     amount_;
  char        other_data_[24];  // just to make it at least 50 bytes
};

array::ArrayStorage*  branches      = nullptr;
array::ArrayStorage*  accounts      = nullptr;
array::ArrayStorage*  tellers       = nullptr;
SequentialStorage*    histories     = nullptr;
thread::Rendezvous start_endezvous;
bool          stop_requested;

class RunTpcbTask : public thread::ImpersonateTask {
 public:
  explicit RunTpcbTask() {
    std::memset(tmp_history_.other_data_, 0, sizeof(tmp_history_.other_data_));
  }
  ErrorStack run(thread::Thread* context) {
    // pre-calculate random numbers to get rid of random number generation as bottleneck
    random_.set_current_seed(context->get_thread_id());
    CHECK_ERROR(
      context->get_thread_memory()->get_node_memory()->allocate_numa_memory(
        kRandomCount * sizeof(uint32_t), &numbers_));
    random_.fill_memory(&numbers_);
    const uint32_t *randoms = reinterpret_cast<const uint32_t*>(numbers_.get_block());

    start_endezvous.wait();

    processed_ = 0;
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    while (true) {
      uint64_t account_id = randoms[processed_ & 0xFFFF] % (kBranches * kAccounts);
      uint64_t teller_id = account_id / kAccountsPerTeller;
      uint64_t branch_id = account_id / kAccounts;
      int64_t  amount = static_cast<int64_t>(random_.uniform_within(0, 1999999)) - 1000000;
      int successive_aborts = 0;
      while (!stop_requested) {
        ErrorCode result_code = try_transaction(context, branch_id, teller_id, account_id, amount);
        if (result_code == kErrorCodeOk) {
          break;
        } else if (result_code == kErrorCodeXctRaceAbort) {
          // abort and retry
          if (context->is_running_xct()) {
            CHECK_ERROR(xct_manager.abort_xct(context));
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
        if (stop_requested) {
          break;
        }
      }
    }
    std::cout << "I'm done! " << context->get_thread_id()
      << ", processed=" << processed_ << std::endl;
    return kRetOk;
  }

  ErrorCode try_transaction(
    thread::Thread* context,
    uint64_t branch_id,
    uint64_t teller_id,
    uint64_t account_id,
    int64_t amount) {
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    CHECK_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));

    CHECK_ERROR_CODE(branches->increment_record_oneshot<int64_t>(
      context,
      branch_id,
      amount,
      0));

    CHECK_ERROR_CODE(tellers->increment_record_oneshot<int64_t>(
      context,
      teller_id,
      amount,
      sizeof(uint64_t)));

    CHECK_ERROR_CODE(accounts->increment_record_oneshot<int64_t>(
      context,
      account_id,
      amount,
      sizeof(uint64_t)));

    tmp_history_.account_id_ = account_id;
    tmp_history_.branch_id_ = branch_id;
    tmp_history_.teller_id_ = teller_id;
    tmp_history_.amount_ = amount;
    CHECK_ERROR_CODE(histories->append_record(context, &tmp_history_, sizeof(HistoryData)));

    Epoch commit_epoch;
    CHECK_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    return kErrorCodeOk;
  }

  uint64_t get_processed() const { return processed_; }

 private:
  memory::AlignedMemory numbers_;
  assorted::UniformRandom random_;
  uint64_t processed_;
  const uint32_t kRandomCount = 1 << 16;

  HistoryData tmp_history_;
};

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
  options.savepoint_.savepoint_path_ = savepoint_path.string();
  ASSERT_ND(!fs::exists(savepoint_path));

  std::cout << "NUMA node count=" << static_cast<int>(options.thread_.group_count_) << std::endl;
  options.snapshot_.folder_path_pattern_ = "/dev/shm/tpcb_seq_expr/snapshot/node_$NODE$";
  options.log_.folder_path_pattern_ = "/dev/shm/tpcb_seq_expr/log/node_$NODE$/logger_$LOGGER$";
  options.log_.loggers_per_node_ = kLoggersPerNode;
  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogInfo;
    // = debugging::DebuggingOptions::kDebugLogWarning;
  options.debugging_.verbose_modules_ = "";
  options.debugging_.verbose_log_level_ = -1;
  options.snapshot_.snapshot_interval_milliseconds_ = 1 << 20;
  options.log_.log_buffer_kb_ = 1 << 20;  // 256MB * 16 cores = 4 GB. nothing.
  options.log_.log_file_size_mb_ = 1 << 10;
  options.memory_.page_pool_size_mb_per_node_ = 1 << 13;  // 8GB per node = 16GB
  kTotalThreads = options.thread_.group_count_ * options.thread_.thread_count_per_group_;

  {
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      StorageManager& str_manager = engine.get_storage_manager();
      std::cout << "Creating TPC-B tables... " << std::endl;
      Epoch ep;
      array::ArrayMetadata branch_meta("branches", sizeof(BranchData), kBranches);
      COERCE_ERROR(str_manager.create_array(&branch_meta, &branches, &ep));
      std::cout << "Created branches " << std::endl;
      array::ArrayMetadata teller_meta("tellers", sizeof(TellerData), kBranches * kTellers);
      COERCE_ERROR(str_manager.create_array(&teller_meta, &tellers, &ep));
      std::cout << "Created tellers " << std::endl;
      array::ArrayMetadata account_meta("accounts", sizeof(AccountData), kBranches * kAccounts);
      COERCE_ERROR(str_manager.create_array(&account_meta, &accounts, &ep));
      std::cout << "Created accounts " << std::endl;
      SequentialMetadata history_meta("histories");
      COERCE_ERROR(str_manager.create_sequential(&history_meta, &histories, &ep));
      std::cout << "Created all!" << std::endl;

      std::vector< RunTpcbTask* > tasks;
      std::vector< thread::ImpersonateSession > sessions;
      for (int i = 0; i < kTotalThreads; ++i) {
        tasks.push_back(new RunTpcbTask());
        sessions.emplace_back(engine.get_thread_pool().impersonate(tasks[i]));
        if (!sessions[i].is_valid()) {
          COERCE_ERROR(sessions[i].invalid_cause_);
        }
      }

      // make sure all threads are done with random number generation
      std::this_thread::sleep_for(std::chrono::seconds(1));
      if (profile) {
        COERCE_ERROR(engine.get_debug().start_profile("tpcb_experiment_seq.prof"));
      }
      start_endezvous.signal();  // GO!
      std::cout << "Started!" << std::endl;
      std::this_thread::sleep_for(std::chrono::microseconds(kDurationMicro));
      std::cout << "Experiment ended." << std::endl;

      uint64_t total = 0;
      assorted::memory_fence_acquire();
      for (int i = 0; i < kTotalThreads; ++i) {
        total += tasks[i]->get_processed();
      }
      if (profile) {
        engine.get_debug().stop_profile();
      }
      std::cout << "total=" << total << ", MTPS="
        << (static_cast<double>(total)/kDurationMicro) << std::endl;
      std::cout << "Shutting down..." << std::endl;

      assorted::memory_fence_release();
      stop_requested = true;
      assorted::memory_fence_release();

      for (int i = 0; i < kTotalThreads; ++i) {
        std::cout << "result[" << i << "]=" << sessions[i].get_result() << std::endl;
        delete tasks[i];
      }
      COERCE_ERROR(engine.uninitialize());
    }
  }

  return 0;
}

}  // namespace sequential
}  // namespace storage
}  // namespace foedus

int main(int argc, char **argv) {
  return foedus::storage::sequential::main_impl(argc, argv);
}
