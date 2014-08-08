/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
/**
 * @file foedus/storage/hash/tpcb_experiment_hash.cpp
 * @brief TPC-B experiment on hash storage with sequential storage for history
 * @author kimurhid
 * @date 2014/07/15
 * @details
 * Unlike array/seq experiments that use array for main tables, this has an additional
 * populate phase to insert required records.
 * @section RESULTS Latest Results
 * 8.8 MTPS. 7% CPU costs are in tag-checking.
 */
#include <atomic>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <set>
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
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace hash {

/** number of branches (TPS scaling factor). */
int kBranches  =   10;

int kTotalThreads = -1;

/** number of log writers per numa node */
const int kLoggersPerNode = 4;

/** number of tellers in 1 branch. */
const int kTellers   =   10;
/** number of accounts in 1 branch. */
const int kAccounts  =   100000;
const int kAccountsPerTeller = kAccounts / kTellers;

const uint64_t kDurationMicro = 1000000;

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

HashStorage*  branches      = nullptr;
HashStorage*  accounts      = nullptr;
HashStorage*  tellers       = nullptr;
sequential::SequentialStorage*  histories = nullptr;
thread::Rendezvous start_endezvous;
bool          stop_requested;

// for better performance, commit frequently.
// (we have to sort write set at commit, so there is something nlogn)
const uint32_t kCommitBatch = 1000;

class PopulateTpcbTask : public thread::ImpersonateTask {
 private:
  uint64_t fill_height_;

 public:

  PopulateTpcbTask(uint64_t populate_to) {
    fill_height_ = populate_to;
  }

  ErrorCode commit_if_full(thread::Thread* context) {
    if (context->get_current_xct().get_write_set_size() >= kCommitBatch) {
      Epoch commit_epoch;
      xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
      CHECK_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
      CHECK_ERROR_CODE(xct_manager.begin_xct(context, xct::kDirtyReadPreferVolatile));
    }
    return kErrorCodeOk;
  }

  ErrorStack run(thread::Thread* context) {
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kDirtyReadPreferVolatile));

    AccountData account;
    std::memset(&account, 0, sizeof(account));

#ifndef NDEBUG
    std::set<uint64_t> account_ids;
#endif  // NDEBUG

    for (uint64_t account_id = 0; account_id < fill_height_; ++account_id) {
      if(account_id%(1<<20) == 0) std::cout << account_id << " is id " << std::endl;
#ifndef NDEBUG
      ASSERT_ND(account_ids.find(account_id) == account_ids.end());
      account_ids.insert(account_id);
#endif  // NDEBUG
      account.account_balance_ = account_id;
      commit_if_full(context);
      WRAP_ERROR_CODE(accounts->insert_record(context, account_id, &account, sizeof(account)));
    }
    Epoch commit_epoch;
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    ASSERT_ND(!context->is_running_xct());
    std::cout << "Populated records by " << context->get_thread_id() << std::endl;
    return kRetOk;
  }
};

class RunTpcbTask : public thread::ImpersonateTask {
private:
  memory::AlignedMemory numbers_;
  assorted::UniformRandom random_;
  uint64_t processed_;
  uint64_t test_add_;
  const uint32_t kRandomCount = 1 << 16;

  HistoryData tmp_history_;


public:
  RunTpcbTask(uint64_t add_num) {
    test_add_ = add_num;
    std::memset(tmp_history_.other_data_, 0, sizeof(tmp_history_.other_data_));
  }
  ErrorStack run(thread::Thread* context) {
    ASSERT_ND(!context->is_running_xct());


    // pre-calculate random numbers to get rid of random number generation as bottleneck
    random_.set_current_seed(context->get_thread_id());
    CHECK_ERROR(
      context->get_thread_memory()->get_node_memory()->allocate_numa_memory(
        kRandomCount * sizeof(uint32_t), &numbers_));
    random_.fill_memory(&numbers_);
    const uint32_t *randoms = reinterpret_cast<const uint32_t*>(numbers_.get_block());

    AccountData account;
    std::memset(&account, 0, sizeof(account));

    start_endezvous.wait();

    std::clock_t start, end;
    start = std::clock();

    processed_ = 0;
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    for (uint64_t insert_num = 0; insert_num < test_add_; insert_num++) {
      uint64_t extra_bits = (processed_ + 1) << 32;
      uint64_t account_id = randoms[processed_ & 0xFFFF] % (kBranches * kAccounts) + extra_bits;
      account.account_balance_ = static_cast<int64_t>(random_.uniform_within(0, 1999999)) - 1000000;
      int successive_aborts = 0;
      while (true) {
        //std::cout<<"Trying"<<std::endl;
        ErrorCode result_code = try_transaction(context, account_id, &account);
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
          if (result_code != kErrorCodeOk) {
            std::cout<<std::endl<<get_processed()<<std::endl;
          }
          if (result_code != kErrorCodeOk) {
            std::cout << "Hrmmm " << result_code << std::endl;
         }
          //ASSERT_ND(result_code == kErrorCodeOk);
          COERCE_ERROR(ERROR_STACK(result_code));
        }
      }
      //std::cout<<"Finished a record: "<<processed_<<std::endl;
      ++processed_;
      if ((processed_ & 0xFF) == 0) { //TODO(Bill): Why?
        assorted::memory_fence_acquire();
        if (stop_requested) {
          break;
        }
      }
    }
    end = std::clock();
    double duration = ((double) (end - start)) / ((double) CLOCKS_PER_SEC);
    std::cout << "I'm done! " << context->get_thread_id()
      << ", processed=" << processed_ << " duration = " << duration
      << " TPS = " << (processed_/duration)/(1000000) << std::endl;
    return kRetOk;
  }

  ErrorCode try_transaction(
    thread::Thread* context,
    uint64_t account_id,
    AccountData* amount) {
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    // CHECK_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    CHECK_ERROR_CODE(xct_manager.begin_xct(context, xct::kDirtyReadPreferVolatile));
    CHECK_ERROR_CODE(accounts->insert_record(context, account_id, amount, sizeof(amount)));
    tmp_history_.account_id_ = account_id; //Why do we do this?
    tmp_history_.amount_ = amount->account_balance_;

    //CHECK_ERROR_CODE(histories->append_record(context, &tmp_history_, sizeof(HistoryData)));

    Epoch commit_epoch;
    CHECK_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));
    return kErrorCodeOk;
  }

  uint64_t get_processed() const { return processed_; }

};

int main_impl(int argc, char **argv) {
  bool profile = false;
  if (argc >= 2 && std::string(argv[1]) == "--profile") {
    profile = true;
    std::cout << "Profiling..." << std::endl;
  }
  fs::Path folder("/dev/shm/tpcb_hash_expr");
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
  options.thread_.group_count_ = 1;
  options.thread_.thread_count_per_group_ = 1;
  options.snapshot_.folder_path_pattern_ = "/dev/shm/tpcb_seq_expr/snapshot/node_$NODE$";
  options.snapshot_.snapshot_interval_milliseconds_ = 1 << 27;  // never
  options.log_.folder_path_pattern_ = "/dev/shm/tpcb_seq_expr/log/node_$NODE$/logger_$LOGGER$";
  options.log_.loggers_per_node_ = 1;
  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogInfo;
    // = debugging::DebuggingOptions::kDebugLogWarning;
  options.debugging_.verbose_modules_ = "";
  options.debugging_.verbose_log_level_ = -1;
  options.log_.log_buffer_kb_ = 1 << 16;
  options.log_.log_file_size_mb_ = 1 << 10;
  options.memory_.page_pool_size_mb_per_node_ = 4 << 10; // may be made bigger when needed
  kTotalThreads = options.thread_.group_count_ * options.thread_.thread_count_per_group_;

  {
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
      if (kMaxEntriesPerBin != 23) {
        std::cout << "Bin-size is wrong for test" << std::endl;
        return 0;
      }
      uint64_t size = 20;
      uint64_t record_num  = 2 << size;
      uint64_t populate_to = (uint64_t)((double) record_num * (double) .9);  // Number of records to add during populate
      uint64_t test_add    = (uint64_t)((double) record_num * (double) .05);;  // Number of records to add during benchmark

      UninitializeGuard guard(&engine);
      StorageManager& str_manager = engine.get_storage_manager();
      std::cout << "Creating TPC-B tables... " << std::endl;
      Epoch ep;

      HashMetadata account_meta("accounts", size);
      COERCE_ERROR(str_manager.create_hash(&account_meta, &accounts, &ep));
      std::cout << "Created accounts " << std::endl;

      sequential::SequentialMetadata history_meta("histories");
      COERCE_ERROR(str_manager.create_sequential(&history_meta, &histories, &ep));

      std::cout << "Created all!" << std::endl;

      std::cout << "Now populating initial records..." << std::endl;
      // this is done serially. we do it in different nodes, but it's just to balance out
      // volatile pages.
      for (uint16_t node = 0; node < options.thread_.group_count_; ++node) {
//         uint64_t branches_per_node = kBranches / options.thread_.group_count_;
//         uint64_t from_branch = branches_per_node * node;
//         uint64_t to_branch = from_branch + branches_per_node;
//         if (node == options.thread_.group_count_ - 1) {
//           to_branch = kBranches;  // in case kBranches is not multiply of node count
//         }
        PopulateTpcbTask task(populate_to);
        thread::ImpersonateSession session(
          engine.get_thread_pool().impersonate_on_numa_node(&task, node));
        if (!session.is_valid()) {
          COERCE_ERROR(session.invalid_cause_);
        }
        std::cout << "populate result=" << session.get_result() << std::endl;
        COERCE_ERROR(session.get_result());
      }
      std::cout << account_meta.get_bin_count() << std::endl;
      std::cout << "Starting test" << std::endl;
      std::vector< RunTpcbTask* > tasks;
      std::vector< thread::ImpersonateSession > sessions;

      kTotalThreads = 1;
      for (int i = 0; i < kTotalThreads; ++i) {
        tasks.push_back(new RunTpcbTask(test_add));
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

      std::this_thread::sleep_for(std::chrono::seconds(1000));


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

}  // namespace hash
}  // namespace storage
}  // namespace foedus

int main(int argc, char **argv) {
  return foedus::storage::hash::main_impl(argc, argv);
}
