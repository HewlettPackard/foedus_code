/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
/**
 * @file foedus/storage/masstree/tpcb_experiment_masstree.cpp
 * @brief TPC-B experiment on masstree storage with sequential storage for history
 * @author kimurhid
 * @date 2014/07/25
 * @details
 * Unlike array/seq experiments that use array for main tables, this has an additional
 * populate phase to insert required records.
 * @section RESULTS Latest Results
 * 7 MTPS (2014/07/25) not bad...
 * 40% of cpu costs find_border_descend. half of it prefetch cost (mm_prefetch). interesting.
 */
#include <atomic>
#include <chrono>
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
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace storage {
namespace masstree {

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

MasstreeStorage*  branches      = nullptr;
MasstreeStorage*  accounts      = nullptr;
MasstreeStorage*  tellers       = nullptr;
sequential::SequentialStorage*  histories = nullptr;
thread::Rendezvous start_endezvous;
bool          stop_requested;

// for better performance, commit frequently.
// (we have to sort write set at commit, so there is something nlogn)
const uint32_t kCommitBatch = 1000;

inline KeySlice nm(uint64_t key) {
  return normalize_primitive<uint64_t>(key);
}

class PopulateTpcbTask : public thread::ImpersonateTask {
 public:
  PopulateTpcbTask(uint16_t from_branch, uint16_t to_branch)
    : from_branch_(from_branch), to_branch_(to_branch) {
  }
  ErrorStack run(thread::Thread* context) {
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager.begin_xct(context, xct::kDirtyReadPreferVolatile));

    std::cout << "Populating records from branch " << from_branch_ << " to "
      << to_branch_ << " in node-" << static_cast<int>(context->get_numa_node()) << std::endl;
    BranchData branch;
    std::memset(&branch, 0, sizeof(branch));
    TellerData teller;
    std::memset(&teller, 0, sizeof(teller));
    AccountData account;
    std::memset(&account, 0, sizeof(account));

#ifndef NDEBUG
    std::set<uint64_t> branch_ids;
    std::set<uint64_t> teller_ids;
    std::set<uint64_t> account_ids;
#endif  // NDEBUG

    for (uint64_t branch_id = from_branch_; branch_id < to_branch_; ++branch_id) {
#ifndef NDEBUG
    ASSERT_ND(branch_ids.find(branch_id) == branch_ids.end());
    branch_ids.insert(branch_id);
#endif  // NDEBUG
      branch.branch_balance_ = 0;
      commit_if_full(context);
      WRAP_ERROR_CODE(branches->insert_record_normalized(
        context,
        nm(branch_id),
        &branch,
        sizeof(branch)));

      for (uint64_t teller_ordinal = 0; teller_ordinal < kTellers; ++teller_ordinal) {
        uint64_t teller_id = kTellers * branch_id + teller_ordinal;
#ifndef NDEBUG
        ASSERT_ND(teller_ids.find(teller_id) == teller_ids.end());
        teller_ids.insert(teller_id);
#endif  // NDEBUG
        teller.branch_id_ = branch_id;
        commit_if_full(context);
        WRAP_ERROR_CODE(tellers->insert_record_normalized(
          context,
          nm(teller_id),
          &teller,
          sizeof(teller)));

        for (uint64_t account_ordinal = 0;
              account_ordinal < kAccountsPerTeller;
              ++account_ordinal) {
          uint64_t account_id = teller_id * kAccountsPerTeller + account_ordinal;
#ifndef NDEBUG
          ASSERT_ND(account_ids.find(account_id) == account_ids.end());
          account_ids.insert(account_id);
#endif  // NDEBUG
          account.branch_id_ = branch_id;
          commit_if_full(context);
          WRAP_ERROR_CODE(accounts->insert_record_normalized(
            context,
            nm(account_id),
            &account,
            sizeof(account)));
        }
      }
    }
    Epoch commit_epoch;
    WRAP_ERROR_CODE(xct_manager.precommit_xct(context, &commit_epoch));

    std::cout << "Populated records by " << context->get_thread_id() << std::endl;
    return kRetOk;
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

 private:
  const uint16_t from_branch_;
  const uint16_t to_branch_;
};

class RunTpcbTask : public thread::ImpersonateTask {
 public:
  RunTpcbTask() {
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
    // CHECK_ERROR_CODE(xct_manager.begin_xct(context, xct::kSerializable));
    CHECK_ERROR_CODE(xct_manager.begin_xct(context, xct::kDirtyReadPreferVolatile));

    int64_t balance = amount;
    CHECK_ERROR_CODE(branches->increment_record_normalized(context, nm(branch_id), &balance, 0));

    balance = amount;
    CHECK_ERROR_CODE(tellers->increment_record_normalized(
      context,
      nm(teller_id),
      &balance,
      sizeof(uint64_t)));

    balance = amount;
    CHECK_ERROR_CODE(accounts->increment_record_normalized(
      context,
      nm(account_id),
      &balance,
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
  fs::Path folder("/dev/shm/tpcb_masstree_expr");
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
  options.snapshot_.snapshot_interval_milliseconds_ = 1 << 20;  // never
  options.log_.folder_path_pattern_ = "/dev/shm/tpcb_seq_expr/log/node_$NODE$/logger_$LOGGER$";
  options.log_.loggers_per_node_ = kLoggersPerNode;
  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogInfo;
    // = debugging::DebuggingOptions::kDebugLogWarning;
  options.debugging_.verbose_modules_ = "";
  options.debugging_.verbose_log_level_ = -1;
  options.log_.log_buffer_kb_ = 1 << 21;
  options.log_.log_file_size_mb_ = 1 << 10;
  options.memory_.page_pool_size_mb_per_node_ = 12 << 10;
  kTotalThreads = options.thread_.group_count_ * options.thread_.thread_count_per_group_;

  {
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      StorageManager& str_manager = engine.get_storage_manager();
      std::cout << "Creating TPC-B tables... " << std::endl;
      Epoch ep;
      MasstreeMetadata branch_meta("branches");
      COERCE_ERROR(str_manager.create_masstree(&branch_meta, &branches, &ep));
      std::cout << "Created branches " << std::endl;
      MasstreeMetadata teller_meta("tellers");
      COERCE_ERROR(str_manager.create_masstree(&teller_meta, &tellers, &ep));
      std::cout << "Created tellers " << std::endl;
      MasstreeMetadata account_meta("accounts");
      COERCE_ERROR(str_manager.create_masstree(&account_meta, &accounts, &ep));
      std::cout << "Created accounts " << std::endl;
      sequential::SequentialMetadata history_meta("histories");
      COERCE_ERROR(str_manager.create_sequential(&history_meta, &histories, &ep));
      std::cout << "Created all!" << std::endl;

      std::cout << "Now populating initial records..." << std::endl;
      // this is done serially. we do it in different nodes, but it's just to balance out
      // volatile pages.
      for (uint16_t node = 0; node < options.thread_.group_count_; ++node) {
        uint64_t branches_per_node = kBranches / options.thread_.group_count_;
        uint64_t from_branch = branches_per_node * node;
        uint64_t to_branch = from_branch + branches_per_node;
        if (node == options.thread_.group_count_ - 1) {
          to_branch = kBranches;  // in case kBranches is not multiply of node count
        }
        PopulateTpcbTask task(from_branch, to_branch);
        thread::ImpersonateSession session(
          engine.get_thread_pool().impersonate_on_numa_node(&task, node));
        if (!session.is_valid()) {
          COERCE_ERROR(session.invalid_cause_);
        }
        std::cout << "populate result=" << session.get_result() << std::endl;
      }

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

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

int main(int argc, char **argv) {
  return foedus::storage::masstree::main_impl(argc, argv);
}
