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
 * At least 1GB of available RAM.
 * At least 4 cores or more.
 *
 * @section OTHER Other notes
 * No special steps to build/run this expriment. This is self-contained.
 * The implementation is equivalent to test_array_tpcb.cpp, but more optimized, and without
 * verification feature.
 *
 * @section RESULTS Latest Results
 * foedus_results/20140414_kimurhid_array_readonly
 */
#include <foedus/error_stack.hpp>
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/assorted/uniform_random.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/memory/aligned_memory.hpp>
#include <foedus/memory/numa_node_memory.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <google/profiler.h>
#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace foedus {
namespace storage {
namespace array {

/** number of branches (TPS scaling factor). */
int BRANCHES  =   100;

int TOTAL_THREADS = 1;

/** number of tellers in 1 branch. */
const int TELLERS   =   10;
/** number of accounts in 1 branch. */
const int ACCOUNTS  =   100000;
const int ACCOUNTS_PER_TELLER = ACCOUNTS / TELLERS;

/** number of histories in TOTAL. */
const int HISTORIES =   10000000;

const uint64_t DURATION_MICRO = 100000;

static_assert(ACCOUNTS % TELLERS == 0, "ACCOUNTS must be multiply of TELLERS");

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

ArrayStorage* branches      = nullptr;
ArrayStorage* accounts      = nullptr;
ArrayStorage* tellers       = nullptr;
ArrayStorage* histories     = nullptr;
bool          stop_requested;

class RunTpcbTask : public thread::ImpersonateTask {
 public:
    RunTpcbTask(uint16_t history_ordinal, std::shared_future<void> start_future)
        : history_ordinal_(history_ordinal), start_future_(start_future) {}
    ErrorStack run(thread::Thread* context) {
        // pre-calculate random numbers to get rid of random number generation as bottleneck
        random_.set_current_seed(history_ordinal_);
        CHECK_ERROR(
            context->get_thread_memory()->get_node_memory()->allocate_numa_memory(
                RANDOM_COUNT * sizeof(uint32_t), &numbers_));
        random_.fill_memory(&numbers_);
        const uint32_t *randoms = reinterpret_cast<const uint32_t*>(numbers_.get_block());

        start_future_.get();

        processed_ = 0;
        xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
        while (true) {
            uint64_t account_id = randoms[processed_ & 0xFFFF] % (BRANCHES * ACCOUNTS);
            uint64_t teller_id = account_id / ACCOUNTS_PER_TELLER;
            uint64_t branch_id = account_id / ACCOUNTS;
            uint64_t history_id = processed_ * TOTAL_THREADS + history_ordinal_;
            int64_t  amount = static_cast<int64_t>(random_.uniform_within(0, 1999999)) - 1000000;
            int successive_aborts = 0;
            while (!stop_requested) {
                ErrorStack error_stack = try_transaction(context,
                    branch_id, teller_id, account_id, history_id, amount);
                if (!error_stack.is_error()) {
                    break;
                } else if (error_stack.get_error_code() == ERROR_CODE_XCT_RACE_ABORT) {
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
                    COERCE_ERROR(error_stack);
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
        return RET_OK;
    }

    ErrorStack try_transaction(thread::Thread* context, uint64_t branch_id, uint64_t teller_id,
        uint64_t account_id, uint64_t history_id, int64_t amount) {
        xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
        CHECK_ERROR(xct_manager.begin_xct(context, xct::SERIALIZABLE));

        int64_t current_balance;
        CHECK_ERROR(branches->get_record(context, branch_id, &current_balance,
                                         0, sizeof(current_balance)));
        int64_t branch_balance = current_balance + amount;
        CHECK_ERROR(branches->overwrite_record(context, branch_id,
                                                &branch_balance, 0, sizeof(branch_balance)));

        CHECK_ERROR(tellers->get_record(context, teller_id, &current_balance,
            sizeof(uint64_t), sizeof(current_balance)));
        int64_t teller_balance = current_balance + amount;
        CHECK_ERROR(tellers->overwrite_record(context, teller_id,
                    &teller_balance, sizeof(uint64_t), sizeof(teller_balance)));

        CHECK_ERROR(accounts->get_record(context, account_id, &current_balance,
            sizeof(uint64_t), sizeof(current_balance)));
        int64_t account_balance = current_balance + amount;
        CHECK_ERROR(accounts->overwrite_record(context, account_id,
                    &account_balance, sizeof(uint64_t), sizeof(account_balance)));

        tmp_history_.account_id_ = account_id;
        tmp_history_.branch_id_ = branch_id;
        tmp_history_.teller_id_ = teller_id;
        tmp_history_.amount_ = amount;
        CHECK_ERROR(histories->overwrite_record(context, history_id, &tmp_history_));

        Epoch commit_epoch;
        CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
        return RET_OK;
    }

    uint64_t get_processed() const { return processed_; }

 private:
    memory::AlignedMemory numbers_;
    assorted::UniformRandom random_;
    uint64_t processed_;
    uint16_t history_ordinal_;
    const uint32_t RANDOM_COUNT = 1 << 16;

    // to start all threads at the same time.
    std::shared_future<void>    start_future_;

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
    options.log_.log_paths_.clear();
    for (auto i = 0; i < options.thread_.group_count_; ++i) {
        std::stringstream str;
        str << "/dev/shm/tpcb_array_expr/foedus_node" << static_cast<int>(i) << ".log";
        options.log_.log_paths_.push_back(str.str());
    }
    options.debugging_.debug_log_min_threshold_
        = debugging::DebuggingOptions::DEBUG_LOG_WARNING;
    options.debugging_.verbose_modules_ = "";
    options.log_.log_buffer_kb_ = 1 << 18;  // 256MB * 16 cores = 4 GB. nothing.
    options.log_.log_file_size_mb_ = 1 << 10;
    options.memory_.page_pool_size_mb_ = 1 << 14;  // 16GB
    TOTAL_THREADS = options.thread_.group_count_ * options.thread_.thread_count_per_group_;

    std::cout << "options=" << options << std::endl;
    {
        Engine engine(options);
        COERCE_ERROR(engine.initialize());
        {
            UninitializeGuard guard(&engine);
            StorageManager& str_manager = engine.get_storage_manager();
            std::cout << "Creating TPC-B tables... " << std::endl;
            COERCE_ERROR(str_manager.create_array_impersonate("branches",
                                            sizeof(BranchData), BRANCHES, &branches));
            std::cout << "Created branches " << std::endl;
            COERCE_ERROR(str_manager.create_array_impersonate("tellers",
                                        sizeof(AccountData), BRANCHES * TELLERS, &tellers));
            std::cout << "Created tellers " << std::endl;
            COERCE_ERROR(str_manager.create_array_impersonate("accounts",
                                        sizeof(TellerData), BRANCHES * ACCOUNTS, &accounts));
            std::cout << "Created accounts " << std::endl;
            COERCE_ERROR(str_manager.create_array_impersonate("histories",
                                                sizeof(HistoryData), HISTORIES, &histories));
            std::cout << "Created all!" << std::endl;

            std::vector< RunTpcbTask* > tasks;
            std::vector< thread::ImpersonateSession > sessions;
            std::promise<void>          start_promise;
            std::shared_future<void>    start_future = start_promise.get_future().share();
            for (int i = 0; i < TOTAL_THREADS; ++i) {
                tasks.push_back(new RunTpcbTask(i, start_future));
                sessions.emplace_back(engine.get_thread_pool().impersonate(tasks[i]));
                if (!sessions[i].is_valid()) {
                    COERCE_ERROR(sessions[i].invalid_cause_);
                }
            }

            // make sure all threads are done with random number generation
            std::this_thread::sleep_for(std::chrono::seconds(1));
            if (profile) {
                ::ProfilerStart("tpcb_experiment.prof");
            }
            start_promise.set_value();  // GO!
            std::cout << "Started!" << std::endl;
            std::this_thread::sleep_for(std::chrono::microseconds(DURATION_MICRO));
            std::cout << "Experiment ended." << std::endl;

            uint64_t total = 0;
            assorted::memory_fence_acquire();
            for (int i = 0; i < TOTAL_THREADS; ++i) {
                total += tasks[i]->get_processed();
            }
            if (profile) {
                ::ProfilerStop();
            }
            std::cout << "total=" << total << ", MTPS="
                << (static_cast<double>(total)/DURATION_MICRO) << std::endl;
            std::cout << "Shutting down..." << std::endl;

            assorted::memory_fence_release();
            stop_requested = true;
            assorted::memory_fence_release();

            for (int i = 0; i < TOTAL_THREADS; ++i) {
                std::cout << "result[" << i << "]=" << sessions[i].get_result() << std::endl;
                delete tasks[i];
            }
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
