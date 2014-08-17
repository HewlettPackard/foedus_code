/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <stdint.h>
#include <gtest/gtest.h>

#include <iostream>
#include <set>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/test_common.hpp"
#include "foedus/assorted/uniform_random.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"
#include "foedus/storage/sequential/sequential_volatile_list_impl.hpp"
#include "foedus/thread/rendezvous_impl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_access.hpp"
#include "foedus/xct/xct_manager.hpp"

/**
 * @file test_sequential_tpcb.cpp
 * Basically same as the array version except that we use sequential for history.
 */
namespace foedus {
namespace storage {
namespace sequential {
DEFINE_TEST_CASE_PACKAGE(SequentialTpcbTest, foedus.storage.sequential);

// tiny numbers
/** number of branches (TPS scaling factor). */
const int kBranches  =   8;
/** number of tellers in 1 branch. */
const int kTellers   =   2;
/** number of accounts in 1 branch. */
const int kAccounts  =   4;
const int kAccountsPerTellers = kAccounts / kTellers;

/** In this testcase, we run at most this number of threads. */
const int kMaxTestThreads = 4;
/** number of transaction to run per thread. */
const int kXctsPerThread = 100;
const int kInitialAccountBalance = 100;
const int kAmountRangeFrom = 1;
const int kAmountRangeTo = 20;

/** number of histories in TOTAL. */
const int kHistories =   kXctsPerThread * kMaxTestThreads;

static_assert(kAccounts % kTellers == 0, "kAccounts must be multiply of kTellers");
static_assert(kHistories % kAccounts == 0, "kHistories must be multiply of kAccounts");

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
  uint64_t    history_id_;  // we have history ID as data in this test
  uint64_t    account_id_;
  uint64_t    teller_id_;
  uint64_t    branch_id_;
  int64_t     amount_;
  char        other_data_[16];  // just to make it at least 50 bytes
};

enum AccessorType {
  kNormal,
  kPrimitive,
  kIncrement,
  kIncrementOneShot,
};

array::ArrayStorage*  branches      = nullptr;
array::ArrayStorage*  accounts      = nullptr;
array::ArrayStorage*  tellers       = nullptr;
SequentialStorage*    histories     = nullptr;
AccessorType  accessor_type;

/** Creates TPC-B tables and populate with initial records. */
class CreateTpcbTablesTask : public thread::ImpersonateTask {
 public:
  ErrorStack run(thread::Thread* context) {
    StorageManager& str_manager = context->get_engine()->get_storage_manager();
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    Epoch highest_commit_epoch;
    Epoch commit_epoch;

    // Create branches
    array::ArrayMetadata branch_meta("branches", sizeof(BranchData), kBranches);
    COERCE_ERROR(str_manager.create_array(context, &branch_meta, &branches, &commit_epoch));
    EXPECT_TRUE(branches != nullptr);
    COERCE_ERROR(xct_manager.begin_xct(context, xct::kSerializable));
    for (int i = 0; i < kBranches; ++i) {
      BranchData data;
      std::memset(&data, 0, sizeof(data));  // make valgrind happy
      data.branch_balance_ = kInitialAccountBalance * kAccounts;
      COERCE_ERROR(branches->overwrite_record(context, i, &data));
    }
    COERCE_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
    highest_commit_epoch.store_max(commit_epoch);

    // Create tellers
    array::ArrayMetadata teller_meta("tellers", sizeof(TellerData), kBranches * kTellers);
    COERCE_ERROR(str_manager.create_array(context, &teller_meta, &tellers, &commit_epoch));
    EXPECT_TRUE(tellers != nullptr);
    COERCE_ERROR(xct_manager.begin_xct(context, xct::kSerializable));
    for (int i = 0; i < kBranches * kTellers; ++i) {
      TellerData data;
      std::memset(&data, 0, sizeof(data));  // make valgrind happy
      data.branch_id_ = i / kTellers;
      data.teller_balance_ = kInitialAccountBalance * kAccountsPerTellers;
      COERCE_ERROR(tellers->overwrite_record(context, i, &data));
    }
    COERCE_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
    highest_commit_epoch.store_max(commit_epoch);

    // Create accounts
    array::ArrayMetadata account_meta("accounts", sizeof(AccountData), kBranches * kAccounts);
    COERCE_ERROR(str_manager.create_array(context, &account_meta, &accounts, &commit_epoch));
    EXPECT_TRUE(accounts != nullptr);
    COERCE_ERROR(xct_manager.begin_xct(context, xct::kSerializable));
    for (int i = 0; i < kBranches * kAccounts; ++i) {
      AccountData data;
      std::memset(&data, 0, sizeof(data));  // make valgrind happy
      data.branch_id_ = i / kAccounts;
      data.account_balance_ = kInitialAccountBalance;
      COERCE_ERROR(accounts->overwrite_record(context, i, &data));
    }
    COERCE_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
    highest_commit_epoch.store_max(commit_epoch);

    // Create histories
    SequentialMetadata history_meta("histories");
    COERCE_ERROR(str_manager.create_sequential(context, &history_meta, &histories, &commit_epoch));
    EXPECT_TRUE(histories != nullptr);
    COERCE_ERROR(xct_manager.begin_xct(context, xct::kSerializable));
    COERCE_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
    highest_commit_epoch.store_max(commit_epoch);

    CHECK_ERROR(xct_manager.wait_for_commit(highest_commit_epoch));
    return kRetOk;
  }
};

/** run TPC-B queries. */
class RunTpcbTask : public thread::ImpersonateTask {
 public:
  RunTpcbTask(int client_id, bool contended, thread::Rendezvous* start_rendezvous)
    : client_id_(client_id), contended_(contended), start_rendezvous_(start_rendezvous) {
    ASSERT_ND(client_id < kMaxTestThreads);
  }
  ErrorStack run(thread::Thread* context) {
    start_rendezvous_->wait();
    assorted::UniformRandom rand;
    rand.set_current_seed(client_id_);
    Epoch highest_commit_epoch;
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    xct::XctId prev_xct_id;
    for (int i = 0; i < kXctsPerThread; ++i) {
      uint64_t account_id;
      if (contended_) {
        account_id = rand.next_uint32() % (kBranches * kAccounts);
      } else {
        const uint64_t accounts_per_thread = (kBranches * kAccounts / kMaxTestThreads);
        account_id = rand.next_uint32() % accounts_per_thread
          + (client_id_ * accounts_per_thread);
      }
      uint64_t teller_id = account_id / kAccountsPerTellers;
      uint64_t branch_id = account_id / kAccounts;
      uint64_t history_id = client_id_ * kXctsPerThread + i;
      int64_t  amount = rand.uniform_within(kAmountRangeFrom, kAmountRangeTo);
      EXPECT_GE(amount, kAmountRangeFrom);
      EXPECT_LE(amount, kAmountRangeTo);
      while (true) {
        ErrorStack error_stack = try_transaction(context, &highest_commit_epoch,
          branch_id, teller_id, account_id, history_id, amount);
        if (!error_stack.is_error()) {
          xct::XctId xct_id = context->get_current_xct().get_id();
          if (prev_xct_id.get_epoch() == xct_id.get_epoch()) {
            EXPECT_LT(prev_xct_id.get_ordinal(), xct_id.get_ordinal());
          }
          prev_xct_id = context->get_current_xct().get_id();
          break;
        } else if (error_stack.get_error_code() == kErrorCodeXctRaceAbort) {
          // abort and retry
          if (context->get_current_xct().is_active()) {
            CHECK_ERROR(xct_manager.abort_xct(context));
          }
        } else {
          COERCE_ERROR(error_stack);
        }
      }
    }
    CHECK_ERROR(xct_manager.wait_for_commit(highest_commit_epoch));
    return foedus::kRetOk;
  }

  ErrorStack try_transaction(
    thread::Thread* context, Epoch *highest_commit_epoch,
    uint64_t branch_id, uint64_t teller_id,
    uint64_t account_id, uint64_t history_id, int64_t amount) {
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    CHECK_ERROR(xct_manager.begin_xct(context, xct::kSerializable));

    int64_t branch_balance_old = -1, branch_balance_new;
    if (accessor_type == kIncrement) {
      branch_balance_new = amount;
      CHECK_ERROR(branches->increment_record<int64_t>(context, branch_id,
                              &branch_balance_new, 0));
      branch_balance_old = branch_balance_new - amount;
    } else if (accessor_type == kIncrementOneShot) {
      CHECK_ERROR(branches->increment_record_oneshot<int64_t>(context, branch_id, amount, 0));
    } else if (accessor_type == kPrimitive) {
      CHECK_ERROR(branches->get_record_primitive<int64_t>(context, branch_id,
                                &branch_balance_old, 0));
      branch_balance_new = branch_balance_old + amount;
      CHECK_ERROR(branches->overwrite_record_primitive<int64_t>(context, branch_id,
                        branch_balance_new, 0));
    } else {
      BranchData branch;
      CHECK_ERROR(branches->get_record(context, branch_id, &branch));
      branch_balance_old = branch.branch_balance_;
      branch_balance_new = branch_balance_old + amount;
      CHECK_ERROR(branches->overwrite_record(context, branch_id,
                      &branch_balance_new, 0, sizeof(branch_balance_new)));
    }
    if (accessor_type != kIncrementOneShot) {
      EXPECT_GE(branch_balance_old, 0);
    }

    int64_t teller_balance_old = -1, teller_balance_new;
    uint64_t teller_branch_id = 0;
    if (accessor_type == kIncrement) {
      CHECK_ERROR(tellers->get_record_primitive<uint64_t>(context, teller_id,
                                &teller_branch_id, 0));
      teller_balance_new = amount;
      CHECK_ERROR(tellers->increment_record<int64_t>(context, teller_id,
                              &teller_balance_new, sizeof(uint64_t)));
      teller_balance_old = teller_balance_new - amount;
    } else if (accessor_type == kIncrementOneShot) {
      CHECK_ERROR(tellers->get_record_primitive<uint64_t>(context, teller_id,
                                &teller_branch_id, 0));
      CHECK_ERROR(tellers->increment_record_oneshot<int64_t>(
        context,
        teller_id,
        amount,
        sizeof(uint64_t)));
    } else if (accessor_type == kPrimitive) {
      CHECK_ERROR(tellers->get_record_primitive<uint64_t>(context, teller_id,
                                &teller_branch_id, 0));
      CHECK_ERROR(tellers->get_record_primitive<int64_t>(context, teller_id,
                              &teller_balance_old, sizeof(uint64_t)));
      teller_balance_new = teller_balance_old + amount;
      CHECK_ERROR(tellers->overwrite_record_primitive<int64_t>(context, teller_id,
          teller_balance_new, sizeof(uint64_t)));
    } else {
      TellerData teller;
      CHECK_ERROR(tellers->get_record(context, teller_id, &teller));
      teller_branch_id = teller.branch_id_;
      teller_balance_old = teller.teller_balance_;
      teller_balance_new = teller_balance_old + amount;
      CHECK_ERROR(tellers->overwrite_record(context, teller_id,
          &teller_balance_new, sizeof(teller.branch_id_), sizeof(teller_balance_new)));
    }
    if (accessor_type != kIncrementOneShot) {
      EXPECT_GE(teller_balance_old, 0);
    }
    EXPECT_EQ(branch_id, teller_branch_id);

    int64_t account_balance_old = -1, account_balance_new;
    uint64_t account_branch_id = 0;
    if (accessor_type == kIncrement) {
      CHECK_ERROR(accounts->get_record_primitive<uint64_t>(context, account_id,
                            &account_branch_id, 0));
      account_balance_new = amount;
      CHECK_ERROR(accounts->increment_record<int64_t>(context, account_id,
                            &account_balance_new, sizeof(uint64_t)));
      account_balance_old = account_balance_new - amount;
    } else if (accessor_type == kIncrementOneShot) {
      CHECK_ERROR(accounts->get_record_primitive<uint64_t>(context, account_id,
                            &account_branch_id, 0));
      CHECK_ERROR(accounts->increment_record_oneshot<int64_t>(
        context,
        account_id,
        amount,
        sizeof(uint64_t)));
    } else if (accessor_type == kPrimitive) {
      CHECK_ERROR(accounts->get_record_primitive<uint64_t>(context, account_id,
                            &account_branch_id, 0));
      CHECK_ERROR(accounts->get_record_primitive<int64_t>(context, account_id,
                            &account_balance_old, sizeof(uint64_t)));
      account_balance_new = account_balance_old + amount;
      CHECK_ERROR(accounts->overwrite_record_primitive<int64_t>(context, account_id,
          account_balance_new, sizeof(uint64_t)));
    } else {
      AccountData account;
      CHECK_ERROR(accounts->get_record(context, account_id, &account));
      account_branch_id = account.branch_id_;
      account_balance_old = account.account_balance_;
      account_balance_new = account_balance_old + amount;
      CHECK_ERROR(accounts->overwrite_record(context, account_id,
          &account_balance_new, sizeof(account.branch_id_), sizeof(account_balance_new)));
    }
    if (accessor_type != kIncrementOneShot) {
      EXPECT_GE(account_balance_old, 0);
    }
    EXPECT_EQ(branch_id, account_branch_id);

    HistoryData history;
    history.history_id_ = history_id;
    history.account_id_ = account_id;
    history.branch_id_ = branch_id;
    history.teller_id_ = teller_id;
    history.amount_ = amount;
    std::memset(history.other_data_, 0, sizeof(history.other_data_));  // make valgrind happy
    CHECK_ERROR(histories->append_record(context, &history, sizeof(history)));

    Epoch commit_epoch;
    ASSERT_ND(context->get_current_xct().get_read_set_size() > 0);
    ASSERT_ND(context->get_current_xct().get_write_set_size() > 0);
    CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));

    if (accessor_type != kIncrementOneShot) {
      std::cout << "Committed! Thread-" << context->get_thread_id() << " Updated "
        << " branch[" << branch_id << "] " << branch_balance_old << " -> " << branch_balance_new
        << " teller[" << teller_id << "] " << teller_balance_old << " -> " << teller_balance_new
        << " account[" << account_id << "] " << account_balance_old
          << " -> " << account_balance_new
        << " history[" << history_id << "] amount=" << amount
        << std::endl;
    } else {
      std::cout << "Committed! Thread-" << context->get_thread_id() << " Updated "
        << " branch[" << branch_id << "], teller[" << teller_id << "], account[" << account_id
          << "], history[" << history_id << "]. amount=" << amount << std::endl;
    }
    highest_commit_epoch->store_max(commit_epoch);
    return kRetOk;
  }

 private:
  int                         client_id_;
  bool                        contended_;

  // to start all threads at the same time.
  thread::Rendezvous*         start_rendezvous_;
};


/** Verify TPC-B results. */
class VerifyTpcbTask : public thread::ImpersonateTask {
 public:
  explicit VerifyTpcbTask(int clients) : clients_(clients) {
    ASSERT_ND(clients <= kMaxTestThreads);
  }
  ErrorStack run(thread::Thread* context) {
    xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
    CHECK_ERROR(xct_manager.begin_xct(context, xct::kSerializable));

    int64_t expected_branch[kBranches];
    int64_t expected_teller[kBranches * kTellers];
    int64_t expected_account[kBranches * kAccounts];
    for (int i = 0; i < kBranches; ++i) {
      expected_branch[i] = kInitialAccountBalance * kAccounts;
    }
    for (int i = 0; i < kBranches * kTellers; ++i) {
      expected_teller[i] = kInitialAccountBalance * kAccountsPerTellers;
    }
    for (int i = 0; i < kBranches * kAccounts; ++i) {
      expected_account[i] = kInitialAccountBalance;
    }

    // we don't have scanning API yet, so manually do it.
    std::set<uint64_t> observed_history_ids;
    for (auto i = 0; i < histories->get_pimpl()->volatile_list_.get_thread_count(); ++i) {
      for (SequentialPage* page = histories->get_pimpl()->volatile_list_.get_head(i); page;) {
        uint16_t record_count = page->get_record_count();
        const char* record_pointers[kMaxSlots];
        uint16_t payload_lengthes[kMaxSlots];
        page->get_all_records_nosync(&record_count, record_pointers, payload_lengthes);

        for (uint16_t rec = 0; rec < record_count; ++rec) {
          EXPECT_EQ(payload_lengthes[rec], sizeof(HistoryData));
          const HistoryData& history = *reinterpret_cast<const HistoryData*>(
            record_pointers[rec] + kRecordOverhead);
          EXPECT_GE(history.amount_, kAmountRangeFrom);
          EXPECT_LE(history.amount_, kAmountRangeTo);

          EXPECT_LT(history.branch_id_, kBranches);
          EXPECT_LT(history.teller_id_, kBranches * kTellers);
          EXPECT_LT(history.account_id_, kBranches * kAccounts);

          EXPECT_EQ(history.branch_id_, history.teller_id_ / kTellers);
          EXPECT_EQ(history.branch_id_, history.account_id_ / kAccounts);
          EXPECT_EQ(history.teller_id_, history.account_id_ / kAccountsPerTellers);

          expected_branch[history.branch_id_] += history.amount_;
          expected_teller[history.teller_id_] += history.amount_;
          expected_account[history.account_id_] += history.amount_;
          EXPECT_EQ(observed_history_ids.end(), observed_history_ids.find(history.history_id_))
            << history.history_id_;
          observed_history_ids.insert(history.history_id_);
        }

        VolatilePagePointer next_pointer = page->next_page().volatile_pointer_;
        if (next_pointer.components.offset != 0) {
          EXPECT_NE(histories->get_pimpl()->volatile_list_.get_tail(i), page);
          page = reinterpret_cast<SequentialPage*>(
            context->get_global_volatile_page_resolver().resolve_offset(next_pointer));
          EXPECT_NE(nullptr, page);
        } else {
          EXPECT_EQ(histories->get_pimpl()->volatile_list_.get_tail(i), page);
          page = nullptr;
        }
      }
    }
    EXPECT_EQ(kXctsPerThread * clients_, observed_history_ids.size());
    for (int i = 0; i < kXctsPerThread * clients_; ++i) {
      EXPECT_NE(observed_history_ids.end(), observed_history_ids.find(i)) << i;
    }

    for (int i = 0; i < kBranches; ++i) {
      BranchData data;
      CHECK_ERROR(branches->get_record(context, i, &data));
      EXPECT_EQ(expected_branch[i], data.branch_balance_) << "branch-" << i;
    }
    for (int i = 0; i < kBranches * kTellers; ++i) {
      TellerData data;
      CHECK_ERROR(tellers->get_record(context, i, &data));
      EXPECT_EQ(i / kTellers, data.branch_id_) << i;
      EXPECT_EQ(expected_teller[i], data.teller_balance_) << "teller-" << i;
    }
    for (int i = 0; i < kBranches * kAccounts; ++i) {
      AccountData data;
      CHECK_ERROR(accounts->get_record(context, i, &data));
      EXPECT_EQ(i / kAccounts, data.branch_id_) << i;
      EXPECT_EQ(expected_account[i], data.account_balance_) << "account-" << i;
    }
    for (uint32_t i = 0; i < context->get_current_xct().get_read_set_size(); ++i) {
      xct::XctAccess& access = context->get_current_xct().get_read_set()[i];
      EXPECT_FALSE(access.observed_owner_id_.is_being_written()) << i;
      EXPECT_FALSE(access.observed_owner_id_.is_deleted()) << i;
      EXPECT_FALSE(access.observed_owner_id_.is_moved()) << i;
    }

    CHECK_ERROR(xct_manager.abort_xct(context));
    return foedus::kRetOk;
  }

 private:
  int clients_;
};

void run_test(int thread_count, bool contended, AccessorType type) {
  accessor_type = type;
  EngineOptions options = get_tiny_options();
  options.log_.log_buffer_kb_ = 1 << 12;
  options.thread_.group_count_ = 1;
  options.thread_.thread_count_per_group_ = thread_count;
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    {
      CreateTpcbTablesTask task;
      COERCE_ERROR(engine.get_thread_pool().impersonate_synchronous(&task));
    }

    {
      thread::Rendezvous start_rendezvous;
      std::vector<RunTpcbTask*> tasks;
      std::vector<thread::ImpersonateSession> sessions;
      for (int i = 0; i < thread_count; ++i) {
        tasks.push_back(new RunTpcbTask(i, contended, &start_rendezvous));
        sessions.emplace_back(engine.get_thread_pool().impersonate(tasks[i]));
        if (!sessions[i].is_valid()) {
          COERCE_ERROR(sessions[i].invalid_cause_);
        }
      }
      start_rendezvous.signal();
      for (int i = 0; i < thread_count; ++i) {
        COERCE_ERROR(sessions[i].get_result());
        delete tasks[i];
      }
    }
    {
      VerifyTpcbTask task(thread_count);
      COERCE_ERROR(engine.get_thread_pool().impersonate_synchronous(&task));
    }
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(SequentialTpcbTest, SingleThreadedNoContention) { run_test(1, false, kNormal); }
TEST(SequentialTpcbTest, TwoThreadedNoContention)    { run_test(2, false, kNormal); }
TEST(SequentialTpcbTest, FourThreadedNoContention)   { run_test(4, false, kNormal); }

TEST(SequentialTpcbTest, SingleThreadedContended)    { run_test(1, true, kNormal); }
TEST(SequentialTpcbTest, TwoThreadedContended)       { run_test(2, true, kNormal); }
TEST(SequentialTpcbTest, FourThreadedContended)      { run_test(4, true, kNormal); }

TEST(SequentialTpcbTest, SingleThreadedNoContentionPrimitive) { run_test(1, false, kPrimitive); }
TEST(SequentialTpcbTest, TwoThreadedNoContentionPrimitive)    { run_test(2, false, kPrimitive); }
TEST(SequentialTpcbTest, FourThreadedNoContentionPrimitive)   { run_test(4, false, kPrimitive); }

TEST(SequentialTpcbTest, SingleThreadedContendedPrimitive)    { run_test(1, true, kPrimitive); }
TEST(SequentialTpcbTest, TwoThreadedContendedPrimitive)       { run_test(2, true, kPrimitive); }
TEST(SequentialTpcbTest, FourThreadedContendedPrimitive)      { run_test(4, true, kPrimitive); }

TEST(SequentialTpcbTest, SingleThreadedNoContentionInc) { run_test(1, false, kIncrement); }
TEST(SequentialTpcbTest, TwoThreadedNoContentionInc)    { run_test(2, false, kIncrement); }
TEST(SequentialTpcbTest, FourThreadedNoContentionInc)   { run_test(4, false, kIncrement); }

TEST(SequentialTpcbTest, SingleThreadedContendedInc)    { run_test(1, true, kIncrement); }
TEST(SequentialTpcbTest, TwoThreadedContendedInc)       { run_test(2, true, kIncrement); }
TEST(SequentialTpcbTest, FourThreadedContendedInc)      { run_test(4, true, kIncrement); }

TEST(SequentialTpcbTest, SingleThreadedNoContentionInc1S) { run_test(1, false, kIncrementOneShot); }
TEST(SequentialTpcbTest, TwoThreadedNoContentionInc1S)    { run_test(2, false, kIncrementOneShot); }
TEST(SequentialTpcbTest, FourThreadedNoContentionInc1S)   { run_test(4, false, kIncrementOneShot); }

TEST(SequentialTpcbTest, SingleThreadedContendedInc1S)    { run_test(1, true, kIncrementOneShot); }
TEST(SequentialTpcbTest, TwoThreadedContendedInc1S)       { run_test(2, true, kIncrementOneShot); }
TEST(SequentialTpcbTest, FourThreadedContendedInc1S)      { run_test(4, true, kIncrementOneShot); }
}  // namespace sequential
}  // namespace storage
}  // namespace foedus
