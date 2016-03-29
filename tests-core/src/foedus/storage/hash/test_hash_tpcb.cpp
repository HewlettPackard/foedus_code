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
#include "foedus/proc/proc_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_rendezvous.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/storage/hash/hash_storage_pimpl.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_page_impl.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/storage/sequential/sequential_storage_pimpl.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_access.hpp"
#include "foedus/xct/xct_manager.hpp"

/**
 * @file test_hash_tpcb.cpp
 * Hash for main tables and sequential for history.
 */
namespace foedus {
namespace storage {
namespace hash {
DEFINE_TEST_CASE_PACKAGE(HashTpcbTest, foedus.storage.hash);

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

HashStorage  branches;
HashStorage  accounts;
HashStorage  tellers;
sequential::SequentialStorage histories;
bool          use_primitive_accessors = false;
bool          use_increment = false;
int thread_count;
bool contended;
soc::SharedRendezvous start_rendezvous;

/** Creates TPC-B tables and populate with initial records. */
ErrorStack create_tpcb_tables_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  StorageManager* str_manager = context->get_engine()->get_storage_manager();
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  Epoch highest_commit_epoch;
  Epoch commit_epoch;

  // Create branches
  const float kHashPreferredRecordsPerBin = 5.0;
  HashMetadata branch_meta("branches");
  branch_meta.set_capacity(kBranches, kHashPreferredRecordsPerBin);
  COERCE_ERROR(str_manager->create_hash(&branch_meta, &branches, &commit_epoch));
  EXPECT_TRUE(branches.exists());
  COERCE_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  for (uint64_t i = 0; i < kBranches; ++i) {
    BranchData data;
    std::memset(&data, 0, sizeof(data));  // make valgrind happy
    data.branch_balance_ = kInitialAccountBalance * kAccounts;
    COERCE_ERROR(branches.insert_record(context, i, &data, sizeof(data)));
  }
  COERCE_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  COERCE_ERROR(branches.verify_single_thread(context));
  highest_commit_epoch.store_max(commit_epoch);

  // Create tellers
  HashMetadata teller_meta("tellers");
  teller_meta.set_capacity(kBranches * kTellers, kHashPreferredRecordsPerBin);
  COERCE_ERROR(str_manager->create_hash(&teller_meta, &tellers, &commit_epoch));
  EXPECT_TRUE(tellers.exists());
  COERCE_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  for (uint64_t i = 0; i < kBranches * kTellers; ++i) {
    TellerData data;
    std::memset(&data, 0, sizeof(data));  // make valgrind happy
    data.branch_id_ = i / kTellers;
    data.teller_balance_ = kInitialAccountBalance * kAccountsPerTellers;
    COERCE_ERROR(tellers.insert_record(context, i, &data, sizeof(data)));
  }
  COERCE_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  COERCE_ERROR(tellers.verify_single_thread(context));
  highest_commit_epoch.store_max(commit_epoch);

  // Create accounts
  HashMetadata account_meta("accounts");
  account_meta.set_capacity(kBranches * kAccounts, kHashPreferredRecordsPerBin);
  COERCE_ERROR(str_manager->create_hash(&account_meta, &accounts, &commit_epoch));
  EXPECT_TRUE(accounts.exists());
  COERCE_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  for (uint64_t i = 0; i < kBranches * kAccounts; ++i) {
    AccountData data;
    std::memset(&data, 0, sizeof(data));  // make valgrind happy
    data.branch_id_ = i / kAccounts;
    data.account_balance_ = kInitialAccountBalance;
    COERCE_ERROR(accounts.insert_record(context, i, &data, sizeof(data)));
  }
  COERCE_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  COERCE_ERROR(accounts.verify_single_thread(context));
  highest_commit_epoch.store_max(commit_epoch);

  // Create histories
  sequential::SequentialMetadata history_meta("histories");
  COERCE_ERROR(str_manager->create_sequential(&history_meta, &histories, &commit_epoch));
  EXPECT_TRUE(histories.exists());
  COERCE_ERROR(xct_manager->begin_xct(context, xct::kSerializable));
  COERCE_ERROR(xct_manager->precommit_xct(context, &commit_epoch));
  highest_commit_epoch.store_max(commit_epoch);

  CHECK_ERROR(xct_manager->wait_for_commit(highest_commit_epoch));
  return kRetOk;
}

/** run TPC-B queries. */
class RunTpcbTask {
 public:
  explicit RunTpcbTask(int client_id) : client_id_(client_id) {
    ASSERT_ND(client_id < kMaxTestThreads);
  }
  ErrorStack run(thread::Thread* context) {
    start_rendezvous.wait();
    assorted::UniformRandom rand;
    rand.set_current_seed(client_id_);
    Epoch highest_commit_epoch;
    xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
    xct::XctId prev_xct_id;
    for (int i = 0; i < kXctsPerThread; ++i) {
      uint64_t account_id;
      if (contended) {
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
            CHECK_ERROR(xct_manager->abort_xct(context));
          }
        } else {
          std::cout << "Unexpected error! Thread-" << context->get_thread_id()
            << " branch=" << branch_id
            << " teller=" << teller_id
            << " account=" << account_id
            << " history=" << history_id
            << " amount=" << amount
            << " " << error_stack
            << std::endl;
          COERCE_ERROR(error_stack);
        }
      }
    }
    CHECK_ERROR(xct_manager->wait_for_commit(highest_commit_epoch));
    return foedus::kRetOk;
  }

  ErrorStack try_transaction(
    thread::Thread* context,
    Epoch *highest_commit_epoch,
    uint64_t branch_id,
    uint64_t teller_id,
    uint64_t account_id,
    uint64_t history_id,
    int64_t amount) {
    xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
    WRAP_ERROR_CODE(xct_manager->begin_xct(context, xct::kSerializable));

    int64_t branch_balance_old = -1, branch_balance_new;
    if (use_increment) {
      branch_balance_new = amount;
      WRAP_ERROR_CODE(branches.increment_record(context, branch_id, &branch_balance_new, 0));
      branch_balance_old = branch_balance_new - amount;
    } else if (use_primitive_accessors) {
      WRAP_ERROR_CODE(branches.get_record_primitive(
        context,
        branch_id,
        &branch_balance_old,
        0,
        false));
      branch_balance_new = branch_balance_old + amount;
      WRAP_ERROR_CODE(branches.overwrite_record_primitive(
        context,
        branch_id,
        branch_balance_new,
        0));
    } else {
      BranchData branch;
      uint16_t capacity = sizeof(branch);
      WRAP_ERROR_CODE(branches.get_record(context, branch_id, &branch, &capacity, false));
      branch_balance_old = branch.branch_balance_;
      branch_balance_new = branch_balance_old + amount;
      WRAP_ERROR_CODE(branches.overwrite_record(
        context,
        branch_id,
        &branch_balance_new,
        0,
        sizeof(branch_balance_new)));
    }
    EXPECT_GE(branch_balance_old, 0);

    int64_t teller_balance_old = -1, teller_balance_new;
    uint64_t teller_branch_id = 0;
    if (use_increment) {
      WRAP_ERROR_CODE(tellers.get_record_primitive(
        context,
        teller_id,
        &teller_branch_id,
        0,
        false));
      teller_balance_new = amount;
      WRAP_ERROR_CODE(tellers.increment_record(
        context,
        teller_id,
        &teller_balance_new,
        sizeof(uint64_t)));
      teller_balance_old = teller_balance_new - amount;
    } else if (use_primitive_accessors) {
      WRAP_ERROR_CODE(tellers.get_record_primitive(
        context,
        teller_id,
        &teller_branch_id,
        0,
        false));
      WRAP_ERROR_CODE(tellers.get_record_primitive(
        context,
        teller_id,
        &teller_balance_old,
        sizeof(uint64_t),
        false));
      teller_balance_new = teller_balance_old + amount;
      WRAP_ERROR_CODE(tellers.overwrite_record_primitive(
        context,
        teller_id,
        teller_balance_new,
        sizeof(uint64_t)));
    } else {
      TellerData teller;
      uint16_t capacity = sizeof(teller);
      WRAP_ERROR_CODE(tellers.get_record(context, teller_id, &teller, &capacity, false));
      teller_branch_id = teller.branch_id_;
      teller_balance_old = teller.teller_balance_;
      teller_balance_new = teller_balance_old + amount;
      WRAP_ERROR_CODE(tellers.overwrite_record(
        context,
        teller_id,
        &teller_balance_new,
        sizeof(teller.branch_id_),
        sizeof(teller_balance_new)));
    }
    EXPECT_GE(teller_balance_old, 0);
    EXPECT_EQ(branch_id, teller_branch_id);

    int64_t account_balance_old = -1, account_balance_new;
    uint64_t account_branch_id = 0;
    if (use_increment) {
      WRAP_ERROR_CODE(accounts.get_record_primitive(
        context,
        account_id,
        &account_branch_id,
        0,
        false));
      account_balance_new = amount;
      WRAP_ERROR_CODE(accounts.increment_record(
        context,
        account_id,
        &account_balance_new,
        sizeof(uint64_t)));
      account_balance_old = account_balance_new - amount;
    } else if (use_primitive_accessors) {
      WRAP_ERROR_CODE(accounts.get_record_primitive(
        context,
        account_id,
        &account_branch_id,
        0,
        false));
      WRAP_ERROR_CODE(accounts.get_record_primitive(
        context,
        account_id,
        &account_balance_old,
        sizeof(uint64_t),
        false));
      account_balance_new = account_balance_old + amount;
      WRAP_ERROR_CODE(accounts.overwrite_record_primitive(
        context,
        account_id,
        account_balance_new,
        sizeof(uint64_t)));
    } else {
      AccountData account;
      uint16_t capacity = sizeof(account);
      WRAP_ERROR_CODE(accounts.get_record(context, account_id, &account, &capacity, false));
      account_branch_id = account.branch_id_;
      account_balance_old = account.account_balance_;
      account_balance_new = account_balance_old + amount;
      WRAP_ERROR_CODE(accounts.overwrite_record(
        context,
        account_id,
        &account_balance_new,
        sizeof(account.branch_id_),
        sizeof(account_balance_new)));
    }
    EXPECT_GE(account_balance_old, 0);
    EXPECT_EQ(branch_id, account_branch_id);

    HistoryData history;
    history.history_id_ = history_id;
    history.account_id_ = account_id;
    history.branch_id_ = branch_id;
    history.teller_id_ = teller_id;
    history.amount_ = amount;
    std::memset(history.other_data_, 0, sizeof(history.other_data_));  // make valgrind happy
    WRAP_ERROR_CODE(histories.append_record(context, &history, sizeof(history)));

    Epoch commit_epoch;
    ASSERT_ND(context->get_current_xct().get_read_set_size() > 0);
    ASSERT_ND(context->get_current_xct().get_write_set_size() > 0);
    WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));

    std::cout << "Committed! Thread-" << context->get_thread_id() << " Updated "
      << " branch[" << branch_id << "] " << branch_balance_old << " -> " << branch_balance_new
      << " teller[" << teller_id << "] " << teller_balance_old << " -> " << teller_balance_new
      << " account[" << account_id << "] " << account_balance_old
        << " -> " << account_balance_new
      << " history[" << history_id << "] amount=" << amount
      << std::endl;
    highest_commit_epoch->store_max(commit_epoch);
    return kRetOk;
  }

 private:
  int                         client_id_;
};


ErrorStack run_tpcb_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  EXPECT_EQ(sizeof(int), args.input_len_);
  return RunTpcbTask(*reinterpret_cast<const int*>(args.input_buffer_)).run(context);
}

/** Verify TPC-B results. */
ErrorStack verify_tpcb_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  COERCE_ERROR(accounts.verify_single_thread(context));
  COERCE_ERROR(branches.verify_single_thread(context));
  COERCE_ERROR(tellers.verify_single_thread(context));
  xct::XctManager* xct_manager = context->get_engine()->get_xct_manager();
  CHECK_ERROR(xct_manager->begin_xct(context, xct::kSerializable));

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
  WRAP_ERROR_CODE(sequential::SequentialStoragePimpl(
    context->get_engine(), histories.get_control_block()).for_every_page(
    [&](sequential::SequentialPage* page){
      uint16_t record_count = page->get_record_count();
      const char* record_pointers[sequential::kMaxSlots];
      uint16_t payload_lengthes[sequential::kMaxSlots];
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
      return kErrorCodeOk;
  }));
  EXPECT_EQ(kXctsPerThread * thread_count, observed_history_ids.size());
  for (uint64_t i = 0; i < static_cast<uint64_t>(kXctsPerThread * thread_count); ++i) {
    EXPECT_NE(observed_history_ids.end(), observed_history_ids.find(i)) << i;
  }

  for (uint64_t i = 0; i < kBranches; ++i) {
    BranchData data;
    uint16_t capacity = sizeof(data);
    CHECK_ERROR(branches.get_record(context, i, &data, &capacity, true));
    EXPECT_EQ(expected_branch[i], data.branch_balance_) << "branch-" << i;
  }
  for (uint64_t i = 0; i < kBranches * kTellers; ++i) {
    TellerData data;
    uint16_t capacity = sizeof(data);
    CHECK_ERROR(tellers.get_record(context, i, &data, &capacity, true));
    EXPECT_EQ(i / kTellers, data.branch_id_) << i;
    EXPECT_EQ(expected_teller[i], data.teller_balance_) << "teller-" << i;
  }
  for (uint64_t i = 0; i < kBranches * kAccounts; ++i) {
    AccountData data;
    uint16_t capacity = sizeof(data);
    CHECK_ERROR(accounts.get_record(context, i, &data, &capacity, true));
    EXPECT_EQ(i / kAccounts, data.branch_id_) << i;
    EXPECT_EQ(expected_account[i], data.account_balance_) << "account-" << i;
  }
  for (uint32_t i = 0; i < context->get_current_xct().get_read_set_size(); ++i) {
    xct::ReadXctAccess& access = context->get_current_xct().get_read_set()[i];
    EXPECT_FALSE(access.observed_owner_id_.is_being_written()) << i;
    EXPECT_FALSE(access.observed_owner_id_.is_deleted()) << i;
    EXPECT_FALSE(access.observed_owner_id_.is_moved()) << i;
  }

  CHECK_ERROR(xct_manager->abort_xct(context));
  return foedus::kRetOk;
}

void multi_thread_test(int thread_count_arg, bool contended_arg,
             bool use_primitive = false, bool use_inc = false) {
  thread_count = thread_count_arg;
  contended = contended_arg;
  use_primitive_accessors = use_primitive;
  use_increment = use_inc;
  EngineOptions options = get_tiny_options();
  options.memory_.page_pool_size_mb_per_node_ = 32;
  options.memory_.page_pool_size_mb_per_node_ *= 2U;  // for rigorous_check
  options.log_.log_buffer_kb_ = 1 << 12;
  options.thread_.group_count_ = 1;
  options.thread_.thread_count_per_group_ = thread_count;
  Engine engine(options);
  engine.get_proc_manager()->pre_register("create_tpcb_tables_task", create_tpcb_tables_task);
  engine.get_proc_manager()->pre_register("run_tpcb_task", run_tpcb_task);
  engine.get_proc_manager()->pre_register("verify_tpcb_task", verify_tpcb_task);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("create_tpcb_tables_task"));

    {
      start_rendezvous.initialize();
      std::vector<thread::ImpersonateSession> sessions;
      for (int i = 0; i < thread_count; ++i) {
        thread::ImpersonateSession session;
        EXPECT_TRUE(
          engine.get_thread_pool()->impersonate("run_tpcb_task", &i, sizeof(i), &session));
        sessions.emplace_back(std::move(session));
      }
      start_rendezvous.signal();
      for (int i = 0; i < thread_count; ++i) {
        COERCE_ERROR(sessions[i].get_result());
        sessions[i].release();
      }
      start_rendezvous.uninitialize();
    }
    COERCE_ERROR(engine.get_thread_pool()->impersonate_synchronous("verify_tpcb_task"));
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(HashTpcbTest, SingleThreadedNoContention) { multi_thread_test(1, false); }
TEST(HashTpcbTest, TwoThreadedNoContention)    { multi_thread_test(2, false); }
TEST(HashTpcbTest, FourThreadedNoContention)   { multi_thread_test(4, false); }

TEST(HashTpcbTest, SingleThreadedContended)    { multi_thread_test(1, true); }
TEST(HashTpcbTest, TwoThreadedContended)       { multi_thread_test(2, true); }
TEST(HashTpcbTest, FourThreadedContended)      { multi_thread_test(4, true); }

TEST(HashTpcbTest, SingleThreadedNoContentionPrimitive) { multi_thread_test(1, false, true); }
TEST(HashTpcbTest, TwoThreadedNoContentionPrimitive)    { multi_thread_test(2, false, true); }
TEST(HashTpcbTest, FourThreadedNoContentionPrimitive)   { multi_thread_test(4, false, true); }

TEST(HashTpcbTest, SingleThreadedContendedPrimitive)    { multi_thread_test(1, true, true); }
TEST(HashTpcbTest, TwoThreadedContendedPrimitive)       { multi_thread_test(2, true, true); }
TEST(HashTpcbTest, FourThreadedContendedPrimitive)      { multi_thread_test(4, true, true); }

TEST(HashTpcbTest, SingleThreadedNoContentionInc) { multi_thread_test(1, false, true, true); }
TEST(HashTpcbTest, TwoThreadedNoContentionInc)    { multi_thread_test(2, false, true, true); }
TEST(HashTpcbTest, FourThreadedNoContentionInc)   { multi_thread_test(4, false, true, true); }

TEST(HashTpcbTest, SingleThreadedContendedInc)    { multi_thread_test(1, true, true, true); }
TEST(HashTpcbTest, TwoThreadedContendedInc)       { multi_thread_test(2, true, true, true); }
TEST(HashTpcbTest, FourThreadedContendedInc)      { multi_thread_test(4, true, true, true); }
}  // namespace hash
}  // namespace storage
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(HashTpcbTest, foedus.storage.hash);
