/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/epoch.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/thread/rendezvous_impl.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/xct/xct_access.hpp>
#include <foedus/xct/xct_id.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <stdint.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

namespace foedus {
namespace xct {


struct Payload {
    uint64_t id_;
    uint64_t data_;
};
const int kRecords = 10;
const int kThreads = 10;
static_assert(kRecords >= kThreads, "booo!");
storage::array::ArrayStorage* storage = nullptr;

class InitTask : public thread::ImpersonateTask {
 public:
    ErrorStack run(thread::Thread* context) {
        xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
        storage::StorageManager& str_manager = context->get_engine()->get_storage_manager();
        Epoch commit_epoch;
        CHECK_ERROR(str_manager.create_array(context, "test", sizeof(Payload), kRecords, &storage,
            &commit_epoch));

        CHECK_ERROR(xct_manager.begin_xct(context, SERIALIZABLE));

        for (int i = 0; i < kRecords; ++i) {
            Payload payload;
            payload.id_ = i;
            payload.data_ = 0;
            CHECK_ERROR(storage->overwrite_record(context, i, &payload));
        }

        CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
        return RET_OK;
    }
};


class TestTask : public thread::ImpersonateTask {
 public:
    TestTask(uint64_t offset, uint64_t amount, thread::Rendezvous* start_rendezvous)
        : offset_(offset), amount_(amount), start_rendezvous_(start_rendezvous) {}
    ErrorStack run(thread::Thread* context) {
        start_rendezvous_->wait();
        xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
        while (true) {
            ErrorStack error_stack = try_transaction(context);
            if (!error_stack.is_error()) {
                break;
            } else if (error_stack.get_error_code() == ERROR_CODE_XCT_RACE_ABORT) {
                // abort and retry
                if (context->is_running_xct()) {
                    CHECK_ERROR(xct_manager.abort_xct(context));
                }
            } else {
                COERCE_ERROR(error_stack);
            }
        }
        return RET_OK;
    }

    ErrorStack try_transaction(thread::Thread* context) {
        xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
        CHECK_ERROR(xct_manager.begin_xct(context, SERIALIZABLE));

        Payload payload;
        CHECK_ERROR(storage->get_record(context, offset_, &payload));
        EXPECT_EQ(offset_, payload.id_);
        payload.data_ += amount_;
        CHECK_ERROR(storage->overwrite_record(context, offset_, &payload));

        Epoch commit_epoch;
        CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
        return RET_OK;
    }

 private:
    uint64_t offset_;
    uint64_t amount_;
    thread::Rendezvous* start_rendezvous_;
};

class GetAllRecordsTask : public thread::ImpersonateTask {
 public:
    explicit GetAllRecordsTask(Payload* output) : output_(output) {}
    ErrorStack run(thread::Thread* context) {
        xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
        CHECK_ERROR(xct_manager.begin_xct(context, SERIALIZABLE));

        for (int i = 0; i < kRecords; ++i) {
            CHECK_ERROR(storage->get_record(context, i, output_ + i));
        }

        Epoch commit_epoch;
        CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
        return RET_OK;
    }

 private:
    Payload* output_;
};

template <typename ASSIGN_FUNC>
void run_test(Engine *engine, ASSIGN_FUNC assign_func) {
    InitTask init_task;
    COERCE_ERROR(engine->get_thread_pool().impersonate(&init_task).get_result());

    thread::Rendezvous start_rendezvous;
    std::vector<TestTask*>      tasks;
    std::vector<thread::ImpersonateSession> sessions;
    for (int i = 0; i < kThreads; ++i) {
        tasks.push_back(new TestTask(assign_func(i), i * 20 + 4, &start_rendezvous));
        sessions.emplace_back(engine->get_thread_pool().impersonate(tasks[i]));
        if (!sessions[i].is_valid()) {
            COERCE_ERROR(sessions[i].invalid_cause_);
        }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    start_rendezvous.signal();
    for (int i = 0; i < kThreads; ++i) {
        COERCE_ERROR(sessions[i].get_result());
        delete tasks[i];
    }

    uint64_t answers[kThreads];
    std::memset(answers, 0, sizeof(answers));
    for (int i = 0; i < kThreads; ++i) {
        answers[assign_func(i)] += i * 20 + 4;
    }
    Payload payloads[kRecords];
    GetAllRecordsTask getall_task(payloads);
    COERCE_ERROR(engine->get_thread_pool().impersonate(&getall_task).get_result());
    for (int i = 0; i < kThreads; ++i) {
        EXPECT_EQ(i, payloads[i].id_);
        EXPECT_EQ(answers[i], payloads[i].data_);
    }
}

TEST(XctCommitConflictTest, NoConflict) {
    EngineOptions options = get_tiny_options();
    options.thread_.thread_count_per_group_ = kThreads;
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
        UninitializeGuard guard(&engine);
        run_test(&engine, [] (int i) { return i; } );  // no conflict
        COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
}

TEST(XctCommitConflictTest, LightConflict) {
    EngineOptions options = get_tiny_options();
    options.thread_.thread_count_per_group_ = kThreads;
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
        UninitializeGuard guard(&engine);
        run_test(&engine, [] (int i) { return i / 2; } );  // two threads per record
        COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
}

TEST(XctCommitConflictTest, HeavyConflict) {
    EngineOptions options = get_tiny_options();
    options.thread_.thread_count_per_group_ = kThreads;
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
        UninitializeGuard guard(&engine);
        run_test(&engine, [] (int i) { return i / 5; } );
        COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
}

TEST(XctCommitConflictTest, ExtremeConflict) {
    EngineOptions options = get_tiny_options();
    options.thread_.thread_count_per_group_ = kThreads;
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
        UninitializeGuard guard(&engine);
        run_test(&engine, [] (int /*i*/) { return 0; } );
        COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
}

}  // namespace xct
}  // namespace foedus
