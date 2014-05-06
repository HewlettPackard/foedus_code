/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/engine.hpp>
#include <foedus/epoch.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <gtest/gtest.h>
#include <cstring>
#include <iostream>

namespace foedus {
namespace storage {
namespace array {
TEST(ArrayBasicTest, Create) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    ArrayStorage* out;
    COERCE_ERROR(engine.get_storage_manager().create_array_impersonate("test", 16, 100, &out));
    EXPECT_TRUE(out != nullptr);
    COERCE_ERROR(engine.uninitialize());
    cleanup_test(options);
}

class QueryTask : public thread::ImpersonateTask {
 public:
    ErrorStack run(thread::Thread* context) {
        ArrayStorage *array =
            dynamic_cast<ArrayStorage*>(
                context->get_engine()->get_storage_manager().get_storage("test2"));
        char buf[16];
        xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
        CHECK_ERROR(xct_manager.begin_xct(context, xct::SERIALIZABLE));

        CHECK_ERROR(array->get_record(context, 24, buf));

        Epoch commit_epoch;
        CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
        CHECK_ERROR(xct_manager.wait_for_commit(commit_epoch));
        return foedus::RET_OK;
    }
};

TEST(ArrayBasicTest, CreateAndQuery) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    ArrayStorage* out;
    COERCE_ERROR(engine.get_storage_manager().create_array_impersonate("test2", 16, 100, &out));
    EXPECT_TRUE(out != nullptr);
    QueryTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
    cleanup_test(options);
}


class WriteTask : public thread::ImpersonateTask {
 public:
    ErrorStack run(thread::Thread* context) {
        ArrayStorage *array =
            dynamic_cast<ArrayStorage*>(
                context->get_engine()->get_storage_manager().get_storage("test3"));
        char buf[16];
        std::memset(buf, 2, 16);
        xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();
        CHECK_ERROR(xct_manager.begin_xct(context, xct::SERIALIZABLE));

        CHECK_ERROR(array->overwrite_record(context, 24, buf));

        Epoch commit_epoch;
        CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
        CHECK_ERROR(xct_manager.wait_for_commit(commit_epoch));
        return foedus::RET_OK;
    }
};

TEST(ArrayBasicTest, CreateAndWrite) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    ArrayStorage* out;
    COERCE_ERROR(engine.get_storage_manager().create_array_impersonate("test3", 16, 100, &out));
    EXPECT_TRUE(out != nullptr);
    WriteTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
    cleanup_test(options);
}

class ReadWriteTask : public thread::ImpersonateTask {
 public:
    ErrorStack run(thread::Thread* context) {
        ArrayStorage *array =
            dynamic_cast<ArrayStorage*>(
                context->get_engine()->get_storage_manager().get_storage("test4"));
        xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();

        // Write values first
        CHECK_ERROR(xct_manager.begin_xct(context, xct::SERIALIZABLE));
        for (int i = 0; i < 100; ++i) {
            uint64_t buf[2];
            buf[0] = i * 46 + 123;
            buf[1] = i * 6534 + 665;
            CHECK_ERROR(array->overwrite_record(context, i, buf));
        }
        Epoch commit_epoch;
        CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
        CHECK_ERROR(xct_manager.wait_for_commit(commit_epoch));

        // Then, read values
        CHECK_ERROR(xct_manager.begin_xct(context, xct::SERIALIZABLE));
        for (int i = 0; i < 100; ++i) {
            uint64_t buf[2];
            CHECK_ERROR(array->get_record(context, i, buf));
            EXPECT_EQ(i * 46 + 123, buf[0]);
            EXPECT_EQ(i * 6534 + 665, buf[1]);
        }
        CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
        CHECK_ERROR(xct_manager.wait_for_commit(commit_epoch));
        return foedus::RET_OK;
    }
};

TEST(ArrayBasicTest, CreateAndReadWrite) {
    EngineOptions options = get_tiny_options();
    options.log_.log_buffer_kb_ = 1 << 10;  // larger to do all writes in one shot
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    ArrayStorage* out;
    COERCE_ERROR(engine.get_storage_manager().create_array_impersonate("test4", 16, 100, &out));
    EXPECT_TRUE(out != nullptr);
    ReadWriteTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
    cleanup_test(options);
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
