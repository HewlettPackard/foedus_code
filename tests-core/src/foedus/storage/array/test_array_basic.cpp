/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/engine.hpp>
#include <foedus/storage/array/array_storage.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/thread/thread_pool.hpp>
#include <foedus/thread/thread.hpp>
#include <gtest/gtest.h>
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

class CreateAndQueryTask : public thread::ImpersonateTask {
 public:
    foedus::ErrorStack run(foedus::thread::Thread* context) {
        ArrayStorage *array =
            dynamic_cast<ArrayStorage*>(
                context->get_engine()->get_storage_manager().get_storage("test2"));
        char buf[16];
        CHECK_ERROR(array->get_record(context, 24, buf));
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
    CreateAndQueryTask task;
    thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
    COERCE_ERROR(session.get_result());
    COERCE_ERROR(engine.uninitialize());
    cleanup_test(options);
}

}  // namespace array
}  // namespace storage
}  // namespace foedus
