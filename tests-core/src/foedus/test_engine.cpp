/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/engine.hpp>
#include <gtest/gtest.h>

namespace foedus {
DEFINE_TEST_CASE_PACKAGE(EngineTest, foedus);

TEST(EngineTest, Instantiate) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
}

TEST(EngineTest, Initialize) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    COERCE_ERROR(engine.uninitialize());
    cleanup_test(options);
}

TEST(EngineTest, IdempotentUninitialize) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    COERCE_ERROR(engine.uninitialize());
    COERCE_ERROR(engine.uninitialize());
    cleanup_test(options);
}

TEST(EngineTest, Restart) {
    EngineOptions options = get_tiny_options();
    for (int i = 0; i < 2; ++i) {
        Engine engine(options);
        COERCE_ERROR(engine.initialize());
        COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
}

// this is a good test to see if initialize/uninitialize pair is complete.
// eg: aren't we incorrectly relying on constructor of some object, rather than initialize()?
// eg: aren't we lazy to skip some uninitialization, which makes next initialize() crash?
// again, don't do anything in constructor! there is no point to do so. do it in initialize().
TEST(EngineTest, RestartManyTimes) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);  // initialize the same engine many times.
    for (int i = 0; i < 2; ++i) {
        COERCE_ERROR(engine.initialize());
        COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
}

}  // namespace foedus
