/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/engine.hpp>
#include <gtest/gtest.h>

namespace foedus {

TEST(EngineTest, Instantiate) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
}

TEST(EngineTest, Initialize) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    COERCE_ERROR(engine.uninitialize());
}

}  // namespace foedus
