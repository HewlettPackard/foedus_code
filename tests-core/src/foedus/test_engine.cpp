/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/engine.hpp>
#include <gtest/gtest.h>

TEST(EngineTest, Instantiate) {
    foedus::EngineOptions options = foedus::get_tiny_options();
    foedus::Engine engine(options);
}

TEST(EngineTest, Initialize) {
    foedus::EngineOptions options = foedus::get_tiny_options();
    foedus::Engine engine(options);
    COERCE_ERROR(engine.initialize());
    COERCE_ERROR(engine.uninitialize());
}
