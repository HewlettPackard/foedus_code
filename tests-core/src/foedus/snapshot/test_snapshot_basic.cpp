/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/snapshot/snapshot_manager.hpp>
#include <foedus/storage/storage_manager.hpp>
#include <foedus/storage/array/fwd.hpp>
#include <foedus/log/log_manager.hpp>
#include <foedus/xct/xct_manager.hpp>
#include <gtest/gtest.h>
/**
 * @file test_snapshot_basic.cpp
 * Easy testcases for snapshotting.
 * These don't involve data, multiple snapshots, etc etc.
 */
namespace foedus {
namespace snapshot {

TEST(SnapshotBasicTest, Empty) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
        UninitializeGuard guard(&engine);
        engine.get_snapshot_manager().trigger_snapshot_immediate(true);
        COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
}

TEST(SnapshotBasicTest, OneArrayCreate) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
        UninitializeGuard guard(&engine);
        storage::array::ArrayStorage* out;
        Epoch commit_epoch;
        COERCE_ERROR(engine.get_storage_manager().create_array_impersonate("test", 16, 100, &out,
            &commit_epoch));
        EXPECT_TRUE(out != nullptr);
        EXPECT_TRUE(commit_epoch.is_valid());
        COERCE_ERROR(engine.get_xct_manager().wait_for_commit(commit_epoch));
        engine.get_snapshot_manager().trigger_snapshot_immediate(true);
        COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
}

TEST(SnapshotBasicTest, TwoArrayCreate) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
        UninitializeGuard guard(&engine);
        storage::array::ArrayStorage* out;
        Epoch commit_epoch;
        COERCE_ERROR(engine.get_storage_manager().create_array_impersonate("test", 16, 10, &out,
            &commit_epoch));
        EXPECT_TRUE(out != nullptr);
        storage::array::ArrayStorage* out2;
        COERCE_ERROR(engine.get_storage_manager().create_array_impersonate("test2", 16, 10, &out2,
            &commit_epoch));
        EXPECT_TRUE(out2 != nullptr);

        EXPECT_TRUE(commit_epoch.is_valid());
        COERCE_ERROR(engine.get_xct_manager().wait_for_commit(commit_epoch));
        engine.get_snapshot_manager().trigger_snapshot_immediate(true);
        COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
}

}  // namespace snapshot
}  // namespace foedus
