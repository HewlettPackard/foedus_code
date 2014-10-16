/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/storage/sequential/sequential_storage.hpp"
#include "foedus/xct/xct_manager.hpp"

/**
 * @file test_restart_meta.cpp
 * Testcases for metadata log handling in restart.
 * No data operation, just metadata logs.
 */
namespace foedus {
namespace restart {
DEFINE_TEST_CASE_PACKAGE(RestartMetaTest, foedus.restart);

TEST(RestartMetaTest, Empty) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    COERCE_ERROR(engine.uninitialize());
  }
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(RestartMetaTest, OneArray) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  storage::StorageId array_id;
  {
    UninitializeGuard guard(&engine);
    storage::array::ArrayStorage out;
    Epoch commit_epoch;
    storage::array::ArrayMetadata meta("test", 16, 100);
    COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out.exists());
    array_id = out.get_id();
    EXPECT_TRUE(commit_epoch.is_valid());
    COERCE_ERROR(engine.get_xct_manager()->wait_for_commit(commit_epoch));
    COERCE_ERROR(engine.uninitialize());
  }
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    storage::array::ArrayStorage out(&engine, "test");
    EXPECT_TRUE(out.exists());
    EXPECT_EQ(array_id, out.get_id());
    EXPECT_EQ(std::string("test"), out.get_name().str());
    EXPECT_EQ(storage::kArrayStorage, out.get_type());
    EXPECT_EQ(100, out.get_array_size());
    EXPECT_EQ(16, out.get_payload_size());

    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(RestartMetaTest, OneArrayOneSequential) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  storage::StorageId array_id;
  storage::StorageId seq_id;
  {
    UninitializeGuard guard(&engine);
    Epoch commit_epoch;

    storage::array::ArrayStorage out;
    storage::array::ArrayMetadata meta("test", 16, 100);
    COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out.exists());
    array_id = out.get_id();
    EXPECT_TRUE(commit_epoch.is_valid());

    storage::sequential::SequentialMetadata meta2("test2");
    storage::sequential::SequentialStorage out2;
    COERCE_ERROR(engine.get_storage_manager()->create_sequential(&meta2, &out2, &commit_epoch));
    EXPECT_TRUE(out2.exists());
    seq_id = out2.get_id();
    EXPECT_TRUE(commit_epoch.is_valid());

    COERCE_ERROR(engine.get_xct_manager()->wait_for_commit(commit_epoch));
    COERCE_ERROR(engine.uninitialize());
  }
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    storage::array::ArrayStorage out(&engine, "test");
    EXPECT_TRUE(out.exists());
    EXPECT_EQ(array_id, out.get_id());
    EXPECT_EQ(std::string("test"), out.get_name().str());
    EXPECT_EQ(storage::kArrayStorage, out.get_type());
    EXPECT_EQ(100, out.get_array_size());
    EXPECT_EQ(16, out.get_payload_size());

    storage::sequential::SequentialStorage out2(&engine, "test2");
    EXPECT_TRUE(out2.exists());
    EXPECT_EQ(seq_id, out2.get_id());
    EXPECT_EQ(std::string("test2"), out2.get_name().str());
    EXPECT_EQ(storage::kSequentialStorage, out2.get_type());

    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(RestartMetaTest, OneMasstree) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  storage::StorageId masstree_id;
  {
    UninitializeGuard guard(&engine);
    storage::masstree::MasstreeStorage out;
    Epoch commit_epoch;
    storage::masstree::MasstreeMetadata meta("test", 30U);
    COERCE_ERROR(engine.get_storage_manager()->create_masstree(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out.exists());
    masstree_id = out.get_id();
    EXPECT_TRUE(commit_epoch.is_valid());
    COERCE_ERROR(engine.get_xct_manager()->wait_for_commit(commit_epoch));
    COERCE_ERROR(engine.uninitialize());
  }
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    storage::masstree::MasstreeStorage out(&engine, "test");
    EXPECT_TRUE(out.exists());
    EXPECT_EQ(masstree_id, out.get_id());
    EXPECT_EQ(std::string("test"), out.get_name().str());
    EXPECT_EQ(storage::kMasstreeStorage, out.get_type());
    EXPECT_EQ(30U, out.get_masstree_metadata()->border_early_split_threshold_);

    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(RestartMetaTest, CreateDropCreate) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  storage::StorageId array_id;
  storage::StorageId seq_id;
  {
    UninitializeGuard guard(&engine);
    Epoch commit_epoch;

    storage::array::ArrayStorage out;
    storage::array::ArrayMetadata meta("test", 16, 100);
    COERCE_ERROR(engine.get_storage_manager()->create_array(&meta, &out, &commit_epoch));
    EXPECT_TRUE(out.exists());
    array_id = out.get_id();
    EXPECT_TRUE(commit_epoch.is_valid());

    COERCE_ERROR(engine.get_storage_manager()->drop_storage(array_id, &commit_epoch));
    EXPECT_FALSE(out.exists());
    EXPECT_TRUE(commit_epoch.is_valid());

    storage::sequential::SequentialMetadata meta2("test2");
    storage::sequential::SequentialStorage out2;
    COERCE_ERROR(engine.get_storage_manager()->create_sequential(&meta2, &out2, &commit_epoch));
    EXPECT_TRUE(out2.exists());
    seq_id = out2.get_id();
    EXPECT_TRUE(commit_epoch.is_valid());

    COERCE_ERROR(engine.get_xct_manager()->wait_for_commit(commit_epoch));
    COERCE_ERROR(engine.uninitialize());
  }
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    storage::array::ArrayStorage out(&engine, "test");
    EXPECT_FALSE(out.exists());
    storage::array::ArrayStorage out_id(&engine, array_id);
    EXPECT_FALSE(out_id.exists());

    storage::sequential::SequentialStorage out2(&engine, "test2");
    EXPECT_TRUE(out2.exists());
    EXPECT_EQ(seq_id, out2.get_id());
    EXPECT_EQ(std::string("test2"), out2.get_name().str());
    EXPECT_EQ(storage::kSequentialStorage, out2.get_type());

    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace restart
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(RestartMetaTest, foedus.restart);
