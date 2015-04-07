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
#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/storage/array/array_log_types.hpp"

/**
 * @file test_direct_io_file.cpp
 * Testcases for DirectIoFile.
 * We should also run valgrind on this testcase especially for memory leak.
 */
namespace foedus {
namespace fs {
DEFINE_TEST_CASE_PACKAGE(DirectIoFileTest, foedus.fs);

TEST(DirectIoFileTest, Create) {
  DirectIoFile file(Path(std::string("testfile_") + get_random_name()));
  COERCE_ERROR_CODE(file.open(true, true, true, true));
  file.close();
}

void test_tmpfs(std::string root) {
  // this is mainly testing the O_DIRECT error on tmpfs
  // http://www.gossamer-threads.com/lists/linux/kernel/720702
  Path folder_path(root + "/foedus_test");
  if (exists(folder_path)) {
    remove_all(folder_path);
  }
  EXPECT_FALSE(exists(folder_path));
  EXPECT_TRUE(fs::create_directories(folder_path));

  Path file_path(folder_path);
  file_path /= get_random_name();
  EXPECT_FALSE(exists(file_path));

  DirectIoFile file(file_path);
  COERCE_ERROR_CODE(file.open(true, true, true, true));
  file.close();
  remove_all(folder_path);
  EXPECT_FALSE(exists(folder_path));
}

TEST(DirectIoFileTest, CreateDevShm) { test_tmpfs("/dev/shm"); }
TEST(DirectIoFileTest, CreateTmp) { test_tmpfs("/tmp"); }

TEST(DirectIoFileTest, CreateAppend) {
  DirectIoFile file(Path(std::string("testfile_") + get_random_name()));
  memory::AlignedMemory memory(1 << 16, 1 << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
  std::memset(memory.get_block(), 1, 1 << 15);
  COERCE_ERROR_CODE(file.open(true, true, true, true));
  COERCE_ERROR_CODE(file.write(1 << 15, memory));
  COERCE_ERROR_CODE(file.write(1 << 15, memory::AlignedMemorySlice(&memory, 1 << 14, 1 << 15)));
  file.close();
  EXPECT_EQ(1 << 16, file_size(file.get_path()));
}

TEST(DirectIoFileTest, CreateWrite) {
  DirectIoFile file(Path(std::string("testfile_") + get_random_name()));
  memory::AlignedMemory memory(1 << 16, 1 << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
  std::memset(memory.get_block(), 1, 1 << 16);
  COERCE_ERROR_CODE(file.open(true, true, false, true));
  COERCE_ERROR_CODE(file.seek(0, DirectIoFile::kDirectIoSeekSet));
  COERCE_ERROR_CODE(file.write(1 << 15, memory));
  COERCE_ERROR_CODE(file.seek(1 << 14, DirectIoFile::kDirectIoSeekSet));
  COERCE_ERROR_CODE(file.write(1 << 15, memory));
  file.close();
  EXPECT_EQ(3 << 14, file_size(file.get_path()));
}

TEST(DirectIoFileTest, WriteWithLogBuffer) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);

    memory::AlignedMemory memory;
    memory.alloc(1U << 12 , 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
    memory::AlignedMemorySlice log_buf(&memory);

    DirectIoFile file(Path(std::string("testfile_") + get_random_name()));
    std::memset(log_buf.get_block(), 1, 1 << 12);
    COERCE_ERROR_CODE(file.open(true, true, true, true));
    COERCE_ERROR_CODE(file.write(1 << 12, log_buf));
    file.close();
    EXPECT_EQ(1 << 12, file_size(file.get_path()));

    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

TEST(DirectIoFileTest, WriteWithLogBufferPad) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  {
    UninitializeGuard guard(&engine);
    memory::AlignedMemory memory;
    memory.alloc(1U << 12 , 1U << 12, memory::AlignedMemory::kNumaAllocOnnode, 0);
    memory::AlignedMemorySlice log_buf(&memory);

    memory::AlignedMemory fill_buf;
    fill_buf.alloc(
      log::FillerLogType::kLogWriteUnitSize,
      1U << 12,
      memory::AlignedMemory::kNumaAllocOnnode,
      0);

    storage::array::ArrayOverwriteLogType* the_log =
      reinterpret_cast< storage::array::ArrayOverwriteLogType* >(log_buf.get_block());
    char payload[16];
    std::memset(payload, 5, 16);
    the_log->populate(1, 2, payload, 0, 16);

    std::memcpy(fill_buf.get_block(), log_buf.get_block(), the_log->header_.log_length_);

    log::FillerLogType* filler_log =
      reinterpret_cast< log::FillerLogType* >(
        reinterpret_cast<char*>(fill_buf.get_block()) + the_log->header_.log_length_);
    filler_log->populate(
      log::FillerLogType::kLogWriteUnitSize - the_log->header_.log_length_);

    DirectIoFile file(Path(std::string("testfile_") + get_random_name()));
    COERCE_ERROR_CODE(file.open(true, true, true, true));
    COERCE_ERROR_CODE(file.write(log::FillerLogType::kLogWriteUnitSize, fill_buf));
    file.close();
    EXPECT_EQ(log::FillerLogType::kLogWriteUnitSize, file_size(file.get_path()));

    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace fs
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(DirectIoFileTest, foedus.fs);
