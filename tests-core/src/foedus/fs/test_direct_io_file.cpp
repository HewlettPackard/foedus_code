/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/fs/direct_io_file.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/memory/aligned_memory.hpp>
#include <gtest/gtest.h>
#include <cstring>
#include <string>
/**
 * @file test_direct_io_file.cpp
 * Testcases for DirectIoFile.
 * We should also run valgrind on this testcase especially for memory leak.
 */
namespace foedus {
namespace fs {

TEST(DirectIoFileTest, Create) {
    DirectIoFile file(Path(std::string("testfile_") + get_random_name()));
    COERCE_ERROR(file.open(true, true, true, true));
    file.close();
}

TEST(DirectIoFileTest, CreateAppend) {
    DirectIoFile file(Path(std::string("testfile_") + get_random_name()));
    memory::AlignedMemory memory(1 << 16, 1 << 12, memory::AlignedMemory::NUMA_ALLOC_ONNODE, 0);
    std::memset(memory.get_block(), 1, 1 << 15);
    COERCE_ERROR(file.open(true, true, true, true));
    COERCE_ERROR(file.write(1 << 15, memory));
    COERCE_ERROR(file.write(1 << 15, memory::AlignedMemorySlice(&memory, 1 << 14, 1 << 15)));
    file.close();
    EXPECT_EQ(1 << 16, file_size(file.get_path()));
}

TEST(DirectIoFileTest, CreateWrite) {
    DirectIoFile file(Path(std::string("testfile_") + get_random_name()));
    memory::AlignedMemory memory(1 << 16, 1 << 12, memory::AlignedMemory::NUMA_ALLOC_ONNODE, 0);
    std::memset(memory.get_block(), 1, 1 << 16);
    COERCE_ERROR(file.open(true, true, false, true));
    COERCE_ERROR(file.seek(0, DirectIoFile::DIRECT_IO_SEEK_SET));
    COERCE_ERROR(file.write(1 << 15, memory));
    COERCE_ERROR(file.seek(1 << 14, DirectIoFile::DIRECT_IO_SEEK_SET));
    COERCE_ERROR(file.write(1 << 15, memory));
    file.close();
    EXPECT_EQ(3 << 14, file_size(file.get_path()));
}

}  // namespace fs
}  // namespace foedus
