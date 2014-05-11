/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/engine.hpp>
#include <foedus/memory/aligned_memory.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/memory/numa_node_memory.hpp>
#include <foedus/memory/numa_core_memory.hpp>
#include <foedus/xct/xct_access.hpp>
#include <gtest/gtest.h>
#include <set>

namespace foedus {
namespace memory {

TEST(EngineMemoryTest, ReadWriteSetMemory) {
    EngineOptions options = get_tiny_options();
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
        UninitializeGuard guard(&engine);
        EngineMemory& memory = engine.get_memory_manager();
        EXPECT_EQ(options.thread_.group_count_, memory.get_node_memory_count());

        std::set< void* > read_sets;  // for duplicate check
        std::set< void* > write_sets;  // for duplicate check
        for (thread::ThreadGroupId i = 0; i < memory.get_node_memory_count(); ++i) {
            NumaNodeMemory* node_memory = memory.get_node_memory(i);
            EXPECT_EQ(options.thread_.thread_count_per_group_,
                        node_memory->get_core_memory_count());
            auto read_set_size = options.xct_.max_read_set_size_;
            auto write_set_size = options.xct_.max_write_set_size_;
            xct::XctAccess* read_set_base = reinterpret_cast<xct::XctAccess*>(
                node_memory->get_read_set_memory().get_block());
            xct::WriteXctAccess* write_set_base = reinterpret_cast<xct::WriteXctAccess*>(
                node_memory->get_write_set_memory().get_block());
            for (thread::ThreadLocalOrdinal j = 0; j < node_memory->get_core_memory_count(); ++j) {
                NumaCoreMemory* core_memory = node_memory->get_core_memory(j);
                EXPECT_EQ(read_set_size, core_memory->get_read_set_size());
                EXPECT_EQ(write_set_size, core_memory->get_write_set_size());

                xct::XctAccess* read_set = core_memory->get_read_set_memory();
                xct::WriteXctAccess* write_set = core_memory->get_write_set_memory();
                EXPECT_TRUE(read_sets.find(read_set) == read_sets.end());
                EXPECT_TRUE(read_sets.find(write_set) == read_sets.end());
                EXPECT_TRUE(write_sets.find(read_set) == write_sets.end());
                EXPECT_TRUE(write_sets.find(write_set) == write_sets.end());

                EXPECT_EQ(read_set_size * j, read_set - read_set_base);
                EXPECT_EQ(write_set_size * j, write_set - write_set_base);

                read_sets.insert(read_set);
                write_sets.insert(write_set);
            }
        }
        COERCE_ERROR(engine.uninitialize());
    }
    cleanup_test(options);
}

}  // namespace memory
}  // namespace foedus
