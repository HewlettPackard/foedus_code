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
#include <sys/resource.h>

#include <iostream>

#include "foedus/assert_nd.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"

namespace foedus {
DEFINE_TEST_CASE_PACKAGE(CheckEnvTest, foedus);

TEST(CheckEnvTest, MaxMapCount) {
  EXPECT_TRUE(has_enough_max_map_count());
}

// On the threshold values we use below:
// It depends on the number of threads how many nofiles/etc we really need,
// but it won't hurt to set a reasonably high threshold as an independent testcase.
// Even if this testcase fails as a false positive, it's just one false positive, not 100s.
// Using such high thresholds makes sure that at least the user has configured it, rather than
// leaving linux's default value.

TEST(CheckEnvTest, Hugepages) {
  uint64_t bytes = EngineOptions::get_available_hugepage_memory(&std::cout);
  EXPECT_GE(bytes, 1ULL << 29);  // At least 500MB hugepages available _now_. default 0.
}

TEST(CheckEnvTest, NoFiles) {
  ::rlimit limit;
  ::getrlimit(RLIMIT_NOFILE, &limit);
  EXPECT_GE(limit.rlim_cur, 50000U);  // default is 1k~8k
}

TEST(CheckEnvTest, Nproc) {
  ::rlimit limit;
  ::getrlimit(RLIMIT_NPROC, &limit);
  EXPECT_GE(limit.rlim_cur, 1U << 12);  // default is 1k or something
}

TEST(CheckEnvTest, Memlock) {
  // Alas, linux's default for this is **rediculously** small.
  ::rlimit limit;
  ::getrlimit(RLIMIT_MEMLOCK, &limit);
  EXPECT_GE(limit.rlim_cur, 1ULL << 30);  // 1TB (memlock is per kb). default is 32kb (lol).
}

TEST(CheckEnvTest, Shmall) {
  uint64_t bytes = EngineOptions::read_int_from_proc_fs("/proc/sys/kernel/shmall", &std::cout);
  EXPECT_GE(bytes, 1ULL << 31);
}
TEST(CheckEnvTest, Shmmax) {
  uint64_t bytes = EngineOptions::read_int_from_proc_fs("/proc/sys/kernel/shmmax", &std::cout);
  EXPECT_GE(bytes, 1ULL << 31);
}

TEST(CheckEnvTest, Shmmni) {
  uint64_t blocks = EngineOptions::read_int_from_proc_fs("/proc/sys/kernel/shmmni", &std::cout);
  EXPECT_GE(blocks, 1ULL << 12);
}


}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(CheckEnvTest, foedus);
