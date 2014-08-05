/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <gtest/gtest.h>

#include <stdint.h>

#include <cstring>

#include "foedus/test_common.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/assorted/uniform_random.hpp"

namespace foedus {
namespace assorted {

template <typename T>
void test_roundtrip() {
  char buf[8];

  UniformRandom rnd(123L);
  for (int i = 0; i < 1000; ++i) {
    T host_value = static_cast<T>(rnd.next_uint64());
    write_bigendian<T>(host_value, buf);
    T converted = read_bigendian<T>(buf);
    EXPECT_EQ(host_value, converted);
  }
}

template <typename T>
void test_comparison() {
  char buf[16];
  char buf2[16];
  std::memset(buf, 0, 16);
  std::memset(buf2, 0, 16);

  UniformRandom rnd(123L);
  for (int i = 0; i < 1000; ++i) {
    T host_value = static_cast<T>(rnd.next_uint64());
    T host_value2 = static_cast<T>(rnd.next_uint64());
    write_bigendian<T>(host_value, buf);
    write_bigendian<T>(host_value2, buf2);
    if (host_value < host_value2) {
      EXPECT_LT(std::memcmp(buf, buf2, sizeof(T)), 0);
    } else if (host_value > host_value2) {
      EXPECT_GT(std::memcmp(buf, buf2, sizeof(T)), 0);
    } else {
      EXPECT_EQ(std::memcmp(buf, buf2, sizeof(T)), 0);
    }
  }
}

DEFINE_TEST_CASE_PACKAGE(EndiannessTest, foedus.assorted);
TEST(EndiannessTest, RoundtripU8) { test_roundtrip<uint8_t>(); }
TEST(EndiannessTest, RoundtripU16) { test_roundtrip<uint16_t>(); }
TEST(EndiannessTest, RoundtripU32) { test_roundtrip<uint32_t>(); }
TEST(EndiannessTest, RoundtripU64) { test_roundtrip<uint64_t>(); }
TEST(EndiannessTest, RoundtripI8) { test_roundtrip<int8_t>(); }
TEST(EndiannessTest, RoundtripI16) { test_roundtrip<int16_t>(); }
TEST(EndiannessTest, RoundtripI32) { test_roundtrip<int32_t>(); }
TEST(EndiannessTest, RoundtripI64) { test_roundtrip<int64_t>(); }

TEST(EndiannessTest, ComparisonU8) { test_comparison<uint8_t>(); }
TEST(EndiannessTest, ComparisonU16) { test_comparison<uint16_t>(); }
TEST(EndiannessTest, ComparisonU32) { test_comparison<uint32_t>(); }
TEST(EndiannessTest, ComparisonU64) { test_comparison<uint64_t>(); }
TEST(EndiannessTest, ComparisonI8) { test_comparison<int8_t>(); }
TEST(EndiannessTest, ComparisonI16) { test_comparison<int16_t>(); }
TEST(EndiannessTest, ComparisonI32) { test_comparison<int32_t>(); }
TEST(EndiannessTest, ComparisonI64) { test_comparison<int64_t>(); }


}  // namespace assorted
}  // namespace foedus
