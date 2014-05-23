/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/test_common.hpp>
#include <foedus/assorted/atomic_fences.hpp>
#include <foedus/assorted/raw_atomics.hpp>
#include <foedus/thread/rendezvous_impl.hpp>
#include <stdint.h>
#include <gtest/gtest.h>
#include <iostream>
#include <thread>
#include <vector>

namespace foedus {
namespace assorted {

const int THREADS = 6;
const int ITERATIONS = 20;
const int REPS = 5;
template <typename T>
struct CasTest {
    explicit CasTest(bool weak = false) : weak_(weak), start_rendezvous_(nullptr) {}
    void test() {
        for (int i = 0; i < REPS; ++i) {
            test_rep();
        }
    }
    void test_rep() {
        static_assert(THREADS * ITERATIONS < 127, "exceeds smallest integer");
        data_ = 0;
        conflicts_ = 0;
        start_rendezvous_ = new thread::Rendezvous();
        for (int i = 0; i <= THREADS * ITERATIONS; ++i) {
            observed_[i] = false;
        }
        assorted::memory_fence_release();
        for (int i = 0; i < THREADS; ++i) {
            threads_.emplace_back(std::thread(&CasTest::handle, this, i));
        }

        start_rendezvous_->signal();
        for (int i = 0; i < THREADS; ++i) {
            threads_[i].join();
        }
        threads_.clear();

        EXPECT_FALSE(observed_[data_]) << data_;
        observed_[data_] = true;
        assorted::memory_fence_acquire();
        for (int i = 0; i <= THREADS * ITERATIONS; ++i) {
            EXPECT_TRUE(observed_[i]) << i;
        }
        std::cout << "In total, about " << conflicts_ << " coflicts" << std::endl;
        delete start_rendezvous_;
        start_rendezvous_ = nullptr;
    }

    void handle(int id) {
        start_rendezvous_->wait();
        EXPECT_GE(id, 0);
        EXPECT_LT(id, THREADS);
        for (int i = 0; i < ITERATIONS; ++i) {
            T old = 127;
            const T desired = value(id, i);
            bool swapped;
            if (weak_) {
                swapped = assorted::raw_atomic_compare_exchange_weak<T>(&data_, &old, desired);
            } else {
                swapped = assorted::raw_atomic_compare_exchange_strong<T>(&data_, &old, desired);
            }
            EXPECT_FALSE(swapped);
            EXPECT_NE(desired, old);
            EXPECT_NE(127, old);
            while (true) {
                T prev_old = old;
                if (weak_) {
                    swapped = assorted::raw_atomic_compare_exchange_weak<T>(&data_, &old, desired);
                } else {
                    swapped = assorted::raw_atomic_compare_exchange_strong<T>(
                        &data_, &old, desired);
                }
                if (swapped) {
                    EXPECT_EQ(prev_old, old);
                    EXPECT_FALSE(observed_[old]);
                    observed_[old] = true;
                    break;
                } else {
                    ++conflicts_;
                    EXPECT_NE(prev_old, old);
                }
            }
        }
    }
    T value(int id, int iteration) const {
        return iteration * THREADS + id + 1;
    }

    bool weak_;
    std::vector<std::thread> threads_;
    thread::Rendezvous *start_rendezvous_;
    T data_;
    int conflicts_;
    bool observed_[THREADS * ITERATIONS];
};

TEST(RawAtomicsTest, Uint8) { CasTest<uint8_t>().test(); }
TEST(RawAtomicsTest, Uint16) { CasTest<uint16_t>().test(); }
TEST(RawAtomicsTest, Uint32) { CasTest<uint32_t>().test(); }
TEST(RawAtomicsTest, Uint64) { CasTest<uint64_t>().test(); }
TEST(RawAtomicsTest, Int8) { CasTest<int8_t>().test(); }
TEST(RawAtomicsTest, Int16) { CasTest<int16_t>().test(); }
TEST(RawAtomicsTest, Int32) { CasTest<int32_t>().test(); }
TEST(RawAtomicsTest, Int64) { CasTest<int64_t>().test(); }

TEST(RawAtomicsTest, Uint8Weak) { CasTest<uint8_t>(true).test(); }
TEST(RawAtomicsTest, Uint16Weak) { CasTest<uint16_t>(true).test(); }
TEST(RawAtomicsTest, Uint32Weak) { CasTest<uint32_t>(true).test(); }
TEST(RawAtomicsTest, Uint64Weak) { CasTest<uint64_t>(true).test(); }
TEST(RawAtomicsTest, Int8Weak) { CasTest<int8_t>(true).test(); }
TEST(RawAtomicsTest, Int16Weak) { CasTest<int16_t>(true).test(); }
TEST(RawAtomicsTest, Int32Weak) { CasTest<int32_t>(true).test(); }
TEST(RawAtomicsTest, Int64Weak) { CasTest<int64_t>(true).test(); }

}  // namespace assorted
}  // namespace foedus
