/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ASSORTED_UNIFORM_RANDOM_HPP_
#define FOEDUS_ASSORTED_UNIFORM_RANDOM_HPP_
#include <foedus/memory/fwd.hpp>
#include <stdint.h>
namespace foedus {
namespace assorted {
/**
 * @brief A very simple and deterministic random generator that is more aligned with standard
 * benchmark such as TPC-C.
 * @ingroup ASSORTED
 * @details
 * Actually this is exactly from TPC-C spec.
 */
class UniformRandom {
 public:
    UniformRandom() : _seed(0) {}
    explicit UniformRandom(uint64_t seed) : _seed(seed) {}

    /**
        * In TPCC terminology, from=x, to=y.
        * NOTE both from and to are _inclusive_.
        */
    uint32_t uniform_within(uint32_t from, uint32_t to) {
        return from + (next_uint32() % (to - from + 1));
    }
    /**
        * Same as uniform_within() except it avoids the "except" value.
        * Make sure from!=to.
        */
    uint32_t uniform_within_except(uint32_t from, uint32_t to, uint32_t except) {
        while (true) {
            uint32_t val = uniform_within(from, to);
            if (val != except) {
                return val;
            }
        }
    }

    /**
        * Non-Uniform random (NURand) in TPCC spec (see Sec 2.1.6).
        * In TPCC terminology, from=x, to=y.
        *  NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
        */
    uint32_t non_uniform_within(uint32_t A, uint32_t from, uint32_t to) {
        uint32_t C = get_c(A);
        return  (((uniform_within(0, A) | uniform_within(from, to)) + C) % (to - from + 1)) + from;
    }

    uint64_t get_current_seed() {
        return _seed;
    }
    void set_current_seed(uint64_t seed) {
        _seed = seed;
    }

    /**
     * @brief Fill up the give memory with random data.
     * @details
     * Call this to pre-calculate many random numbers. When the function we test is very fast,
     * generating random numbers might become the bottleneck. This method is to avoid it.
     * This is not inlined.
     */
    void fill_memory(foedus::memory::AlignedMemory *memory);

 private:
    uint32_t next_uint32() {
        _seed = _seed * 0xD04C3175 + 0x53DA9022;
        return (_seed >> 32) ^ (_seed & 0xFFFFFFFF);
    }
    uint64_t _seed;

    /**
        * C is a run-time constant randomly chosen within [0 .. A] that can be
        * varied without altering performance. The same C value, per field
        * (C_LAST, C_ID, and OL_I_ID), must be used by all emulated terminals.
        * constexpr, but let's not bother C++11.
        */
    uint32_t get_c(uint32_t A) const {
        // yes, I'm lazy. but this satisfies the spec.
        const uint64_t C_SEED = 0x734b00c6d7d3bbdaULL;
        return C_SEED % (A + 1);
    }
};

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_UNIFORM_RANDOM_HPP_
