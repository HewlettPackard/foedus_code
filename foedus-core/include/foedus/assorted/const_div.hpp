/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ASSORTED_CONST_DIV_HPP_
#define FOEDUS_ASSORTED_CONST_DIV_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/assert_nd.hpp>
#include <stdint.h>
namespace foedus {
namespace assorted {
const uint32_t POWER_2_31 = 1U << 31;
const uint64_t POWER_2_63 = 1ULL << 63;
const uint32_t FULL_32_BITS = 0xFFFFFFFF;
const uint32_t FULL_31_BITS = 0x7FFFFFFF;
const uint64_t FULL_64_BITS = 0xFFFFFFFFFFFFFFFFULL;
const uint64_t FULL_63_BITS = 0x7FFFFFFFFFFFFFFFULL;

/**
 * @brief The pre-calculated p-m pair for optimized integer division by constant.
 * @ingroup ASSORTED
 * @details
 * This object implements the optimized integer division described in [HACKER] Chap 10.
 * We assume the divisor is within 32bit and dividee is either 32bit or 64bit.
 * This is a bit different assumption from existing library; libdivide.
 *
 * @section USE Usecases
 * This is used in places where integer division by a constant integer is the bottleneck.
 * So far the only such place is partitioning and array storage (\ref ARRAY).
 * In read-only experiments on array storage, we did observe more than 20% overhead in std::lldiv().
 * It's a division by fan-out, so of course we can afford to pre-calculate \e p and \e m.
 * Hence, we added this object.
 *
 * @section EXAMPLE Example
 * For example, use it as follows.
 * @code{.cpp}
 * ConstDiv div(254);  // to divide by constant 254. This object should be precalculated.
 * std::cout << "0x12345678/254=" << div.div32(0x12345678) << std::endl;
 * std::cout << "0x123456789ABCDEF/254=" << div.div64(0x123456789ABCDEFULL) << std::endl;
 * @endcode
 *
 * @section ETC Etc
 * This object is totally header-only. This object is totally immutable, so .
 * This object is POD and can trivially copy/move.
 *
 * @section REF References
 * For more details, read the following:
 *  \li [HACKER] "Hacker's delight 2nd ed." If you don't own a copy, you are not a real programmer.
 *  \li [libdivide] https://github.com/ridiculousfish/libdivide
 *  \li [libdivide-pdf] http://ridiculousfish.com/files/faster_unsigned_division_by_constants.pdf
 */
struct ConstDiv {
    enum Constants {
        /** Whether the divisor is a power of 2. When this flag is on, we just shift bits. */
        FLAG_POWER_OF_2 = 0x01,
        /** Add inidicator for 32bit division. */
        FLAG_ADD_32 = 0x02,
        /** Add inidicator for 64bit division. */
        FLAG_ADD_64 = 0x04,
    };

    /**
     * Pre-calculate the p-m pair for the given divisor.
     * @param[in] d divisor
     */
    explicit ConstDiv(uint32_t d) {
        init(d);
    }

    ConstDiv() {
        init(1);
    }

    void init(uint32_t d);

    /**
     * @brief 32-bit integer division that outputs both quotient and remainder.
     * @param[in] n dividee
     * @return quotient
     * @details
     * Calculation of remainder is separated below. If the caller calls the two functions in a row,
     * I believe compiler is smart enough to get rid of extra multiplication.
     */
    uint32_t div32(uint32_t n) const;
    /** Calculate remainder. */
    uint32_t rem32(uint32_t n, uint32_t d, uint32_t q) const;

    /**
     * @brief 64-bit integer division that outputs both quotient and remainder.
     * @param[in] n dividee
     * @return quotient
     */
    uint64_t div64(uint64_t n) const;
    /** Calculate remainder. */
    uint32_t rem64(uint64_t n, uint32_t d, uint64_t q) const;

    /** Highest bits to represent d. 2^(d_highest_bits_) <= d < 2^(d_highest_bits_+1). */
    uint8_t d_highest_bits_;  // +1 => 1

    /** "s" for 32 bit division. */
    uint8_t shift32_;  // +1 => 2

    /** "s" for 64 bit division. */
    uint8_t shift64_;  // +1 => 3

    /** misc flags. */
    uint8_t flags_;  // +1 => 4

    /** magic number for 32 bit division. */
    uint32_t magic32_;  // +4 => 8

    /** magic number for 64 bit division. */
    uint64_t magic64_;  // +8 => 16

#ifndef NDEBUG
    /** Oridinal divisor. For sanity check. */
    uint32_t d_;
    uint32_t dummy_;
#endif  // NDEBUG
};

inline void ConstDiv::init(uint32_t d) {
    // this one is inlined just to avoid multiple-definition, not for performance.
    ASSERT_ND(d);
    d_highest_bits_ = 31 - __builtin_clz(d);  // TODO(Hideaki): non-GCC support
#ifndef NDEBUG
    d_ = d;
    dummy_ = 0;
#endif  // NDEBUG

    // power of 2 is a bit special.
    if ((d & (d - 1)) == 0) {
        ASSERT_ND(d == (1U << d_highest_bits_));
        shift32_ = 0;
        shift64_ = 0;
        flags_ = FLAG_POWER_OF_2;
        magic32_ = 0;
        magic64_ = 0;
        return;
    }

    flags_ = 0;

    // calculate 32bit/64bit magic numbers and add indicator, this part is based on [libdivide-pdf]
    // rather than [HACKERS] although it is also based on [HACKERS].
    {
        shift32_ = d_highest_bits_;
        uint32_t m = (1ULL << (32 + d_highest_bits_)) / d;
        uint32_t rem = (1ULL << (32 + d_highest_bits_)) % d;
        ASSERT_ND(rem > 0 && rem < d);
        uint32_t e = d - rem;
        if (e >= (1U << d_highest_bits_)) {
            // we have add indicator (2^W <= M < 2^(W+1), m = M - 2^W).
            // here is a nice idea in libdivide.
            // We let it overflow, but we do so for remainder too, thus even with overflow
            // we can correctly calculate the quotient!
            // We use the magic number for this case with divide-by-2 in div32 to account for this.
            flags_ |= FLAG_ADD_32;
            m *= 2;
            uint32_t twice_rem = rem * 2;
            if (twice_rem >= d || twice_rem < rem) {
                ++m;
            }
        }
        magic32_ = m + 1;
    }

    // then 64bit version.
    {
        shift64_ = d_highest_bits_;
        // TODO(Hideaki): non-GCC version
        __uint128_t numer = 1;
        numer <<= 64 + d_highest_bits_;
        uint64_t m = numer / d;
        uint32_t rem = numer % d;
        ASSERT_ND(rem > 0 && rem < d);
        uint32_t e = d - rem;
        if (e >= (1ULL << d_highest_bits_)) {
            flags_ |= FLAG_ADD_64;
            m *= 2;
            uint32_t twice_rem = rem * 2;
            if (twice_rem >= d || twice_rem < rem) {
                ++m;
            }
        }
        magic64_ = m + 1;
    }
}

inline uint32_t ConstDiv::rem32(uint32_t n, uint32_t d, uint32_t q) const {
#ifndef NDEBUG
    ASSERT_ND(d == d_);
#endif  // NDEBUG
    ASSERT_ND(n / d == q);
    if (flags_ & FLAG_POWER_OF_2) {
        return n & ((1 << d_highest_bits_) - 1);
    } else {
        return n - d * q;
    }
}
inline uint32_t ConstDiv::rem64(uint64_t n, uint32_t d, uint64_t q) const {
#ifndef NDEBUG
    ASSERT_ND(d == d_);
#endif  // NDEBUG
    ASSERT_ND(n / d == q);
    if (flags_ & FLAG_POWER_OF_2) {
        return n & ((1ULL << d_highest_bits_) - 1ULL);
    } else {
        return n - d * q;
    }
}

inline uint32_t ConstDiv::div32(uint32_t n) const {
    if (flags_ & FLAG_POWER_OF_2) {
        return n >> d_highest_bits_;
    } else {
        uint64_t product = static_cast<uint64_t>(n) * magic32_;
        uint32_t quotient = static_cast<uint32_t>(product >> 32);
        if (flags_ & FLAG_ADD_32) {
            quotient += (n - quotient) >> 1;
        }
        return quotient >> shift32_;
    }
}

inline uint64_t ConstDiv::div64(uint64_t n) const {
    if (flags_ & FLAG_POWER_OF_2) {
        return n >> d_highest_bits_;
    }

    if (n < (1ULL << 32)) {
        // cheap case
        return div32(static_cast<uint32_t>(n));
    }

    ASSERT_ND(n >= (1ULL << 32));
    // TODO(Hideaki): non-GCC which doesn't have uint128_t.
    __uint128_t product = static_cast<__uint128_t>(n) * magic64_;
    uint64_t quotient = static_cast<uint64_t>(product >> 64);
    if (flags_ & FLAG_ADD_64) {
        quotient += (n - quotient) >> 1;
    }
    return quotient >> shift64_;
}

}  // namespace assorted
}  // namespace foedus
#endif  // FOEDUS_ASSORTED_CONST_DIV_HPP_
