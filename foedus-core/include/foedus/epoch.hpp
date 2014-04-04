/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_EPOCH_HPP_
#define FOEDUS_EPOCH_HPP_

#include <cstdint>
#include <iosfwd>
namespace foedus {
/**
 * @defgroup EPOCH Time Epoch
 * @brief Epoch is an corase-grained timestamp used all over the places.
 * @details
 * @par What is Epoch
 * bluh bluh.
 * @par Who/When Epoch is Incremented
 * bluh bluh.
 */

/**
 * @ingroup EPOCH
 * @brief Represents an epoch.
 * @details
 * This class is header-only \b except the std::ostream redirect.
 * @todo Handle wrapping-around
 */
class Epoch {
 public:
    /** Integer representation of epoch. */
    typedef uint32_t epoch_integer;
    /** Defines constant values. */
    enum Constants {
        /** This value means the epoch is not valid. */
        EPOCH_INVALID = 0,
        /** The smallest valid epoch. */
        EPOCH_INITIAL = 1,
    };

    Epoch() : epoch_(EPOCH_INVALID) {}
    // default copy-constructor/assignment/destructor suffice

    /**
     * @brief Advances this epoch by one.
     * @return Whether wrap-around happened
     * @todo Handle wrapping-around
     */
    bool    increment() {
        // TODO(Hideaki) wrapping around
        ++epoch_;
        return false;
    }

    bool    is_valid() const { return epoch_ != EPOCH_INVALID; }

    /** Returns the raw integer representation. */
    epoch_integer get_epoch() const { return epoch_; }

    bool    operator==(const Epoch &other) const { return epoch_ == other.epoch_; }
    bool    operator!=(const Epoch &other) const { return epoch_ != other.epoch_; }

    /** @todo Handle wrapping-around */
    bool    operator<(const Epoch &other) const {
        // TODO(Hideaki) wrapping around
        return epoch_ < other.epoch_;
    }
    /** @todo Handle wrapping-around */
    bool    operator>(const Epoch &other) const {
        // TODO(Hideaki) wrapping around
        return epoch_ > other.epoch_;
    }

    bool    operator<=(const Epoch &other) const { return !operator<(other); }
    bool    operator>=(const Epoch &other) const { return !operator>(other); }

    friend std::ostream& operator<<(std::ostream& o, const Epoch& v);

 private:
    /** The raw integer representation. */
    epoch_integer epoch_;
};

}  // namespace foedus

#endif  // FOEDUS_EPOCH_HPP_
