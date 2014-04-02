/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_EPOCH_HPP_
#define FOEDUS_EPOCH_HPP_

#include <cstdint>
#include <ostream>
namespace foedus {
/**
 * @defgroup EPOCH Time Epoch
 * @brief Epoch is an corase-grained timestamp used all over the places.
 * @details
 * @section OVERVIEW What is Epoch
 * bluh bluh.
 * @section INCREMENT Who/When Epoch is Incremented
 * bluh bluh.
 */

/**
 * @ingroup EPOCH
 * @brief Represents an epoch.
 * @details
 * This class is header-only.
 * @todo Handle wrapping-around
 */
class Epoch {
 public:
    /** Defines constant values. */
    enum Constants {
        /** This value means the epoch is not valid. */
        EPOCH_INVALID = 0,
        /** The smallest valid epoch. */
        EPOCH_INITIAL = 1,
    };

    Epoch() : epoch_(EPOCH_INVALID) {}
    Epoch(const Epoch &other) : epoch_(other.epoch_) {}
    ~Epoch() {}
    Epoch*  operator=(const Epoch &other) { epoch_ = other.epoch_; }

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
    uint64_t get_epoch() const { return epoch_; }

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

 private:
    /** The raw integer representation. */
    uint64_t    epoch_;
};

}  // namespace foedus

inline std::ostream& operator<<(std::ostream& o, const foedus::Epoch& v) {
    if (v.is_valid()) {
        o << v.get_epoch();
    } else {
        o << "<INVALID>";
    }
    return o;
}

#endif  // FOEDUS_EPOCH_HPP_
