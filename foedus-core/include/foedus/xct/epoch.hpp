/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_EPOCH_HPP_
#define FOEDUS_XCT_EPOCH_HPP_

#include <stdint.h>
#include <iosfwd>
namespace foedus {
namespace xct {
/**
 * @ingroup XCT
 * @brief Represents a time epoch.
 * @details
 * Epoch is an corase-grained timestamp used all over the places.
 *
 * @par What is Epoch
 * An Epoch represents a few to 100s milliseconds. XctManager periodically or by-demand advances
 * the value so that the commit protocol, loggers, garbage collector, etc can \e loosely and
 * efficiently and synchronize with other parts of the engine.
 * Compared to fine-grained sequence number like Oracle/PostgreSQL's timestamp, this is
 * much more scalable. Several commercial and open-source storage engines use such a coarse-grained
 * timestamp nowadays.
 *
 * @par Wrap-around
 * This class can handle wrapping-around \b assuming there is no case where we have two epochs
 * that are 2^31 or more distant. As one epoch represents tens of milliseconds, this assumption
 * should hold. All very-old epochs will disappear from logs and snapshots by the time
 * we wrap around. We use wrap-around-aware comparison algorithms
 *
 * @see RFC 1982
 * @see http://en.wikipedia.org/wiki/Serial_number_arithmetic
 * @see http://github.com/pjkundert/sequence
 *
 * @par Translation unit
 * This class is header-only \b except the std::ostream redirect.
 */
class Epoch {
 public:
    /** \b Unsigned integer representation of epoch. "Unsigned" is important. see the links. */
    typedef uint32_t EpochInteger;
    /** Defines constant values. */
    enum Constants {
        /** This value means the epoch is not valid. */
        EPOCH_INVALID = 0,
        /** The smallest valid epoch. */
        EPOCH_INITIAL = 1,
    };

    /** Construct an empty/invalid epoch. */
    Epoch() : epoch_(EPOCH_INVALID) {}
    /** Construct an epoch of specified integer representation. */
    explicit Epoch(EpochInteger value) : epoch_(value) {}
    // default copy-constructor/assignment/destructor suffice

    bool    is_valid() const { return epoch_ != EPOCH_INVALID; }

    /** Returns the raw integer representation. */
    EpochInteger value() const { return epoch_; }

    void    operator++() { ++epoch_; }

    /**
     * Returns the \e distance from this epoch to the given epoch, as defined in RFC 1982.
     */
    int32_t distance(const Epoch &other) const {
        return static_cast<int32_t>(other.epoch_ - epoch_);  // SIGNED. see the links
    }

    bool    operator==(const Epoch &other)  const { return epoch_ == other.epoch_; }
    bool    operator!=(const Epoch &other)  const { return epoch_ != other.epoch_; }
    bool    operator<(const Epoch &other)   const { return distance(other) > 0; }
    bool    operator>(const Epoch &other)   const { return distance(other) < 0; }
    bool    operator<=(const Epoch &other)  const { return !operator<(other); }
    bool    operator>=(const Epoch &other)  const { return !operator>(other); }

    friend std::ostream& operator<<(std::ostream& o, const Epoch& v);

 private:
    /** The raw integer representation. */
    EpochInteger epoch_;
};

}  // namespace xct
}  // namespace foedus

#endif  // FOEDUS_XCT_EPOCH_HPP_
