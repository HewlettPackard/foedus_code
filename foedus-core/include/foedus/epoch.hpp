/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_EPOCH_HPP_
#define FOEDUS_EPOCH_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"

namespace foedus {
/**
 * @ingroup ENGINE
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
 * @par Value range
 * We use 28 bits to represent an epoch (we need to save 4 bits for the sake of foedus::xct::XctId).
 * The first 4 bits must be always zero. This means the value range of [0, 2^28 - 1].
 * Assuming 20ms per epoch, this is about 1 year.
 *
 * @par Wrap-around
 * This class can handle wrapping-around \b assuming there is no case where we have two epochs
 * that are 2^27 or more distant. As one epoch represents tens of milliseconds, this assumption
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
    /** Zero is always reserved for invalid epoch. A valid epoch always skips this value. */
    kEpochInvalid = 0,
    /** As there is no transaction in ep-1, initial durable_epoch is 1. */
    kEpochInitialDurable = 1,
    /** The first epoch (before wrap-around) that might have transactions is ep-2. */
    kEpochInitialCurrent = 2,
    /** Bits to represent an epoch. */
    kEpochBits = 28,
    /** Epoch values wrap around at this value. */
    kEpochIntOverflow = (1 << kEpochBits),
    /** Used for before(). */
    kEpochIntHalf = (1 << (kEpochBits - 1)),
  };

  /** Construct an invalid epoch. */
  Epoch() CXX11_NOEXCEPT : epoch_(kEpochInvalid) {}
  /** Construct an epoch of specified integer representation. */
  explicit Epoch(EpochInteger value) CXX11_NOEXCEPT : epoch_(value) {
    ASSERT_ND(value < kEpochIntOverflow);
  }
  // default copy-constructor/assignment/destructor suffice

  /** Clears this epoch variable so that it points to an invalid epoch */
  void    reset() { epoch_ = kEpochInvalid; }
  bool    is_valid() const {
    ASSERT_ND(epoch_ < kEpochIntOverflow);
    return epoch_ != kEpochInvalid;
  }

  /** Returns the raw integer representation. */
  EpochInteger value() const { return epoch_; }

  Epoch&  operator++() {
    ASSERT_ND(is_valid());  // we prohibit increment from invalid epoch
    if (epoch_ == kEpochIntOverflow - 1) {
      epoch_ = 1;  // skip 0, which is always an invalid epoch.
    } else {
      ++epoch_;
    }
    return *this;
  }
  Epoch&  operator--() {
    ASSERT_ND(is_valid());  // we prohibit decrement from invalid epoch
    if (epoch_ == 1) {
      epoch_ = kEpochIntOverflow - 1;  // skip 0, which is always an invalid epoch.
    } else {
      --epoch_;
    }
    return *this;
  }
  Epoch one_less() const {
    Epoch tmp(epoch_);
    tmp.operator--();
    return tmp;
  }
  Epoch one_more() const {
    Epoch tmp(epoch_);
    tmp.operator++();
    return tmp;
  }

  /**
   * @brief Kind of std::min(this, other).
   * @pre other.is_valid() otherwise what's the point?
   * @details
   * If this.is_valid(), this is exactly std::min. If not, other is unconditionally taken.
   */
  void    store_min(const Epoch& other) {
    ASSERT_ND(other.is_valid());
    if (!is_valid() || other < *this) {
      epoch_ = other.epoch_;
    }
  }
  /**
   * @brief Kind of std::max(this, other).
   * @pre other.is_valid() otherwise what's the point?
   * @details
   * If this.is_valid(), this is exactly std::max. If not, other is unconditionally taken.
   */
  void    store_max(const Epoch& other) {
    ASSERT_ND(other.is_valid());
    if (!is_valid() || other > *this) {
      epoch_ = other.epoch_;
    }
  }

  /**
   * Returns if this epoch is \e before the given epoch in the sense of
   * \e distance defined in RFC 1982. Both this and other must be valid epochs.
   */
  bool before(const Epoch &other) const {
    ASSERT_ND(is_valid());
    ASSERT_ND(other.is_valid());
    int64_t diff = static_cast<int64_t>(other.epoch_) - static_cast<int64_t>(epoch_);
    return diff <= -kEpochIntHalf || (diff > 0 && diff < kEpochIntHalf);
  }

  bool    operator==(const Epoch &other)  const { return epoch_ == other.epoch_; }
  bool    operator!=(const Epoch &other)  const { return epoch_ != other.epoch_; }
  bool    operator<(const Epoch &other)   const { return before(other); }
  bool    operator>(const Epoch &other)   const { return other.before(*this); }
  bool    operator<=(const Epoch &other)  const { return !operator>(other); }
  bool    operator>=(const Epoch &other)  const { return !operator<(other); }

  friend std::ostream& operator<<(std::ostream& o, const Epoch& v);

 private:
  /**
   * The raw integer representation.
   * @invariant 0 <= epoch_ < kEpochIntOverflow
   */
  EpochInteger epoch_;
};

/**
 * @ingroup IDIOMS
 * @brief A constant epoch object that represents an invalid epoch.
 */
const Epoch INVALID_EPOCH;
}  // namespace foedus

#endif  // FOEDUS_EPOCH_HPP_
