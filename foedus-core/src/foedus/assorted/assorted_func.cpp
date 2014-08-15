/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/assorted/assorted_func.hpp"

#ifdef __GNUC__  // for get_pretty_type_name()
#include <cxxabi.h>
#endif  // __GNUC__
#include <stdint.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
#include <thread>

#include "foedus/debugging/rdtsc.hpp"

namespace foedus {
namespace assorted {

int64_t int_div_ceil(int64_t dividee, int64_t dividor) {
  std::ldiv_t result = std::div(dividee, dividor);
  return result.rem != 0 ? (result.quot + 1) : result.quot;
}

std::string replace_all(const std::string& target, const std::string& search,
               const std::string& replacement) {
  std::string subject = target;
  while (true) {
    std::size_t pos = subject.find(search);
    if (pos != std::string::npos) {
      subject.replace(pos, search.size(), replacement);
    } else {
      break;
    }
  }
  return subject;
}

std::string replace_all(const std::string& target, const std::string& search,
               int replacement) {
  std::stringstream str;
  str << replacement;
  std::string rep = str.str();
  return replace_all(target, search, rep);
}

std::string os_error() {
  return os_error(errno);
}

std::string os_error(int error_number) {
  if (error_number == 0) {
    return "[No Error]";
  }
  std::stringstream str;
  // TODO(Hideaki) is std::strerror thread-safe? Thre is no std::strerror_r. Windows, mmm.
  str << "[Errno " << error_number << "] " << std::strerror(error_number);
  return str.str();
}

std::ostream& operator<<(std::ostream& o, const Hex& v) {
  std::ios::fmtflags old_flags = o.flags();
  o << "0x";
  if (v.fix_digits_ >= 0) {
    o.width(v.fix_digits_);
    o.fill('0');
  }
  o << std::hex << std::uppercase << v.val_;
  o.flags(old_flags);
  return o;
}

std::ostream& operator<<(std::ostream& o, const Top& v) {
  for (uint32_t i = 0; i < std::min<uint32_t>(v.data_len_, v.max_bytes_); ++i) {
    o << i << ":" << static_cast<int>(v.data_[i]);
    if (i != 0) {
      o << ", ";
    }
  }
  if (v.data_len_ > v.max_bytes_) {
    o << "...";
  }
  return o;
}

std::string demangle_type_name(const char* mangled_name) {
#ifdef __GNUC__
  int status;
  char* demangled = abi::__cxa_demangle(mangled_name, nullptr, nullptr, &status);
  if (demangled) {
    std::string ret(demangled);
    ::free(demangled);
    return ret;
  }
#endif  // __GNUC__
  return mangled_name;
}

uint64_t generate_almost_prime_below(uint64_t threshold) {
  if (threshold <= 2) {
    return 1;  // almost an invalid input...
  } else if (threshold < 3500) {
    // for a small number, we just use a (very) sparse prime list
    uint16_t small_primes[] = {3677, 2347, 1361, 773, 449, 263, 151, 89, 41, 17, 2};
    for (int i = 1;; ++i) {
      if (threshold > small_primes[i]) {
        return small_primes[i - 1];
      }
    }
  } else {
    // the following formula is monotonically increasing for i>=22 (which gives 3923).
    uint64_t prev = 3677;
    for (uint64_t i = 22;; ++i) {
      uint64_t cur = (i * i * i * i * i - 133 * i * i * i * i + 6729 * i * i * i
        - 158379 * i * i + 1720294 * i - 6823316) >> 2;
      if (cur >= threshold) {
        return prev;
      } else if (cur <= prev) {
        // sanity checking.
        return prev;
      } else {
        prev = cur;
      }
    }
  }
}

void SpinlockStat::yield_backoff() {
  if (spins_ == 0) {
    // do the real initialization only when we couldn't get a lock.
    // hopefully 99% cases we don't hit here.
    std::hash<std::thread::id> h;
    rnd_.set_current_seed(h(std::this_thread::get_id()));
    rnd_.next_uint32();
  }
  ++spins_;
  // exponential backoff. also do yielding occasionally.
  const uint64_t kMinSleepCycles = 1ULL << 7;  // one atomic CAS
  const uint64_t kMaxSleepCycles = 1ULL << 12;  // even this might be too long..
  uint64_t wait_cycles_max = spins_ < 5U ? kMinSleepCycles << spins_ : kMaxSleepCycles;
  uint32_t wait_cycles = kMinSleepCycles + rnd_.next_uint32() % (wait_cycles_max - kMinSleepCycles);
  debugging::wait_rdtsc_cycles(wait_cycles);
  if (spins_ % 256U == 0) {
    spinlock_yield();
  }
}

}  // namespace assorted
}  // namespace foedus

