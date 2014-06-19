/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ASSORTED_ASSORTED_FUNC_HPP_
#define FOEDUS_ASSORTED_ASSORTED_FUNC_HPP_
#include <foedus/cxx11.hpp>
#include <stdint.h>

#if defined(__GNUC__)
#include <xmmintrin.h>
#endif  // defined(__GNUC__)

#include <iosfwd>
#include <string>
#include <typeinfo>
namespace foedus {
namespace assorted {

/**
 * @brief Returns the smallest multiply of ALIGNMENT that is equal or larger than the given number.
 * @ingroup ASSORTED
 * @tparam T integer type
 * @tparam ALIGNMENT alignment size. must be power of two
 * @details
 * In other words, round-up. For example of 8-alignment, 7 becomes 8, 8 becomes 8, 9 becomes 16.
 * @see https://en.wikipedia.org/wiki/Data_structure_alignment
 * @see Hacker's Delight 2nd Ed. Chap 3-1.
 */
template <typename T, unsigned int ALIGNMENT>
inline T align(T value) {
  return static_cast<T>((value + ALIGNMENT - 1) & (-ALIGNMENT));
}

/**
 * 8-alignment.
 * @ingroup ASSORTED
 */
template <typename T> inline T align8(T value) { return align<T, 8>(value); }

/**
 * 64-alignment.
 * @ingroup ASSORTED
 */
template <typename T> inline T align64(T value) { return align<T, 64>(value); }


/**
 * Efficient ceil(dividee/dividor) for integer.
 * @ingroup ASSORTED
 */
int64_t int_div_ceil(int64_t dividee, int64_t dividor);

/**
 * target.replaceAll(search, replacement). Sad that std C++ doesn't provide such a basic stuff.
 * regex is an overkill for this purpose.
 * @ingroup ASSORTED
 */
std::string replace_all(const std::string& target, const std::string& search,
               const std::string& replacement);
/**
 * target.replaceAll(search, String.valueOf(replacement)).
 * @ingroup ASSORTED
 */
std::string replace_all(const std::string& target, const std::string& search,
               int replacement);

/**
 * Thread-safe strerror(errno). We might do some trick here for portability, too.
 * @ingroup ASSORTED
 */
std::string os_error();

/**
 * This version receives errno.
 * @ingroup ASSORTED
 */
std::string os_error(int error_number);

/**
 * @brief Convenient way of writing hex integers to stream.
 * @ingroup ASSORTED
 * @details
 * Use it as follows.
 * @code{.cpp}
 * std::cout << Hex(1234) << ...
 * // same output as:
 * // std::cout << "0x" << std::hex << std::uppercase << 1234 << std::nouppercase << std::dec << ...
 * @endcode
 */
struct Hex {
  template<typename T>
  Hex(T val, int fix_digits = -1) : val_(static_cast<uint64_t>(val)), fix_digits_(fix_digits) {}

  uint64_t val_;
  int fix_digits_;
  friend std::ostream& operator<<(std::ostream& o, const Hex& v);
};


/**
 * @brief Equivalent to _mm_pause() or x86 PAUSE instruction.
 * @ingroup ASSORTED
 * @details
 * Invoke this where you do a spinlock. It especially helps valgrind.
 * Probably you should invoke this after a few spins.
 * @see http://stackoverflow.com/questions/7371869/minimum-time-a-thread-can-pause-in-linux
 * "NOP instruction can be between 0.4-0.5 clocks and PAUSE instruction can consume 38-40 clocks."
 * @see SPINLOCK_WHILE(x)
 */
inline void spinlock_yield() {
#if defined(__GNUC__)
  ::_mm_pause();
#else  // defined(__GNUC__)
  // Non-gcc compiler.
  asm volatile("pause" ::: "memory");  // TODO(Hideaki) but what about ARM
#endif  // defined(__GNUC__)
}
/** Helper for SPINLOCK_WHILE. */
inline void spinlock_yield_if(bool condition) {
  if (condition) {
    spinlock_yield();
  }
}

/**
 * Alternative for static_assert(sizeof(foo) == sizeof(bar), "oh crap") to display sizeof(foo).
 * @ingroup ASSORTED
 * @details
 * Use it like this:
 * @code{.cpp}
 * STATIC_SIZE_CHECK(sizeof(foo), sizeof(bar))
 * @endcode
 */
template<uint64_t SIZE1, uint64_t SIZE2>
inline int static_size_check() {
  CXX11_STATIC_ASSERT(SIZE1 == SIZE2,
    "Static Size Check failed. Look for a message like this to see the value of Size1 and "
    "Size2: 'In instantiation of int foedus::assorted::static_size_check() [with long unsigned"
    " int SIZE1 = <size1>ul; long unsigned int SIZE2 = <size2>ul]'");
  return 0;
}

/**
 * @brief Demangle the given C++ type name \e if possible (otherwise the original string).
 * @ingroup ASSORTED
 */
std::string demangle_type_name(const char* mangled_name);

/**
 * @brief Returns the name of the C++ type as readable as possible.
 * @ingroup ASSORTED
 * @tparam T the type
 */
template <typename T>
std::string get_pretty_type_name() {
  return demangle_type_name(typeid(T).name());
}

}  // namespace assorted
}  // namespace foedus

/**
 * @def SPINLOCK_WHILE(x)
 * @brief A macro to busy-wait (spinlock) with occasional pause.
 * @ingroup ASSORTED
 * @details
 * Use this as follows.
 * @code{.cpp}
 * SPINLOCK_WHILE(my_variable == 0) {
 *   do_something();
 * }
 * @endcode
 */
#define SPINLOCK_WHILE(x) \
  for (uint8_t __spins = 0; (x); foedus::assorted::spinlock_yield_if(++__spins == 0))

// Use __COUNTER__ to generate a unique method name
#define STATIC_SIZE_CHECK_CONCAT_DETAIL(x, y) x##y
#define STATIC_SIZE_CHECK_CONCAT(x, y) STATIC_SIZE_CHECK_CONCAT_DETAIL(x, y)
#define STATIC_SIZE_CHECK_METHOD_NAME \
  STATIC_SIZE_CHECK_CONCAT(_dummy_static_size_check, __COUNTER__)
#define STATIC_SIZE_CHECK(desired, actual) \
  inline void STATIC_SIZE_CHECK_METHOD_NAME() { \
    foedus::assorted::static_size_check< desired, actual >();\
  }

/**
 * @def INSTANTIATE_ALL_TYPES(M)
 * @brief A macro to explicitly instantiate the given template for all types we care.
 * @ingroup ASSORTED
 * @details
 * M is the macro to explicitly instantiate a template for the given type.
 * This macro explicitly instantiates the template for bool, float, double, all integers
 * (signed/unsigned), and std::string.
 * This is useful when \e definition of the template class/method involve too many details
 * and you rather want to just give \e declaration of them in header.
 *
 * Use this as follows. In header file.
 * @code{.h}
 * template <typename T> void cool_func(T arg);
 * @endcode
 * Then, in cpp file.
 * @code{.cpp}
 * template <typename T> void cool_func(T arg) {
 *   ... (implementation code)
 * }
 * #define EXPLICIT_INSTANTIATION_COOL_FUNC(x) template void cool_func< x > (x arg);
 * INSTANTIATE_ALL_TYPES(EXPLICIT_INSTANTIATION_COOL_FUNC);
 * @endcode
 * Remember, you should invoke this macro in cpp, not header, otherwise you will get
 * multiple-definition errors.
 * @note Doxygen doesn't understand template explicit instantiation, giving warnings. Not a big
 * issue, but you should shut up the Doxygen warnings by putting cond/endcond. See
 * externalizable.cpp for example.
 */
/**
 * @def INSTANTIATE_ALL_NUMERIC_TYPES(M)
 * @brief INSTANTIATE_ALL_TYPES minus std::string.
 * @ingroup ASSORTED
 */
/**
 * @def INSTANTIATE_ALL_INTEGER_TYPES(M)
 * @brief INSTANTIATE_ALL_NUMERIC_TYPES minus bool/double/float.
 * @ingroup ASSORTED
 */
#define INSTANTIATE_ALL_INTEGER_TYPES(M) M(int64_t);  /** NOLINT(readability/function) */\
  M(int32_t); M(int16_t); M(int8_t); M(uint64_t);  /** NOLINT(readability/function) */\
  M(uint32_t); M(uint16_t); M(uint8_t); /** NOLINT(readability/function) */

#define INSTANTIATE_ALL_NUMERIC_TYPES(M) INSTANTIATE_ALL_INTEGER_TYPES(M);\
  M(bool); M(float); M(double); /** NOLINT(readability/function) */

#define INSTANTIATE_ALL_TYPES(M) INSTANTIATE_ALL_NUMERIC_TYPES(M);\
  M(std::string);  /** NOLINT(readability/function) */


#endif  // FOEDUS_ASSORTED_ASSORTED_FUNC_HPP_
