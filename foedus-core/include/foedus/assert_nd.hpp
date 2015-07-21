/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_ASSERT_ND_HPP_
#define FOEDUS_ASSERT_ND_HPP_

#include <string>

/**
 * @def ASSERT_ND(x)
 * @ingroup IDIOMS
 * @brief A warning-free wrapper macro of assert() that has no performance effect in release
 * mode \e even \e when 'x' is not a simple variable.
 * @details
 * The standard assert() in release mode often results in compiler warnings for unused variables.
 * A common mistake is to wrap it like this:
 * @code{.cpp}
 * #define ASSERT_INCORRECT(x) do { (void)(x); } while(0)
 * @endcode
 * This can cause a performance issue when 'x' is a function call etc because the compiler might
 * not be able to confidently get rid of the code.
 * Instead, we use the idea of (void) sizeof(x) trick in the following URL.
 * @see http://cnicholson.net/2009/02/stupid-c-tricks-adventures-in-assert/
 */
/**
 * @def UNUSED_ND(var)
 * @ingroup IDIOMS
 * @brief Cross-compiler UNUSED macro for the same purpose as ASSERT_ND(x).
 */
namespace foedus {
  /** Helper function to report what assertion failed. */
  std::string print_assert(const char* file, const char* func, int line, const char* description);
  /** Prints out backtrace. This method is best-effort, maybe do nothing in some compiler/OS. */
  std::string print_backtrace();

  /**
   * print_assert() + print_backtrace().
   * It also leaves the info in a global variable so that signal handler can show that later.
   */
  void print_assert_backtrace(
    const char* file,
    const char* func,
    int line,
    const char* description);

  /** Retrieves the info left by print_assert_backtrace(). Called by signal handler */
  std::string get_recent_assert_backtrace();
}  // namespace foedus

#ifdef NDEBUG
#define ASSERT_ND(x) do { (void) sizeof(x); } while (0)
#define UNUSED_ND(var) ASSERT_ND(var)
#else  // NDEBUG
#include <cassert>
#ifndef ASSERT_ND_NOBACKTRACE
#define ASSERT_QUOTE(str) #str
#define ASSERT_EXPAND_AND_QUOTE(str) ASSERT_QUOTE(str)
#define ASSERT_ND(x) do { if (!(x)) { \
  foedus::print_assert_backtrace(__FILE__, __FUNCTION__, __LINE__, ASSERT_QUOTE(x)); \
  assert(x); \
  } } while (0)
#else  // ASSERT_ND_NOBACKTRACE
#define ASSERT_ND(x) assert(x)
#endif  // ASSERT_ND_NOBACKTRACE
#define UNUSED_ND(var)
#endif  // NDEBUG

#endif  // FOEDUS_ASSERT_ND_HPP_
