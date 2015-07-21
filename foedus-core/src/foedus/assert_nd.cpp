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
#include "foedus/assert_nd.hpp"

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "foedus/assorted/rich_backtrace.hpp"

namespace foedus {
std::string print_assert(const char* file, const char* func, int line, const char* description) {
  std::stringstream str;
  str << "***************************************************************************" << std::endl;
  str << "**** Assertion failed! \"" << description
    << "\" did not hold in " << func << "(): " << file << ":" << line << std::endl;
  str << "***************************************************************************" << std::endl;
  return str.str();
}

std::string print_backtrace() {
  std::vector<std::string> traces = assorted::get_backtrace(true);
  std::stringstream str;
  str << "=== Stack frame (length=" << traces.size() << ")" << std::endl;
  for (uint16_t i = 0; i < traces.size(); ++i) {
    str << "- [" << i << "/" << traces.size() << "] " << traces[i] << std::endl;
  }
  return str.str();
}

/**
 * Leaves recent crash information in a static global variable so that a signal handler can pick it.
 */
std::string static_recent_assert_backtrace;

void print_assert_backtrace(const char* file, const char* func, int line, const char* description) {
  std::string message = print_assert(file, func, line, description);
  message += print_backtrace();
  static_recent_assert_backtrace += message;
  std::cerr << message;
}


std::string get_recent_assert_backtrace() {
  return static_recent_assert_backtrace;
}

}  // namespace foedus
