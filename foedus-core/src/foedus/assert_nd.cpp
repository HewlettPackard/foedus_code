/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/assert_nd.hpp"

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "foedus/assorted/rich_backtrace.hpp"

namespace foedus {
void print_assert(const char* file, const char* func, int line, const char* description) {
  std::stringstream str;
  str << "***************************************************************************" << std::endl;
  str << "**** Assertion failed! \"" << description
    << "\" did not hold in " << func << "(): " << file << ":" << line << std::endl;
  str << "***************************************************************************" << std::endl;
  std::cerr << str.str();
}

void print_backtrace() {
  std::vector<std::string> traces = assorted::get_backtrace(true);
  std::stringstream str;
  str << "=== Stack frame (length=" << traces.size() << ")" << std::endl;
  for (uint16_t i = 0; i < traces.size(); ++i) {
    str << "- [" << i << "/" << traces.size() << "] " << traces[i] << std::endl;
  }
  std::cerr << str.str();
}

}  // namespace foedus
