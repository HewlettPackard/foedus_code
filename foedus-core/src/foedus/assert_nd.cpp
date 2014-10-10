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
