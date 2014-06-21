/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/error_stack.hpp"

#include <glog/logging.h>

#include <iostream>

#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
void ErrorStack::output(std::ostream* ptr) const {
  std::ostream &o = *ptr;  // just to workaround non-const reference rule.
  if (!is_error()) {
    o << "No error";
  } else {
    o << get_error_name(error_code_) << "(" << error_code_ << "):" << get_message();
    if (os_errno_ != 0) {
      o << " (Latest system call error=" << assorted::os_error(os_errno_) << ")";
    }
    if (get_custom_message()) {
      o << " (Additional message=" << get_custom_message() << ")";
    }

    for (uint16_t stack_index = 0; stack_index < get_stack_depth(); ++stack_index) {
      o << std::endl << "  " << get_filename(stack_index)
        << ":" << get_linenum(stack_index) << ": ";
      if (get_func(stack_index) != nullptr) {
        o << get_func(stack_index) << "()";
      }
    }
    if (get_stack_depth() >= foedus::ErrorStack::kMaxStackDepth) {
      o << std::endl << "  .. and more. Increase kMaxStackDepth to see full stacktraces";
    }
  }
}

void ErrorStack::dump_and_abort(const char *abort_message) const {
  LOG(FATAL) << "FATAL:" << abort_message << std::endl << *this << std::endl;
  ASSERT_ND(false);
  std::cout.flush();
  std::cerr.flush();
  std::abort();
}

std::ostream& operator<<(std::ostream& o, const ErrorStack& obj) {
  obj.output(&o);
  return o;
}

}  // namespace foedus

