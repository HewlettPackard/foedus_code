/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/fixed_error_stack.hpp"

#include <glog/logging.h>

#include <iostream>

#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
FixedErrorStack& FixedErrorStack::operator=(const ErrorStack &src) {
  os_errno_ = 0;
  error_code_ = kErrorCodeOk;
  stack_depth_ = 0;
  custom_message_.clear();
  if (!src.is_error()) {
    return *this;
  }

  if (src.get_custom_message()) {
    custom_message_.assign(src.get_custom_message());
  }
  stack_depth_ = src.get_stack_depth();
  for (uint16_t i = 0; i < stack_depth_; ++i) {
    filenames_[i].assign(src.get_filename(i));
    funcs_[i].assign(src.get_func(i));
    linenums_[i] = src.get_linenum(i);
  }
  os_errno_ = src.get_os_errno();
  error_code_ = src.get_error_code();
  return *this;
}

void FixedErrorStack::output(std::ostream* ptr) const {
  std::ostream &o = *ptr;  // just to workaround non-const reference rule.
  if (!is_error()) {
    o << "No error";
  } else {
    o << get_error_name(error_code_) << "(" << error_code_ << "):" << get_message();
    if (os_errno_ != 0) {
      o << " (Latest system call error=" << assorted::os_error(os_errno_) << ")";
    }
    if (!get_custom_message().empty()) {
      o << " (Additional message=" << get_custom_message() << ")";
    }

    for (uint16_t stack_index = 0; stack_index < get_stack_depth(); ++stack_index) {
      o << std::endl << "  " << get_filename(stack_index)
        << ":" << get_linenum(stack_index) << ": ";
      if (!get_func(stack_index).empty()) {
        o << get_func(stack_index) << "()";
      }
    }
    if (get_stack_depth() >= ErrorStack::kMaxStackDepth) {
      o << std::endl << "  .. and more. Increase kMaxStackDepth to see full stacktraces";
    }
  }
}

std::ostream& operator<<(std::ostream& o, const FixedErrorStack& obj) {
  obj.output(&o);
  return o;
}

}  // namespace foedus

