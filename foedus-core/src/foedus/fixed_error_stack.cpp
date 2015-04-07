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

ErrorStack FixedErrorStack::to_error_stack() const {
  if (!is_error()) {
    return kRetOk;
  }
  std::stringstream msg;
  output(&msg);
  return ERROR_STACK_MSG(error_code_, msg.str().c_str());
}

void FixedErrorStack::from_error_stack(const ErrorStack& other) {
  if (!other.is_error()) {
    clear();
    return;
  }

  error_code_ = other.get_error_code();
  os_errno_ = other.get_os_errno();
  stack_depth_ = other.get_stack_depth();
  custom_message_.clear();
  if (other.get_custom_message()) {
    custom_message_ = other.get_custom_message();
  }
  for (uint16_t i = 0; i < stack_depth_; ++i) {
    filenames_[i] = other.get_filename(i);
    funcs_[i] = other.get_func(i);
    linenums_[i] = other.get_linenum(i);
  }
}


std::ostream& operator<<(std::ostream& o, const FixedErrorStack& obj) {
  obj.output(&o);
  return o;
}

}  // namespace foedus

