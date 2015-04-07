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
#include "foedus/error_stack.hpp"

#include <glog/logging.h>

#include <iostream>
#include <sstream>
#include <string>

#include "foedus/assert_nd.hpp"
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

/**
 * Leaves recent dump information in a static global variable so that a signal handler can pick it.
 */
std::string static_recent_dump_and_abort;

void ErrorStack::dump_and_abort(const char *abort_message) const {
  std::stringstream str;
  str << "foedus::ErrorStack::dump_and_abort: " << abort_message << std::endl << *this << std::endl;
  str << print_backtrace();

  static_recent_dump_and_abort += str.str();
  LOG(FATAL) << str.str();
  ASSERT_ND(false);
  std::cout.flush();
  std::cerr.flush();
  std::abort();
}

std::string ErrorStack::get_recent_dump_and_abort() {
  return static_recent_dump_and_abort;
}

std::ostream& operator<<(std::ostream& o, const ErrorStack& obj) {
  obj.output(&o);
  return o;
}

}  // namespace foedus

