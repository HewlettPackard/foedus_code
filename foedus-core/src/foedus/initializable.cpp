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
#include "foedus/initializable.hpp"

#include <glog/logging.h>

#include <cstdlib>
#include <iostream>
#include <typeinfo>

#include "foedus/assert_nd.hpp"

namespace foedus {
UninitializeGuard::~UninitializeGuard() {
  if (target_->is_initialized()) {
    if (policy_ != kSilent) {
      LOG(ERROR) << "UninitializeGuard has found that " << typeid(*target_).name()
        <<  "#uninitialize() was not called when it was destructed. This is a BUG!"
        << " We must call uninitialize() before destructors!";
      print_backtrace();
    }
    if (policy_ == kAbortIfNotExplicitlyUninitialized) {
      LOG(FATAL) << "FATAL: According to kAbortIfNotExplicitlyUninitialized policy,"
        << " we abort the program" << std::endl;
      ASSERT_ND(false);
      std::abort();
    } else {
      // okay, call uninitialize().
      ErrorStack error = target_->uninitialize();
      // Note that this is AFTER uninitialize(). "target_" might be Engine
      // or DebuggingSupports. So, we might not be able to use glog any more.
      // Thus, we must use stderr in this case.
      if (error.is_error()) {
        switch (policy_) {
        case kAbortIfUninitializeError:
          std::cerr << "FATAL: UninitializeGuard encounters an error on uninitialize()."
            << " Aborting as we can't propagate this error appropriately."
            << " error=" << error << std::endl;
          ASSERT_ND(false);
          std::abort();
          break;
        case kWarnIfUninitializeError:
          std::cerr << "WARN: UninitializeGuard encounters an error on uninitialize()."
            << " We can't propagate this error appropriately. Not cool!"
            << " error=" << error << std::endl;
          break;
        default:
          // warns nothing. this policy is NOT recommended
          ASSERT_ND(policy_ == kSilent);
        }
      } else {
        if (policy_ != kSilent) {
          std::cerr << "But, fortunately uninitialize() didn't return errors, phew"
            << std::endl;
        }
      }
    }
  }
}
}  // namespace foedus

