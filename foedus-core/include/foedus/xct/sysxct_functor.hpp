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
#ifndef FOEDUS_XCT_SYSXCT_FUNCTOR_HPP_
#define FOEDUS_XCT_SYSXCT_FUNCTOR_HPP_

#include "foedus/error_code.hpp"

namespace foedus {
namespace xct {

/**
 * @brief A functor representing the logic in a system transaction via virtual-function.
 * @ingroup SYSXCT
 * @details
 * Each sysxct inherits this class and overrides the run() method to implement the sysxct.
 * This class is a pure interface. No data nor no pre-defined anything.
 *
 * We initially passed around template functors, but that demands exposing ThreadPimpl to
 * the code that implements individual sysxct. Rather, we now pass around a virtual function.
 * The cost of virtual function invocation should be negligible for each sysxct execution.
 * Well, let's keep an eye.
 *
 * @note Because this wil have vtable, do NOT place this object in shared memory!
 * You shouldn't place this object in any places other than passing around as a param.
 */
class SysxctFunctor {
 public:
  /**
   * Execute the system transaction. You should override this method.
   */
  virtual ErrorCode run() = 0;
};

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_SYSXCT_FUNCTOR_HPP_
