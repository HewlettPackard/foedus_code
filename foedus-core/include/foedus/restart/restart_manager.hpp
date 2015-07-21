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
#ifndef FOEDUS_RESTART_RESTART_MANAGER_HPP_
#define FOEDUS_RESTART_RESTART_MANAGER_HPP_
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/restart/fwd.hpp"
namespace foedus {
namespace restart {
/**
 * @brief Restart manager, which recovers the state of database by invoking log-gleaner
 * at start-up.
 * @ingroup RESTART
 * @details
 * @par Simplistic Restart
 * A DBMS usually has a complex ('sophisticated') restart routine especially if they try to reduce
 * start-up time. On the other hand, FOEDUS simply invokes the log gleaner just like a periodic
 * snapshot. This is a huge saving in code complexity. As far as we optimize the log gleaner,
 * the start-up time is not slower than typical replay-every-log-style traditional restart, either.
 */
class RestartManager CXX11_FINAL : public virtual Initializable {
 public:
  explicit RestartManager(Engine* engine);
  ~RestartManager();

  // Disable default constructors
  RestartManager() CXX11_FUNC_DELETE;
  RestartManager(const RestartManager&) CXX11_FUNC_DELETE;
  RestartManager& operator=(const RestartManager&) CXX11_FUNC_DELETE;

  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

 private:
  RestartManagerPimpl *pimpl_;
};
}  // namespace restart
}  // namespace foedus
#endif  // FOEDUS_RESTART_RESTART_MANAGER_HPP_
