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
#ifndef FOEDUS_SAVEPOINT_SAVEPOINT_OPTIONS_HPP_
#define FOEDUS_SAVEPOINT_SAVEPOINT_OPTIONS_HPP_
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/externalize/externalizable.hpp"
#include "foedus/fs/filesystem.hpp"

namespace foedus {
namespace savepoint {
/**
 * @brief Set of options for savepoint manager.
 * @ingroup SAVEPOINT
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct SavepointOptions CXX11_FINAL : public virtual externalize::Externalizable {
  /**
   * Constructs option values with default values.
   */
  SavepointOptions();

  /**
   * @brief Full path of the savepoint file.
   * @details
   * This file is atomically and durably updated for each epoch-based commit.
   * Default is "savepoint.xml".
   */
  fs::FixedPath savepoint_path_;

  EXTERNALIZABLE(SavepointOptions);
};
}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_OPTIONS_HPP_
