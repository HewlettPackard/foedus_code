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
#include "foedus/tpcc/tpcc_driver.hpp"

#include <iostream>
#include <string>

#include "foedus/engine_options.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/fs/path.hpp"

namespace foedus {
namespace tpcc {

void replicate_binaries(EngineOptions* options) {
  std::cout << "Replicating binaries for " << options->thread_.group_count_ << "nodes" << std::endl;
  std::string executable_path = assorted::get_current_executable_path();
  std::cout << "Oridinal exec='" << executable_path << "' ("
    << fs::file_size(fs::Path(executable_path)) << " bytes)" << std::endl;
}

}  // namespace tpcc
}  // namespace foedus
