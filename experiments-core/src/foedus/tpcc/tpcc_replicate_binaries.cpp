/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
