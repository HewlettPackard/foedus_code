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
#include "foedus/soc/soc_manager.hpp"

#include <vector>

#include "foedus/soc/soc_manager_pimpl.hpp"

namespace foedus {
namespace soc {
SocManager::SocManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new SocManagerPimpl(engine);
}
SocManager::~SocManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  SocManager::initialize() { return pimpl_->initialize(); }
bool        SocManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  SocManager::uninitialize() { return pimpl_->uninitialize(); }

SharedMemoryRepo* SocManager::get_shared_memory_repo() { return &pimpl_->memory_repo_; }

void SocManager::trap_spawned_soc_main() {
  std::vector< proc::ProcAndName > procedures;
  trap_spawned_soc_main(procedures);
}

void SocManager::trap_spawned_soc_main(const std::vector< proc::ProcAndName >& procedures) {
  SocManagerPimpl::spawned_child_main(procedures);
}

ErrorStack SocManager::wait_for_children_module(bool init, ModuleType module) {
  return pimpl_->wait_for_children_module(init, module);
}

ErrorStack SocManager::wait_for_master_module(bool init, ModuleType module) {
  return pimpl_->wait_for_master_module(init, module);
}
void SocManager::report_engine_fatal_error() { pimpl_->report_engine_fatal_error(); }

}  // namespace soc
}  // namespace foedus
