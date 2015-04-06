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
#include "foedus/xct/xct_manager.hpp"
#include "foedus/xct/xct_manager_pimpl.hpp"
namespace foedus {
namespace xct {
XctManager::XctManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new XctManagerPimpl(engine);
}
XctManager::~XctManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack  XctManager::initialize() { return pimpl_->initialize(); }
bool        XctManager::is_initialized() const { return pimpl_->is_initialized(); }
ErrorStack  XctManager::uninitialize() { return pimpl_->uninitialize(); }
void        XctManager::pause_accepting_xct() { pimpl_->pause_accepting_xct(); }
void        XctManager::resume_accepting_xct() { pimpl_->resume_accepting_xct(); }
void XctManager::wait_for_current_global_epoch(Epoch target_epoch, int64_t wait_microseconds) {
  pimpl_->wait_for_current_global_epoch(target_epoch, wait_microseconds);
}


}  // namespace xct
}  // namespace foedus
