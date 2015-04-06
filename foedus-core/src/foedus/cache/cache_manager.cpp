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
#include "foedus/cache/cache_manager.hpp"

#include <string>

#include "foedus/cache/cache_manager_pimpl.hpp"

namespace foedus {
namespace cache {

CacheManager::CacheManager(Engine* engine) : pimpl_(nullptr) {
  pimpl_ = new CacheManagerPimpl(engine);
}
CacheManager::~CacheManager() {
  delete pimpl_;
  pimpl_ = nullptr;
}

ErrorStack CacheManager::initialize_once() { return pimpl_->initialize_once(); }
ErrorStack CacheManager::uninitialize_once() { return pimpl_->uninitialize_once(); }
std::string CacheManager::describe() const { return pimpl_->describe(); }

}  // namespace cache
}  // namespace foedus
