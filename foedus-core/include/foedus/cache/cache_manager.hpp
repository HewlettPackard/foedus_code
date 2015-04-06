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
#ifndef FOEDUS_CACHE_CACHE_MANAGER_HPP_
#define FOEDUS_CACHE_CACHE_MANAGER_HPP_

#include <string>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/cache/cache_options.hpp"
#include "foedus/cache/fwd.hpp"

namespace foedus {
namespace cache {
/**
 * @brief Snapshot cache manager.
 * @details
 * A snapshot cache manager exists in each SOC engine, maintaining their SOC-local
 * snapshot cache. Everything is SOC-local, so actually a master engine does nothing here.
 */
class CacheManager : public DefaultInitializable {
 public:
  CacheManager() CXX11_FUNC_DELETE;
  explicit CacheManager(Engine* engine);
  ~CacheManager();
  ErrorStack  initialize_once() CXX11_OVERRIDE;
  ErrorStack  uninitialize_once() CXX11_OVERRIDE;

  const CacheOptions&    get_options() const;

  /** kind of to_string(). this might be slow, so do not call too often */
  std::string describe() const;

 private:
  CacheManagerPimpl* pimpl_;
};
}  // namespace cache
}  // namespace foedus
#endif  // FOEDUS_CACHE_CACHE_MANAGER_HPP_
