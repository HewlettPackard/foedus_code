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
#include "foedus/engine_options.hpp"

#include <tinyxml2.h>

#include <vector>

#include "foedus/assert_nd.hpp"

namespace foedus {
EngineOptions::EngineOptions() {
}
EngineOptions::EngineOptions(const EngineOptions& other) {
  operator=(other);
}

// template-ing just for const/non-const
template <typename ENGINE_OPTION_PTR, typename CHILD_PTR>
std::vector< CHILD_PTR > get_children_impl(ENGINE_OPTION_PTR option) {
  std::vector< CHILD_PTR > children;
  children.push_back(&option->cache_);
  children.push_back(&option->debugging_);
  children.push_back(&option->log_);
  children.push_back(&option->memory_);
  children.push_back(&option->proc_);
  children.push_back(&option->restart_);
  children.push_back(&option->savepoint_);
  children.push_back(&option->snapshot_);
  children.push_back(&option->soc_);
  children.push_back(&option->storage_);
  children.push_back(&option->thread_);
  children.push_back(&option->xct_);
  return children;
}
std::vector< externalize::Externalizable* > get_children(EngineOptions* option) {
  return get_children_impl<EngineOptions*, externalize::Externalizable*>(option);
}
std::vector< const externalize::Externalizable* > get_children(const EngineOptions* option) {
  return get_children_impl<const EngineOptions*, const externalize::Externalizable*>(option);
}

EngineOptions& EngineOptions::operator=(const EngineOptions& other) {
  auto mine = get_children(this);
  auto others = get_children(&other);
  ASSERT_ND(mine.size() == others.size());
  for (size_t i = 0; i < mine.size(); ++i) {
    mine[i]->assign(others[i]);
  }
  return *this;
}

ErrorStack EngineOptions::load(tinyxml2::XMLElement* element) {
  *this = EngineOptions();  // This guarantees default values for optional XML elements.
  for (externalize::Externalizable* child : get_children(this)) {
    CHECK_ERROR(get_child_element(element, child->get_tag_name(), child));
  }
  return kRetOk;
}

ErrorStack EngineOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options given to the engine at start-up"));
  for (const externalize::Externalizable* child : get_children(this)) {
    CHECK_ERROR(add_child_element(element, child->get_tag_name(), "", *child));
  }
  return kRetOk;
}

}  // namespace foedus
