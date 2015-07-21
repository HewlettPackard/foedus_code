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
#include "foedus/savepoint/savepoint_options.hpp"

#include <string>

#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace savepoint {
SavepointOptions::SavepointOptions() {
  savepoint_path_ = "savepoint.xml";
}
ErrorStack SavepointOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, savepoint_path_);
  return kRetOk;
}

ErrorStack SavepointOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for savepoint manager"));
  EXTERNALIZE_SAVE_ELEMENT(element, savepoint_path_, "Full path of the savepoint file.");
  return kRetOk;
}

}  // namespace savepoint
}  // namespace foedus
