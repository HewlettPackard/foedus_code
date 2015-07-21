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
#include "foedus/proc/proc_options.hpp"

#include <cstdlib>
#include <sstream>
#include <string>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace proc {
ProcOptions::ProcOptions() {
  max_proc_count_ = kDefaultMaxProcCount;
  shared_library_path_pattern_ = "";
  shared_library_dir_pattern_ = "";
}

std::string ProcOptions::convert_shared_library_path_pattern(int node) const {
  return assorted::replace_all(shared_library_path_pattern_.str(), "$NODE$", node);
}

std::string ProcOptions::convert_shared_library_dir_pattern(int node) const {
  return assorted::replace_all(shared_library_dir_pattern_.str(), "$NODE$", node);
}

ErrorStack ProcOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, max_proc_count_);
  EXTERNALIZE_LOAD_ELEMENT(element, shared_library_path_pattern_);
  EXTERNALIZE_LOAD_ELEMENT(element, shared_library_dir_pattern_);
  return kRetOk;
}

ErrorStack ProcOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for loading system/user procedures"));

  EXTERNALIZE_SAVE_ELEMENT(element, max_proc_count_, "Maximum number of system/user procedures.");
  EXTERNALIZE_SAVE_ELEMENT(element, shared_library_path_pattern_,
    "String pattern of ';'-separated path of shared libraries to load in each NUMA node.\n"
    " The default value is empty, which means we don't load any shared libraries.\n"
    " If non-empty, we load the shared libraries of the path to register user-defined procedures.\n"
    " A placeholder '$NODE$' is replaced with the NUMA node number.");
  EXTERNALIZE_SAVE_ELEMENT(element, shared_library_dir_pattern_, "String pattern of "
    "';'-separated path of directories that contain shared libaries to load.\n"
    " Similar to shared_library_path_pattern_. The difference is that all '.so' files under"
    " the directory is loaded. The default value is empty.");
  return kRetOk;
}

}  // namespace proc
}  // namespace foedus
