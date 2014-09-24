/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/soc_options.hpp"

#include <cstdlib>
#include <sstream>
#include <string>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace soc {
SocOptions::SocOptions() {
  soc_type_ = kChildEmulated;
  shared_user_memory_size_kb_ = kDefaultSharedUserMemorySizeKb;
  spawn_executable_pattern_ = "";
  spawn_ld_library_path_pattern_ = "";
}

std::string SocOptions::convert_spawn_executable_pattern(int node) const {
  if (spawn_executable_pattern_.empty()) {
    // if empty, use the current binary
    return assorted::get_current_executable_path();
  }
  return assorted::replace_all(spawn_executable_pattern_.str(), "$NODE$", node);
}

std::string SocOptions::convert_spawn_ld_library_path_pattern(int node) const {
  if (spawn_ld_library_path_pattern_.empty()) {
    // if empty, retrieve it from environment variable
    const char* master_value = std::getenv("LD_LIBRARY_PATH");
    if (master_value) {
      return master_value;
    } else {
      return "";
    }
  }
  return assorted::replace_all(spawn_ld_library_path_pattern_.str(), "$NODE$", node);
}

ErrorStack SocOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ENUM_ELEMENT(element, soc_type_);
  EXTERNALIZE_LOAD_ELEMENT(element, shared_user_memory_size_kb_);
  EXTERNALIZE_LOAD_ELEMENT(element, spawn_executable_pattern_);
  EXTERNALIZE_LOAD_ELEMENT(element, spawn_ld_library_path_pattern_);
  return kRetOk;
}

ErrorStack SocOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for SOC manager"));

  EXTERNALIZE_SAVE_ENUM_ELEMENT(element, soc_type_, "How to launch SOC engine instances.");
  EXTERNALIZE_SAVE_ELEMENT(element, shared_user_memory_size_kb_,
    "As part of the global shared memory, we reserve this size of 'user memory' that can be"
    " used for arbitrary purporses by the user to communicate between SOCs.");
  EXTERNALIZE_SAVE_ELEMENT(element, spawn_executable_pattern_,
    "String pattern of path of executables to spawn SOC engines in each NUMA node.\n"
    " The default value is empty, which means we use the binary of the master (/proc/self/exe).\n"
    " If non-empty, we use the path to launch each SOC engine.\n"
    " A placeholder '$NODE$' is replaced with the NUMA node number.\n"
    " If soc_type_ is not kChildLocalSpawned or kChildRemoteSpawned, this option is ignored.");
  EXTERNALIZE_SAVE_ELEMENT(element, spawn_ld_library_path_pattern_, "String pattern of "
    "LD_LIBRARY_PATH environment variable to spawn SOC engines in each NUMA node.\n"
    " The default value is empty, which means we don't overwrite LD_LIBRARY_PATH of this master"
    " process. To overwrite master process's LD_LIBRARY_PATH with empty value, put one space etc.");
  return kRetOk;
}

}  // namespace soc
}  // namespace foedus
