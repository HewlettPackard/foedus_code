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
#include <sys/resource.h>

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/soc/shared_memory_repo.hpp"

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

/** Returns byte size of available memory backed by non-transparent hugepages. */
uint64_t get_available_hugepage_memory(std::ostream* details_out) {
  std::ifstream file("/proc/meminfo");
  if (!file.is_open()) {
    *details_out << "[FOEDUS] Failed to open /proc/meminfo. Cannot check available hugepages."
      << std::endl;
    return 0;
  }

  std::string line;
  uint64_t hugepage_size = 1ULL << 21;
  uint64_t hugepage_count = 0;
  while (std::getline(file, line)) {
    if (line.find("Hugepagesize:") != std::string::npos) {
      // /proc/meminfo should have "Hugepagesize:    1048576 kB" for 1GB hugepages
      if (line.find("1048576 kB") != std::string::npos) {
        hugepage_size = 1ULL << 30;
      } else {
        ASSERT_ND(line.find("2048 kB") != std::string::npos);
      }
    } else if (line.find("HugePages_Free:") != std::string::npos) {
      ASSERT_ND(hugepage_count == 0);
      // And "HugePages_Free:    12345" for the number of available hugepages.
      std::string pages_str = line.substr(std::string("HugePages_Free:").length());
      hugepage_count = std::stoull(pages_str);
    }
  }
  file.close();
  return hugepage_count * hugepage_size;
}

ErrorStack EngineOptions::prescreen(std::ostream* details_out) const {
  ASSERT_ND(details_out);
  const uint64_t kMarginRatio = 4;  // Add 1/4 to be safe

  // we don't stop prescreening on individual errors so that
  // the user can see all issues at once.
  bool has_any_error = false;

  // total
  uint64_t available_hugepage_bytes = get_available_hugepage_memory(details_out);
  uint64_t required_shared_bytes;
  uint64_t required_local_bytes;
  calculate_required_memory(&required_shared_bytes, &required_local_bytes);

  uint64_t required_total_bytes = required_shared_bytes + required_local_bytes;
  uint64_t required_total_safe_bytes = required_total_bytes + required_total_bytes / kMarginRatio;
  if (available_hugepage_bytes < required_total_safe_bytes) {
    has_any_error = true;

    *details_out
      << "[FOEDUS] There are not enough hugepages available."
      << " Based on the values in EngineOptions, the machine should have at least "
      << required_total_safe_bytes << " bytes ("
      << assorted::int_div_ceil(required_total_safe_bytes, 1ULL << 21) << " 2MB pages, or "
      << assorted::int_div_ceil(required_total_safe_bytes, 1ULL << 30) << " 1GB pages)"
      << " of hugepages, but there are only " << available_hugepage_bytes << " bytes available."
      << " eg: sudo sh -c 'echo xyz > /proc/sys/vm/nr_hugepages' "
      << std::endl;
  }

  prescreen_ulimits(required_total_safe_bytes, &has_any_error, details_out);

  if (has_any_error) {
    *details_out
      << "**********************************************************" << std::endl
      << "**** ENVIRONMENT PRESCREENING FAILED." << std::endl
      << "**** FOEDUS does not start up because of issues listed above." << std::endl
      << "**********************************************************" << std::endl;

    return ERROR_STACK(kErrorCodeEnvPrescreenFailed);
  } else {
    return kRetOk;
  }
}

void EngineOptions::prescreen_ulimits(
  uint64_t required_total_safe_bytes,
  bool* has_any_error,
  std::ostream* details_out) const {
  // nofile (number of file/socket that can be opened)
  ::rlimit nofile_limit;
  ::getrlimit(RLIMIT_NOFILE, &nofile_limit);
  const uint64_t kMinNoFile = 1U << 16;
  if (nofile_limit.rlim_cur < kMinNoFile) {
    *has_any_error = true;

    *details_out
      << "[FOEDUS] ulimit -n is too small. You must have at least " << kMinNoFile << std::endl;
  }

  // Note that proc means threads in linux.
  ::rlimit proc_limit;
  ::getrlimit(RLIMIT_NPROC, &proc_limit);
  const uint64_t kMinProc = 1U << 16;
  if (proc_limit.rlim_cur < kMinProc) {
    *has_any_error = true;

    *details_out
      << "[FOEDUS] ulimit -u is too small. You must have at least " << kMinProc << std::endl;
  }

  // memlock
  ::rlimit memlock_limit;
  ::getrlimit(RLIMIT_MEMLOCK, &memlock_limit);
  if (memlock_limit.rlim_cur * (1ULL << 10) < required_total_safe_bytes) {
    *has_any_error = true;

    *details_out
      << "[FOEDUS] ulimit -l is too small. You must have at least "
      << (required_total_safe_bytes >> 10) << std::endl;
  }

  // Should also check: RLIMIT_AS, RLIMIT_DATA, RLIMIT_FSIZE, RLIMIT_LOCKS
  // but it's rarely an issue in typical setup.
}

void EngineOptions::calculate_required_memory(
  uint64_t* shared_bytes,
  uint64_t* local_bytes) const {
  const uint32_t nodes = thread_.group_count_;
  const uint32_t total_threads = thread_.get_total_thread_count();

  // First, shared memories. soc::SharedMemoryRepo has exact methods for that.
  *shared_bytes = 0;

  // No idea how big the XML representation would be, but surely within 4MB.
  uint64_t kMaxXmlSize = 1ULL << 22;
  *shared_bytes += soc::SharedMemoryRepo::calculate_global_memory_size(kMaxXmlSize, *this);
  *shared_bytes += soc::SharedMemoryRepo::calculate_node_memory_size(*this) * nodes;

  // Then, local memories, which are allocated in various places, so
  // we need to list up each of them.. maybe missing something.
  *local_bytes = 0;

  // snapshot cache pool
  *local_bytes += cache_.snapshot_cache_size_mb_per_node_ * (1ULL << 20) * nodes;

  // logger buffer
  *local_bytes += log_.log_buffer_kb_ * (1ULL << 10) * log_.loggers_per_node_ * nodes;

  // misc memory in NumaNodeMemory. for volatile pool and snapshot cache pool
  *local_bytes += sizeof(memory::PagePoolOffsetChunk) * 2ULL * total_threads * 2;

  // core-local memories in NumaCoreMemory. work_memory and "small_memory" (terrible name, yes)
  *local_bytes += xct_.local_work_memory_size_mb_ * (1ULL << 20) * total_threads;
  *local_bytes += memory::NumaCoreMemory::calculate_local_small_memory_size(*this) * total_threads;
}



}  // namespace foedus
