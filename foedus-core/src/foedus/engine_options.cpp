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
#include <valgrind.h>
#include <sys/resource.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
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
uint64_t EngineOptions::get_available_hugepage_memory(std::ostream* details_out) {
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
  std::stringstream out_buffer;
  const uint64_t kMarginRatio = 4;  // Add 1/4 to be safe

  // we don't stop prescreening on individual errors so that
  // the user can see all issues at once.
  bool has_any_error = false;

  if (RUNNING_ON_VALGRIND && memory_.rigorous_page_boundary_check_) {
    out_buffer
      << "[FOEDUS] WARNING. We strongly discourage rigorous_page_boundary_check_ on valgrind."
      << " If you are sure what you are doing, consider increasing VG_N_SEGMENTS and recompile"
      << " valgrind."
      << std::endl;
  }

  // Check available hugepages
  uint64_t available_hugepage_bytes = get_available_hugepage_memory(&out_buffer);
  uint64_t required_shared_bytes;
  uint64_t required_local_bytes;
  calculate_required_memory(&required_shared_bytes, &required_local_bytes);

  uint64_t required_total_bytes = required_shared_bytes + required_local_bytes;
  uint64_t required_total_safe_bytes = required_total_bytes + required_total_bytes / kMarginRatio;
  if (available_hugepage_bytes < required_total_safe_bytes) {
    has_any_error = true;

    out_buffer
      << "[FOEDUS] There are not enough hugepages available."
      << " Based on the values in EngineOptions, the machine should have at least "
      << required_total_safe_bytes << " bytes ("
      << assorted::int_div_ceil(required_total_safe_bytes, 1ULL << 21) << " 2MB pages, or "
      << assorted::int_div_ceil(required_total_safe_bytes, 1ULL << 30) << " 1GB pages)"
      << " of hugepages, but there are only " << available_hugepage_bytes << " bytes available."
      << " eg: sudo sh -c 'echo xyz > /proc/sys/vm/nr_hugepages' "
      << std::endl;
  }

  // Check ulimit values
  prescreen_ulimits(required_total_safe_bytes, &has_any_error, &out_buffer);

  // Check sysctl values
  uint64_t required_shared_safe_bytes
    = required_shared_bytes + required_shared_bytes / kMarginRatio;
  prescreen_sysctl(required_shared_safe_bytes, &has_any_error, &out_buffer);

  std::string error_messages = out_buffer.str();
  *details_out << error_messages;

  if (has_any_error) {
    if (memory_.suppress_memory_prescreening_) {
      *details_out
        << "**********************************************************" << std::endl
        << "**** ENVIRONMENT PRESCREENING DETECTED SOME ISSUES." << std::endl
        << "**** HOWEVER, suppress_memory_prescreening option was specified." << std::endl
        << "**** FOEDUS will start up." << std::endl
        << "**********************************************************" << std::endl;
      return kRetOk;
    } else {
      *details_out
        << "**********************************************************" << std::endl
        << "**** ENVIRONMENT PRESCREENING FAILED." << std::endl
        << "**** FOEDUS does not start up because of issues listed above." << std::endl
        << "**********************************************************" << std::endl;
      return ERROR_STACK_MSG(kErrorCodeEnvPrescreenFailed, error_messages.c_str());
    }
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
  const uint64_t kMinNoFile = std::max(1U << 13, thread_.get_total_thread_count() * 16U);
  if (nofile_limit.rlim_cur < kMinNoFile) {
    *has_any_error = true;

    *details_out
      << "[FOEDUS] ulimit -n is too small (" << nofile_limit.rlim_cur
      << "). You must have at least " << kMinNoFile << std::endl;
  }
  // Record of a struggle: WTF,, no idea why, but I'm seeing an weird behavior only on Ubuntu.
  // I did set limits.conf, and ulimit -n is saying 100000, but the above code returns "8192"
  // on Ubuntu. As a tentative solution, reduced the min value to 8192.
  // This happens only when I run the code as jenkins user from jenkins service.
  // If I run it as myself, or "sudo su jenkins" then run it, it runs fine. WWWTTTTFFF.

  // 2015 Jun: Ahhh, I got it. It's because jenkins service is started by a daemon script:
  // http://blog.mindfab.net/2013/12/changing-ulimits-for-jenkins-daemons.html
  //  "The important part is that you have to specify the ulimits, e.g., for the number
  //   of open files before start-stop-daemon is called. The reason is that
  //   **start-stop-daemon doesn't consider pam**
  //   and hence will not find the limits which have been specified in /etc/security/limits.conf."

  // Note that proc means threads in linux.
  ::rlimit proc_limit;
  ::getrlimit(RLIMIT_NPROC, &proc_limit);
  const uint64_t kMinProc = std::max(1U << 12, thread_.get_total_thread_count() * 2U);
  if (proc_limit.rlim_cur < kMinProc) {
    *has_any_error = true;

    *details_out
      << "[FOEDUS] ulimit -u is too small(" << proc_limit.rlim_cur
      << "). You must have at least " << kMinProc << std::endl;
  }

  // memlock
  ::rlimit memlock_limit;
  ::getrlimit(RLIMIT_MEMLOCK, &memlock_limit);
  if (memlock_limit.rlim_cur * (1ULL << 10) < required_total_safe_bytes) {
    *has_any_error = true;

    *details_out
      << "[FOEDUS] ulimit -l is too small(" << memlock_limit.rlim_cur
      << "). You must have at least "
      << (required_total_safe_bytes >> 10) << std::endl;
  }

  // Should also check: RLIMIT_AS, RLIMIT_DATA, RLIMIT_FSIZE, RLIMIT_LOCKS
  // but it's rarely an issue in typical setup.
}

uint64_t EngineOptions::read_int_from_proc_fs(const char* path, std::ostream* details_out) {
  // _sysctl() is now strongly discouraged, so let's simlpy read as a file.
  std::ifstream file(path);
  if (!file.is_open()) {
    *details_out << "[FOEDUS] Fails to read " << path;
    return 0;
  }

  std::string line;
  std::getline(file, line);
  file.close();

  return std::stoull(line);
}

void EngineOptions::prescreen_sysctl(
  uint64_t required_shared_safe_bytes,
  bool* has_any_error,
  std::ostream* details_out) const {
  uint64_t shmall = read_int_from_proc_fs("/proc/sys/kernel/shmall", details_out);
  if (shmall < required_shared_safe_bytes) {
    *has_any_error = true;

    *details_out
      << "[FOEDUS] /proc/sys/kernel/shmall is too small (" << shmall << ".)"
      << " It must be at least " << required_shared_safe_bytes
      << ". We recommend to simply set semi-inifinite value: "
      << " sudo sysctl -w kernel.shmall=1152921504606846720"
      << " and adding an entry 'kernel.shmall = 1152921504606846720' to /etc/sysctl.conf"
      << " then sudo sysctl -p"
      << std::endl;
  }

  uint64_t shmmax = read_int_from_proc_fs("/proc/sys/kernel/shmmax", details_out);
  if (shmmax < required_shared_safe_bytes) {
    *has_any_error = true;

    *details_out
      << "[FOEDUS] /proc/sys/kernel/shmmax is too small(" << shmmax << ")."
      << " It must be at least " << required_shared_safe_bytes
      << ". We recommend to simply set semi-inifinite value: "
      << " sudo sysctl -w kernel.shmmax=9223372036854775807"
      << " and adding an entry 'kernel.shmmax = 9223372036854775807' to /etc/sysctl.conf"
      << " then sudo sysctl -p"
      << std::endl;
  }

  uint64_t shmmni = read_int_from_proc_fs("/proc/sys/kernel/shmmni", details_out);
  const uint64_t kMinShmmni = 4096;
  if (shmmni < kMinShmmni) {
    *has_any_error = true;

    *details_out
      << "[FOEDUS] /proc/sys/kernel/shmmni is too small(" << shmmni << ")."
      << " It must be at least " << kMinShmmni
      << ". We recommend to set : "
      << " sudo sysctl -w kernel.shmmni=" << kMinShmmni
      << " and adding an entry 'kernel.shmmni = " << kMinShmmni << "' to /etc/sysctl.conf"
      << " then sudo sysctl -p"
      << std::endl;
  }

  uint64_t shm_group = read_int_from_proc_fs("/proc/sys/vm/hugetlb_shm_group", details_out);
  // This one is not an error. It works in some environment even without this parameter.
  // So, we only warn about it so far. Also, we don't even check if the user is in this group.
  if (shm_group == 0) {
    *details_out
      << "[FOEDUS] Warning: /proc/sys/vm/hugetlb_shm_group is not set."
      << " In some environment, this is fine: FOEDUS can allocate shared memory backed by hugepages"
      << " without configuring it, but some environment might fail without it"
      << std::endl;
  }

  uint64_t map_count = read_int_from_proc_fs("/proc/sys/vm/max_map_count", details_out);
  if (map_count <= 65530U) {
    *details_out
      << "[FOEDUS] /proc/sys/vm/max_map_count is only " << map_count
      << " When rigorous_memory_boundary_check or rigorous_page_boundary_check features"
      << " are specified, you must set a large number to it."
      << ". We recommend to set : "
      << " sudo sysctl -w vm.max_map_count=2147483647"
      << " and adding an entry 'vm.max_map_count=2147483647' to /etc/sysctl.conf"
      << " then sudo sysctl -p"
      << std::endl;
  }
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
  *shared_bytes += (static_cast<uint64_t>(memory_.page_pool_size_mb_per_node_) << 20) * nodes;

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
