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
#include "foedus/memory/shared_memory.hpp"

#include <fcntl.h>
#include <numa.h>
#include <numaif.h>
#include <unistd.h>
#include <valgrind.h>  // just for RUNNING_ON_VALGRIND macro.
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/assorted_func.hpp"
#include "foedus/debugging/rdtsc.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/memory_id.hpp"

namespace foedus {
namespace memory {

// Note, we can't use glog in this file because shared memory is used before glog is initialized.

SharedMemory::SharedMemory(SharedMemory &&other) noexcept : block_(nullptr) {
  *this = std::move(other);
}
SharedMemory& SharedMemory::operator=(SharedMemory &&other) noexcept {
  release_block();
  meta_path_ = other.meta_path_;
  size_ = other.size_;
  numa_node_ = other.numa_node_;
  shmid_ = other.shmid_;
  shmkey_ = other.shmkey_;
  owner_pid_ = other.owner_pid_;
  block_ = other.block_;
  other.block_ = nullptr;
  return *this;
}

bool SharedMemory::is_owned() const {
  return owner_pid_ != 0 && owner_pid_ == ::getpid();
}

ErrorStack SharedMemory::alloc(const std::string& meta_path, uint64_t size, int numa_node) {
  release_block();

  if (size % (1ULL << 21) != 0) {
    size = ((size_ >> 21) + 1ULL) << 21;
  }

  // create a meta file. we must first create it then generate key.
  // shmkey will change whenever we modify the file.
  if (fs::exists(fs::Path(meta_path))) {
    std::string msg = std::string("Shared memory meta file already exists:") + meta_path;
    return ERROR_STACK_MSG(kErrorCodeSocShmAllocFailed, msg.c_str());
  }
  std::ofstream file(meta_path, std::ofstream::binary);
  if (!file.is_open()) {
    std::string msg = std::string("Failed to create shared memory meta file:") + meta_path;
    return ERROR_STACK_MSG(kErrorCodeSocShmAllocFailed, msg.c_str());
  }

  // randomly generate shmkey. We initially used ftok(), but it occasionally gives lots of
  // conflicts for some reason, esp on aarch64. we just need some random number, so here
  // we use pid and CPU cycle.
  pid_t the_pid = ::getpid();
  uint64_t key64 = debugging::get_rdtsc() ^ the_pid;
  key_t the_key = (key64 >> 32) ^ key64;

  if (the_key == 0) {
    // rdtsc and getpid not working??
    std::string msg = std::string("Dubious shmkey");
    return ERROR_STACK_MSG(kErrorCodeSocShmAllocFailed, msg.c_str());
  }

  // Write out the size/node/shmkey of the shared memory in the meta file
  file.write(reinterpret_cast<char*>(&size), sizeof(size));
  file.write(reinterpret_cast<char*>(&numa_node), sizeof(numa_node));
  file.write(reinterpret_cast<char*>(&the_key), sizeof(key_t));
  file.flush();
  file.close();

  size_ = size;
  numa_node_ = numa_node;
  owner_pid_ = the_pid;
  meta_path_ = meta_path;
  shmkey_ = the_key;

  // if this is running under valgrind, we have to avoid using hugepages due to a bug in valgrind.
  // When we are running on valgrind, we don't care performance anyway. So shouldn't matter.
  bool running_on_valgrind = RUNNING_ON_VALGRIND;
  // see https://bugs.kde.org/show_bug.cgi?id=338995

  // Use libnuma's numa_set_preferred to initialize the NUMA node of the memory.
  // This is the only way to control numa allocation for shared memory.
  // mbind does nothing for shared memory.
  ScopedNumaPreferred numa_scope(numa_node, true);

  shmid_ = ::shmget(
    shmkey_,
    size_,
    IPC_CREAT | IPC_EXCL | S_IRUSR | S_IWUSR | (running_on_valgrind ? 0 : SHM_HUGETLB));
  if (shmid_ == -1) {
    std::string msg = std::string("shmget() failed! size=") + std::to_string(size_)
      + std::string(", os_error=") + assorted::os_error() + std::string(", meta_path=") + meta_path;
    return ERROR_STACK_MSG(kErrorCodeSocShmAllocFailed, msg.c_str());
  }

  block_ = reinterpret_cast<char*>(::shmat(shmid_, nullptr, 0));

  if (block_ == reinterpret_cast<void*>(-1)) {
    ::shmctl(shmid_, IPC_RMID, nullptr);  // first thing. release it! before everything else.
    block_ = nullptr;
    std::stringstream msg;
    msg << "shmat alloc failed!" << *this << ", error=" << assorted::os_error();
    release_block();
    std::string str = msg.str();
    return ERROR_STACK_MSG(kErrorCodeSocShmAllocFailed, str.c_str());
  }

  std::memset(block_, 0, size_);  // see class comment for why we do this immediately
  // This memset takes a very long time due to the issue in linux kernel:
  // https://git.kernel.org/cgit/linux/kernel/git/torvalds/linux.git/commit/?id=8382d914ebf72092aa15cdc2a5dcedb2daa0209d
  // In linux 3.15 and later, this problem gets resolved and highly parallelizable.
  return kRetOk;
}

void SharedMemory::attach(const std::string& meta_path) {
  release_block();
  if (!fs::exists(fs::Path(meta_path))) {
    std::cerr << "Shared memory meta file does not exist:" << meta_path << std::endl;
    return;
  }
  // the meta file contains the size of the shared memory
  std::ifstream file(meta_path, std::ifstream::binary);
  if (!file.is_open()) {
    std::cerr << "Failed to open shared memory meta file:" << meta_path << std::endl;
    return;
  }
  uint64_t shared_size = 0;
  int numa_node = 0;
  key_t the_key = 0;
  file.read(reinterpret_cast<char*>(&shared_size), sizeof(shared_size));
  file.read(reinterpret_cast<char*>(&numa_node), sizeof(numa_node));
  file.read(reinterpret_cast<char*>(&the_key), sizeof(key_t));
  file.close();

  // we always use hugepages, so it's at least 2MB
  if (shared_size < (1ULL << 21)) {
    std::cerr << "Failed to read size of shared memory from meta file:" << meta_path
      << ". It looks like:" << shared_size << std::endl;
    return;
  }
  if (the_key == 0) {
    std::cerr << "Failed to read shmkey from meta file:" << meta_path << std::endl;
    return;
  }

  size_ = shared_size;
  numa_node_ = numa_node;
  meta_path_ = meta_path;
  shmkey_ = the_key;
  owner_pid_ = 0;

  shmid_ = ::shmget(shmkey_, size_, 0);
  if (shmid_ == -1) {
    std::cerr << "shmget() attach failed! size=" << size_ << ", error=" << assorted::os_error()
      << std::endl;
    return;
  }

  block_ = reinterpret_cast<char*>(::shmat(shmid_, nullptr, 0));
  if (block_ == reinterpret_cast<void*>(-1)) {
    block_ = nullptr;
    std::cerr << "shmat attach failed!" << *this << ", error=" << assorted::os_error() << std::endl;
    release_block();
    return;
  }
}

void SharedMemory::mark_for_release() {
  if (block_ != nullptr && shmid_ != 0) {
    // Some material says that Linux allows shmget even after shmctl(IPC_RMID), but it doesn't.
    // It allows shmat() after shmctl(IPC_RMID), but not shmget().
    // So we have to invoke IPC_RMID after all child processes acked.
    ::shmctl(shmid_, IPC_RMID, nullptr);
  }
}

void SharedMemory::release_block() {
  if (block_ != nullptr) {
    // mark the memory to be reclaimed
    if (is_owned()) {
      mark_for_release();
    }

    // Just detach it. as we already invoked shmctl(IPC_RMID) at beginning, linux will
    // automatically release it once the reference count reaches zero.
    int dt_ret = ::shmdt(block_);
    if (dt_ret == -1) {
      std::cerr << "shmdt() failed." << *this << ", error=" << assorted::os_error() << std::endl;
    }

    block_ = nullptr;

    // clean up meta file.
    if (is_owned()) {
      std::remove(meta_path_.c_str());
    }
  }
}

std::ostream& operator<<(std::ostream& o, const SharedMemory& v) {
  o << "<SharedMemory>";
  o << "<meta_path>" << v.get_meta_path() << "</meta_path>";
  o << "<size>" << v.get_size() << "</size>";
  o << "<owned>" << v.is_owned() << "</owned>";
  o << "<owner_pid>" << v.get_owner_pid() << "</owner_pid>";
  o << "<numa_node>" << v.get_numa_node() << "</numa_node>";
  o << "<shmid>" << v.get_shmid() << "</shmid>";
  o << "<shmkey>" << v.get_shmkey() << "</shmkey>";
  o << "<address>" << reinterpret_cast<uintptr_t>(v.get_block()) << "</address>";
  o << "</SharedMemory>";
  return o;
}

}  // namespace memory
}  // namespace foedus

