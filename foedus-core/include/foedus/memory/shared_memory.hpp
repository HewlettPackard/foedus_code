/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_SHARED_MEMORY_HPP_
#define FOEDUS_MEMORY_SHARED_MEMORY_HPP_

#include <stdint.h>
#include <sys/types.h>

#include <iosfwd>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"

namespace foedus {
namespace memory {
/**
 * @brief Represents memory shared between processes.
 * @ingroup MEMORY
 * @details
 * This class was initially a part of AlignedMemory, but we separated it out to reflect the
 * special semantics of this type of memory.
 *
 * @par Alloc/dealloc type
 * As this is a shared memory, there is only one choice unlike AlignedMemory.
 * We always use shmget() with SHM_HUGETLB and shmat()/shmdt().
 *
 * @par Pros/cons of System-V shmget()
 * We made this choice of share memory allocation API after trying all available options.
 * The biggest pro of System-V shmget is that it does not need to mount a hugetlbfs for
 * allocating hugepages. The cons are that it can't exceed kernel.shmmax and also we have to
 * do a linux-dependent trick to reliably release the shared memory.
 * For other choices, see the sections below.
 *
 * @par Ownership
 * Every shared memory is allocated and owned by the master process.
 * Forked/execed child processes, whether they are 'emulated' processes or not, do not have
 * ownership thus should not release them at exit.
 *
 * @par Moveable/Copiable
 * This object is \e NOT copiable.
 * This object \e is moveable (the original object becomes null after the move).
 *
 * @par Why not POSIX shm_open()
 * It can't use hugepages, period. shm_open is preferable because it's POSIX standard and also
 * allows friendly naming to uniquefy the shared memory unlike shmget()'s integer ID.
 * However, so far it doesn't allow hugepages and also we have to manually delete the file
 * under /dev/shm.
 *
 * @par Why not open() and mmap() with MAP_SHARED
 * It requires mounting hugetlbfs and also the user has to specify the path of the mounted
 * file system in our configuration. This open/mmap is preferable because it is not affected
 * by kernel.shmmax, which was an 'oversight' and might be corrected later, though. See the
 * LWN article.
 *
 * @par Why not mmap() with MAP_PRIVATE and MAP_SHARED
 * To allow sharing memory between arbitrary processes, we need non-private shared memory.
 *
 * @par Debugging shared memories
 * ipcs -m
 * ipcrm, etc etc.
 *
 * @par Valgrind, shmat, and hugepages
 * Seems like valgrind has a bug on shmat with hugepages.
 * I filed a bug report, not sure how it will be resolved.
 * This causes all valgrind execution to fail on almost all testcases, crap.
 * As a tentative work around, I check whether we are running on valgrind and, if so, get rid
 * of SHM_HUGETLB from shmget call. Just for this reason, we have to include valgrind headers.
 * gggrrrr.
 *
 * @see http://lwn.net/Articles/375096/
 * @see https://groups.google.com/forum/#!topic/fa.linux.kernel/OKplGDFf3EU
 * @see http://www.linuxquestions.org/questions/programming-9/shmctl-ipc_rmid-precludes-further-attachments-574636/
 * @see https://bugs.kde.org/show_bug.cgi?id=338995
 */
class SharedMemory CXX11_FINAL {
 public:
  /** Empty constructor which allocates nothing. */
  SharedMemory() CXX11_NOEXCEPT
    : size_(0), numa_node_(0), shmid_(0), shmkey_(0), owner_pid_(0), block_(CXX11_NULLPTR) {}

  // Disable copy constructor
  SharedMemory(const SharedMemory &other) CXX11_FUNC_DELETE;
  SharedMemory& operator=(const SharedMemory &other) CXX11_FUNC_DELETE;

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  /**
   * Move constructor that steals the memory block from other.
   */
  SharedMemory(SharedMemory &&other) noexcept;
  /**
   * Move assignment operator that steals the memory block from other.
   */
  SharedMemory& operator=(SharedMemory &&other) noexcept;
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

  /** Automatically releases the memory. */
  ~SharedMemory() { release_block(); }

  /**
   * @brief Newly allocate a shared memory of given size on given NUMA node.
   * This method should be called only at the master process.
   * @param[in] meta_path This method will create a temporary meta file of this path and use it
   * to generate a unique ID via ftok() and also put the size of the memory as content.
   * It must be unique, for example '/tmp/foedus_shm_[master-PID]_vpool_[node-ID]'.
   * @param[in] size Byte size of the memory block. Actual allocation is at least of this size.
   * @param[in] numa_node Where the physical memory is allocated. Use for binding via libnuma.
   * @attention When memory allocation fails for some reason (eg Out-Of-Memory), this method
   * does NOT fail nor throws an exception. Instead, it sets the block_ NULL.
   * So, the caller is responsible for checking it after construction.
   */
  ErrorStack  alloc(const std::string& meta_path, uint64_t size, int numa_node);
  /**
   * @brief Attach an already-allocated shared memory so that this object points to the memory.
   * @param[in] meta_path Path of the temporary meta file that contains memory size and
   * shared memory ID.
   * @attention If binding fails for some reason, this method
   * does NOT fail nor throws an exception. Instead, it sets the block_ NULL.
   * So, the caller is responsible for checking it after construction.
   */
  void        attach(const std::string& meta_path);

  /** Returns the path of the meta file. */
  const std::string& get_meta_path() const { return meta_path_; }
  /** Returns the memory block. */
  char*       get_block() const { return block_; }
  /** Returns the ID of this shared memory */
  int         get_shmid() const { return shmid_; }
  /** Returns the key of this shared memory */
  key_t       get_shmkey() const { return shmkey_; }
  /** If non-zero, it means the ID of the process that allocated the shared memory. */
  pid_t       get_owner_pid() const { return owner_pid_; }
  /** Returns if this object doesn't hold a valid memory block. */
  bool        is_null() const { return block_ == CXX11_NULLPTR; }
  /** Returns if this process owns this memory and is responsible to delete it. */
  bool        is_owned() const;
  /** Returns the byte size of the memory block. */
  uint64_t    get_size() const { return size_; }
  /** Where the physical memory is allocated. */
  int         get_numa_node() const { return numa_node_; }

  /**
   * @brief Marks the shared memory as being removed so that it will be reclaimed when all processes
   * detach it.
   * @details
   * This is part of deallocation of shared memory in master process.
   * After calling this method, no process can attach this shared memory. So, do not call this
   * too early. However, on the other hand, do not call this too late.
   * If the master process dies for an unexpected reason, the shared memory remains until
   * next reboot. Call it as soon as child processes ack-ed that they have attached the memory
   * or that there are some issues the master process should exit.
   * This method is idempotent, meaning you can safely call this many times.
   */
  void        mark_for_release();

  /** Releases the memory block \b IF this process has an ownership. */
  void        release_block();

  friend std::ostream&    operator<<(std::ostream& o, const SharedMemory& v);

 private:
  /** Path of the meta file */
  std::string meta_path_;
  /** Byte size of the memory block. */
  uint64_t    size_;
  /** Where the physical memory is allocated. */
  int         numa_node_;
  /** Shared memory ID used for shmat/shmdt. */
  int         shmid_;
  /** Shared memory key used for shmget. */
  key_t       shmkey_;
  /**
   * If this value is non-zero and also equivalent to ::getpid(), this process is the one
   * that allocated this memory and also is responsible to delete it.
   */
  pid_t       owner_pid_;
  /** Allocated memory block. */
  char*       block_;

  void        generate_shmkey();
};

}  // namespace memory
}  // namespace foedus

#endif  // FOEDUS_MEMORY_SHARED_MEMORY_HPP_
