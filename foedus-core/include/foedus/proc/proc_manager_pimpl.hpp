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
#ifndef FOEDUS_PROC_PROC_MANAGER_PIMPL_HPP_
#define FOEDUS_PROC_PROC_MANAGER_PIMPL_HPP_

#include <string>
#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/proc/fwd.hpp"
#include "foedus/proc/proc_id.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/shared_mutex.hpp"

namespace foedus {
namespace proc {
/**
 * This small control block is used to synchronize the access to the array.
 */
struct ProcManagerControlBlock {
  // this is backed by shared memory. not instantiation. just reinterpret_cast.
  ProcManagerControlBlock() = delete;
  ~ProcManagerControlBlock() = delete;

  void initialize() {
    lock_.initialize();
    count_ = 0;
  }
  void uninitialize() {
    lock_.uninitialize();
  }

  /**
   * Mutex to protect data.
   * Read access via process ID does not need a lock (because we only append to the last).
   * Modifications and reads via name (because it's sorted) needs to take a lock.
   */
  soc::SharedMutex  lock_;
  LocalProcId       count_;
};

/**
 * @brief Pimpl object of ProcManager.
 * @ingroup PROC
 * @details
 * A private pimpl object for ProcManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ProcManagerPimpl final : public DefaultInitializable {
 public:
  /** All shared data in this module */
  struct SharedData {
    SharedData() : control_block_(nullptr), procs_(nullptr), name_sort_(nullptr) {}
    ProcManagerControlBlock* control_block_;
    /** The procedure list maintained in this module is an array of ProcName. */
    ProcAndName*  procs_;
    /** IDs sorted by name for quick lookup */
    LocalProcId*  name_sort_;
  };

  ProcManagerPimpl() = delete;
  explicit ProcManagerPimpl(Engine* engine) : engine_(engine) {}

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  std::string describe_registered_procs() const;
  ErrorStack  get_proc(const ProcName& name, Proc* out);
  ErrorStack  pre_register(const ProcAndName& proc_and_name);
  ErrorStack  local_register(const ProcAndName& proc_and_name);
  ErrorStack  emulated_register(const ProcAndName& proc_and_name);
  SharedData* get_local_data();
  const SharedData* get_local_data() const;

  static LocalProcId find_by_name(const ProcName& name, SharedData* shared_data);
  static LocalProcId insert(const ProcAndName& proc_and_name, SharedData* shared_data);

  Engine* const               engine_;
  std::vector< ProcAndName >  pre_registered_procs_;
  /**
   * Shared data of all SOCs. Index is SOC ID.
   */
  std::vector< SharedData >   all_soc_procs_;
};
static_assert(
  sizeof(ProcManagerControlBlock) <= soc::NodeMemoryAnchors::kProcManagerMemorySize,
  "ProcManagerControlBlock is too large.");
}  // namespace proc
}  // namespace foedus
#endif  // FOEDUS_PROC_PROC_MANAGER_PIMPL_HPP_
