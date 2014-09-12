/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_PROC_PROC_MANAGER_PIMPL_HPP_
#define FOEDUS_PROC_PROC_MANAGER_PIMPL_HPP_

#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/proc/fwd.hpp"
#include "foedus/proc/proc_id.hpp"

namespace foedus {
namespace proc {
/**
 * @brief Pimpl object of ProcManager.
 * @ingroup PROC
 * @details
 * A private pimpl object for ProcManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ProcManagerPimpl final : public DefaultInitializable {
 public:
  /**
   * This small control block is used to synchronize the access to the array.
   */
  struct ControlBlock {
    // this is backed by shared memory. not instantiation. just reinterpret_cast.
    ControlBlock() = delete;
    ~ControlBlock() = delete;

    /**
     * A simple spin lock to protect data.
     * Read access via process ID does not need a lock (because we only append to the last).
     * Modifications and reads via name (because it's sorted) needs to take a lock.
     */
    bool        locked_;
    LocalProcId count_;
  };
  /** All shared data in this module */
  struct SharedData {
    SharedData() : control_block_(nullptr), procs_(nullptr), name_sort_(nullptr) {}
    ControlBlock* control_block_;
    /** The procedure list maintained in this module is an array of ProcName. */
    ProcAndName*  procs_;
    /** IDs sorted by name for quick lookup */
    LocalProcId*  name_sort_;
  };

  ProcManagerPimpl() = delete;
  explicit ProcManagerPimpl(Engine* engine) : engine_(engine) {}

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ErrorStack  pre_register(const ProcAndName& proc_and_name);
  ErrorStack  local_register(const ProcAndName& proc_and_name);
  ErrorStack  emulated_register(const ProcAndName& proc_and_name);

  static LocalProcId find_by_name(const ProcName& name, SharedData* shared_data);
  static LocalProcId insert(const ProcAndName& proc_and_name, SharedData* shared_data);

  Engine* const               engine_;
  std::vector< ProcAndName >  pre_registered_procs_;
  SharedData                  shared_data_;
};
static_assert(
  sizeof(ProcManagerPimpl::ControlBlock) <= (1U << 12),
  "ProcManagerPimpl::ControlBlock is too large.");
}  // namespace proc
}  // namespace foedus
#endif  // FOEDUS_PROC_PROC_MANAGER_PIMPL_HPP_
