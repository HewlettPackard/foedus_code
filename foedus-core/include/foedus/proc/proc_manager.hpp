/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_PROC_PROC_MANAGER_HPP_
#define FOEDUS_PROC_PROC_MANAGER_HPP_

#include <string>
#include <vector>

#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/proc/fwd.hpp"
#include "foedus/proc/proc_id.hpp"

namespace foedus {
namespace proc {
/**
 * @brief Procedure manager, which maintains the list of system/user procedures.
 * @ingroup PROC
 */
class ProcManager CXX11_FINAL : public virtual Initializable {
 public:
  explicit ProcManager(Engine* engine);
  ~ProcManager();

  // Disable default constructors
  ProcManager() CXX11_FUNC_DELETE;
  ProcManager(const ProcManager&) CXX11_FUNC_DELETE;
  ProcManager& operator=(const ProcManager&) CXX11_FUNC_DELETE;

  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  /**
   * @brief Returns the function pointer of the specified procedure.
   * @param[in] name Name of the procedure that has been registered via one of the following methods
   * @param[out] out Function pointer of the procedure.
   * @return Error if the given procedure name is not found.
   */
  ErrorStack  get_proc(const ProcName& name, Proc* out);

  /**
   * @brief Pre-register a function pointer as a user procedure so that all SOCs will have it
   * when they are forked.
   * @pre Engine's initialize() is \b NOT yet called because this is a pre-registration.
   * @pre Child SOC types are either kChildEmulated or kChildForked.
   * @pre This is a master engine. (if this is a child SOC engine, pre-register is too late)
   * @details
   * This can be used only for kChildEmulated or kChildForked because function pointers can be
   * shared only when the function pointers are finalized before the fork, or in the same process.
   * So, once Engine is initialized, this method always fails.
   */
  ErrorStack  pre_register(const ProcAndName& proc_and_name);
  /** Just a synonym. */
  ErrorStack  pre_register(const ProcName& name, Proc proc) {
    return pre_register(ProcAndName(name, proc));
  }

  /** Returns procedures given to pre_register() */
  const std::vector< ProcAndName >& get_pre_registered_procedures() const;

  /**
   * @brief Register a function pointer as a user procedure in the current SOC.
   * @pre Engine is initialized.
   * @pre This engine is an SOC engine (child engine), not the master.
   * @details
   * This can be used any time after the initialization, but it takes effect only in the current
   * SOC. Function pointers cannot be shared with other processes.
   */
  ErrorStack  local_register(const ProcAndName& proc_and_name);

  /**
   * @brief Register a function pointer as a user procedure in all SOCs, assuming that
   * child SOCs are of kChildEmulated type.
   * @pre Engine is initialized.
   * @pre Child SOCs are of kChildEmulated type.
   * @details
   * This can be used any time after the initialization and also takes effect in all SOCs.
   * However, this can be used only with kChildEmulated SOCs.
   * Most testcases and small programs that do not need high scalability can use this.
   */
  ErrorStack  emulated_register(const ProcAndName& proc_and_name);

  /** For debug uses only. Returns a summary of procedures registered in this engine */
  std::string describe_registered_procs() const;

 private:
  ProcManagerPimpl *pimpl_;
};
}  // namespace proc
}  // namespace foedus
#endif  // FOEDUS_PROC_PROC_MANAGER_HPP_
