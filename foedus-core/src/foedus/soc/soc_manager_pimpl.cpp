/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/soc/soc_manager_pimpl.hpp"

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/epoch.hpp"
#include "foedus/error_stack_batch.hpp"

namespace foedus {
namespace soc {
// SOC manager is initialized at first even before debug module.
// So, we can't use glog yet.
ErrorStack SocManagerPimpl::initialize_once() {
  return kRetOk;
}

ErrorStack SocManagerPimpl::uninitialize_once() {
  ErrorStackBatch batch;
  return SUMMARIZE_ERROR_BATCH(batch);
}

}  // namespace soc
}  // namespace foedus
