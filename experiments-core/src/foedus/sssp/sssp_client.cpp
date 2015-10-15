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
#include "foedus/sssp/sssp_client.hpp"

#include <glog/logging.h>

#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace sssp {

ErrorStack sssp_client_task(const proc::ProcArguments& args) {
  thread::Thread* context = args.context_;
  if (args.input_len_ != sizeof(SsspClientTask::Inputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  if (args.output_buffer_size_ < sizeof(SsspClientTask::Outputs)) {
    return ERROR_STACK(kErrorCodeUserDefined);
  }
  *args.output_used_ = sizeof(SsspClientTask::Outputs);
  const SsspClientTask::Inputs* inputs
    = reinterpret_cast<const SsspClientTask::Inputs*>(args.input_buffer_);
  SsspClientTask task(*inputs, reinterpret_cast<SsspClientTask::Outputs*>(args.output_buffer_));
  return task.run(context);
}

ErrorStack SsspClientTask::run(thread::Thread* context) {
  context_ = context;
  engine_ = context->get_engine();
  xct_manager_ = engine_->get_xct_manager();
  storages_.initialize_tables(engine_);
  channel_ = reinterpret_cast<SsspClientChannel*>(
    engine_->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
  std::memset(analytic_other_outputs_, 0, sizeof(analytic_other_outputs_));

  ErrorStack result;
  if (inputs_.navigational_) {
    result = run_impl_navigational();
  } else {
    result = run_impl_analytic();
  }

  if (result.is_error()) {
    LOG(ERROR) << "SSSP Client-" << get_worker_id() << " exit with an error:" << result;
  }
  ++channel_->exit_nodes_;
  return result;
}


}  // namespace sssp
}  // namespace foedus
