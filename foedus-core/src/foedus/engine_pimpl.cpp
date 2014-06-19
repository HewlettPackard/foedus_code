/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine_pimpl.hpp>
#include <foedus/error_stack_batch.hpp>
#include <glog/logging.h>
#include <algorithm>
namespace foedus {

EnginePimpl::EnginePimpl(Engine* engine, const EngineOptions &options) :
  options_(options),
  engine_(engine),
  // although we give a pointer to engine, these objects must not access it yet.
  // even the Engine object has not set the pimpl pointer.
  debug_(engine),
  memory_manager_(engine),
  savepoint_manager_(engine),
  thread_pool_(engine),
  log_manager_(engine),
  snapshot_manager_(engine),
  storage_manager_(engine),
  xct_manager_(engine),
  restart_manager_(engine) {
}


ErrorStack EnginePimpl::initialize_once() {
  for (Initializable* child : get_children()) {
    CHECK_ERROR(child->initialize());
  }
  LOG(INFO) << "================================================================================";
  LOG(INFO) << "================================== FOEDUS ENGINE INITIALIZATION DONE ===========";
  LOG(INFO) << "================================================================================";
  return kRetOk;
}
ErrorStack EnginePimpl::uninitialize_once() {
  LOG(INFO) << "================================================================================";
  LOG(INFO) << "================================== FOEDUS ENGINE EXITTING...... ================";
  LOG(INFO) << "================================================================================";
  ErrorStackBatch batch;
  // uninit in reverse order of initialization
  auto children = get_children();
  std::reverse(children.begin(), children.end());
  for (Initializable* child : children) {
    CHECK_ERROR(child->uninitialize());
  }
  return SUMMARIZE_ERROR_BATCH(batch);
}
}  // namespace foedus
