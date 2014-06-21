/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/thread/impersonate_task.hpp"
#include "foedus/thread/impersonate_task_pimpl.hpp"
namespace foedus {
namespace thread {
ImpersonateTask::ImpersonateTask() : pimpl_(new ImpersonateTaskPimpl()) {
}

ImpersonateTask::~ImpersonateTask() {
  delete pimpl_;
  pimpl_ = nullptr;
}
}  // namespace thread
}  // namespace foedus
