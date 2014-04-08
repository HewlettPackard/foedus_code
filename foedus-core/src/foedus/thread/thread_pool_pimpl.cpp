/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/thread/thread_group.hpp>
#include <foedus/thread/thread_pool_pimpl.hpp>
#include <foedus/thread/thread_options.hpp>
#include <foedus/memory/engine_memory.hpp>
#include <foedus/engine_options.hpp>
namespace foedus {
namespace thread {
ErrorStack ThreadPoolPimpl::initialize_once() {
    if (!engine_->get_memory().is_initialized()) {
        return ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_INIT);
    }
    const ThreadOptions &options = engine_->get_options().thread_;
    assert(engine_->get_memory().is_initialized());
    for (ThreadGroupId group_id = 0; group_id < options.group_count_; ++group_id) {
        groups_.push_back(new ThreadGroup(engine_, group_id));
        CHECK_ERROR(groups_.back()->initialize());
    }
    return RET_OK;
}

ErrorStack ThreadPoolPimpl::uninitialize_once() {
    ErrorStackBatch batch;
    if (!engine_->get_memory().is_initialized()) {
        batch.emprace_back(ERROR_STACK(ERROR_CODE_DEPEDENT_MODULE_UNAVAILABLE_UNINIT));
    }
    batch.uninitialize_and_delete_all(&groups_);
    return SUMMARIZE_ERROR_BATCH(batch);
}

}  // namespace thread
}  // namespace foedus
