/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/thread/thread.hpp>
#include <foedus/thread/thread_group.hpp>
#include <foedus/thread/thread_options.hpp>
#include <vector>
namespace foedus {
namespace thread {
ErrorStack ThreadGroup::initialize_once() {
    ThreadLocalOrdinal count = engine_->get_options().thread_.thread_count_per_group_;
    for (ThreadLocalOrdinal ordinal = 0; ordinal < count; ++ordinal) {
        ThreadId id = compose_thread_id(group_id_, ordinal);
        threads_.push_back(new Thread(engine_, this, id));
        CHECK_ERROR(threads_.back()->initialize());
    }
    return RET_OK;
}

ErrorStack ThreadGroup::uninitialize_once() {
    ErrorStackBatch batch;
    batch.uninitialize_and_delete_all(&threads_);
    return SUMMARIZE_ERROR_BATCH(batch);
}

}  // namespace thread
}  // namespace foedus
