/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/thread/thread_group.hpp"

#include <ostream>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack_batch.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_options.hpp"

namespace foedus {
namespace thread {
ThreadGroup::ThreadGroup(Engine *engine, ThreadGroupId group_id)
  : engine_(engine), group_id_(group_id) {
}
ThreadGroup::~ThreadGroup() {
}

ErrorStack ThreadGroup::initialize_once() {
  node_memory_ = engine_->get_memory_manager().get_local_memory();
  ThreadLocalOrdinal count = engine_->get_options().thread_.thread_count_per_group_;
  for (ThreadLocalOrdinal ordinal = 0; ordinal < count; ++ordinal) {
    ThreadId id = compose_thread_id(group_id_, ordinal);
    ThreadGlobalOrdinal global_ordinal = to_global_ordinal(id, count);
    threads_.push_back(new Thread(engine_, id, global_ordinal));
    CHECK_ERROR(threads_.back()->initialize());
  }
  return kRetOk;
}

ErrorStack ThreadGroup::uninitialize_once() {
  ErrorStackBatch batch;
  batch.uninitialize_and_delete_all(&threads_);
  node_memory_ = nullptr;
  return SUMMARIZE_ERROR_BATCH(batch);
}

std::ostream& operator<<(std::ostream& o, const ThreadGroup& v) {
  o << "<ThreadGroup>";
  o << "<group_id_>" << static_cast<int>(v.group_id_) << "</group_id_>";
  o << "<threads_>";
  for (Thread* child_thread : v.threads_) {
    o << *child_thread;
  }
  o << "</threads_>";
  o << "</ThreadGroup>";
  return o;
}

}  // namespace thread
}  // namespace foedus
