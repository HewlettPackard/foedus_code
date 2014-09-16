/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/thread/thread_ref.hpp"

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/thread/thread_pimpl.hpp"

namespace foedus {
namespace thread {

ThreadRef::ThreadRef()
  : engine_(CXX11_NULLPTR), id_(0), control_block_(CXX11_NULLPTR), mcs_blocks_(CXX11_NULLPTR) {}

ThreadRef::ThreadRef(Engine* engine, ThreadId id) : engine_(engine), id_(id) {
  soc::SharedMemoryRepo* memory_repo = engine->get_soc_manager().get_shared_memory_repo();
  soc::ThreadMemoryAnchors* anchors = memory_repo->get_thread_memory_anchors(id);
  control_block_ = anchors->thread_memory_;
  mcs_blocks_ = anchors->mcs_lock_memories_;
}

ThreadGroupRef::ThreadGroupRef() : engine_(nullptr), group_id_(0) {
}

ThreadGroupRef::ThreadGroupRef(Engine* engine, ThreadGroupId group_id)
  : engine_(engine), group_id_(group_id) {
  uint16_t count = engine->get_options().thread_.thread_count_per_group_;
  for (uint16_t i = 0; i < count; ++i) {
    threads_.emplace_back(ThreadRef(engine, compose_thread_id(group_id, i)));
  }
}


}  // namespace thread
}  // namespace foedus
