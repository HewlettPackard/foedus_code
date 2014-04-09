/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_GROUP_HPP_
#define FOEDUS_THREAD_THREAD_GROUP_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/initializable.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/thread/fwd.hpp>
namespace foedus {
namespace thread {
/**
 * @brief Represents a group of pre-allocated threads running in one NUMA node.
 * @ingroup THREAD
 * @details
 * Detailed description of this class.
 */
class ThreadGroup : public virtual Initializable {
 public:
    friend class ThreadPoolPimpl;
    ThreadGroup() CXX11_FUNC_DELETE;
    ThreadGroup(Engine* engine, ThreadGroupId group_id);
    ~ThreadGroup();
    ErrorStack  initialize() CXX11_OVERRIDE CXX11_FINAL;
    bool        is_initialized() const CXX11_OVERRIDE CXX11_FINAL;
    ErrorStack  uninitialize() CXX11_OVERRIDE CXX11_FINAL;

    ThreadGroupId           get_group_id() const;
    memory::NumaNodeMemory* get_node_memory() const;

    /** Returns Thread object for the given ordinal in this group. */
    Thread*     get_thread(ThreadLocalOrdinal ordinal) const;

 private:
    ThreadGroupPimpl*       pimpl_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_GROUP_HPP_
