/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_GROUP_PIMPL_HPP_
#define FOEDUS_THREAD_THREAD_GROUP_PIMPL_HPP_
#include <foedus/initializable.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <iosfwd>
#include <vector>
namespace foedus {
namespace thread {
/**
 * @brief Pimpl object of ThreadGroup.
 * @ingroup THREAD
 * @details
 * A private pimpl object for ThreadGroup.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ThreadGroupPimpl final : public DefaultInitializable {
 public:
    ThreadGroupPimpl() = delete;
    ThreadGroupPimpl(Engine* engine, ThreadGroupId group_id)
        : engine_(engine), group_id_(group_id), node_memory_(nullptr) {}
    ErrorStack  initialize_once() override final;
    ErrorStack  uninitialize_once() override final;

    friend  std::ostream& operator<<(std::ostream& o, const ThreadGroupPimpl& v);

    Engine* const           engine_;

    /** ID of this thread group. */
    ThreadGroupId           group_id_;

    /**
     * Memory repository shared among threads in this group.
     * ThreadGroup does NOT own it, meaning it doesn't call its initialize()/uninitialize().
     * EngineMemory owns it in terms of that.
     */
    memory::NumaNodeMemory* node_memory_;

    /**
     * List of Thread in this group. Index is ThreadLocalOrdinal.
     */
    std::vector<Thread*>    threads_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_GROUP_PIMPL_HPP_
