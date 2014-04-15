/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_HPP_
#define FOEDUS_THREAD_THREAD_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/xct/fwd.hpp>
namespace foedus {
namespace thread {
/**
 * @brief Represents one thread running on one NUMA core.
 * @ingroup THREAD
 * @details
 */
class Thread CXX11_FINAL : public virtual Initializable {
 public:
    Thread() CXX11_FUNC_DELETE;
    explicit Thread(Engine* engine, ThreadGroupPimpl* group, ThreadId id);
    ~Thread();
    ErrorStack  initialize() CXX11_OVERRIDE;
    bool        is_initialized() const CXX11_OVERRIDE;
    ErrorStack  uninitialize() CXX11_OVERRIDE;

    Engine*     get_engine() const;
    ThreadId    get_thread_id() const;

    /** Returns whether this thread is now running a transaction. */
    bool        is_running_xct() const;

    /** Returns the private memory repository of this thread. */
    memory::NumaCoreMemory* get_thread_memory() const;


    /**
     * @brief Activates the transaction object on the thread.
     * @pre is_running_xct() == false
     */
    void            activate_xct();

    /**
     * @brief Deactivates the transaction object on the thread.
     * @pre is_running_xct() == true
     */
    void            deactivate_xct();

    /** Returns the pimpl of this object. Use it only when you know what you are doing. */
    ThreadPimpl*    get_pimpl() const { return pimpl_; }

 private:
    ThreadPimpl*    pimpl_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_HPP_
