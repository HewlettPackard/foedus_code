/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_HPP_
#define FOEDUS_THREAD_THREAD_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/log/fwd.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/memory/fwd.hpp>
#include <foedus/memory/page_resolver.hpp>
#include <foedus/xct/fwd.hpp>
#include <iosfwd>
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

    /**
     * Returns the transaction that is currently running on this thread.
     */
    xct::Xct&   get_current_xct();
    /** Returns if this thread is running an active transaction. */
    bool        is_running_xct() const;

    /** Returns the private memory repository of this thread. */
    memory::NumaCoreMemory* get_thread_memory() const;

    /**
     * @brief Returns the private log buffer for this thread.
     */
    log::ThreadLogBuffer&   get_thread_log_buffer();

    /**
     * Returns the page resolver to convert page ID to page pointer.
     * All worker threads copy the page resolver into its local memory at startup.
     * This gives the most efficient page resolve without any remote NUMA memory access.
     */
    const memory::GlobalPageResolver& get_global_page_resolver() const {
        return global_page_resolver_;
    }

    /** Returns the pimpl of this object. Use it only when you know what you are doing. */
    ThreadPimpl*    get_pimpl() const { return pimpl_; }

    friend std::ostream& operator<<(std::ostream& o, const Thread& v);

 private:
    /**
     * The page resolver to convert page ID to page pointer (a copy of the object in EngineMemory).
     * As an exception to pimpl idiom, this object is allocated in Thread itself.
     * This is because the page resolve is such a freuquently used object that must have
     * a really low overhead to retrieve (in other words, in-lined).
     * Fortunately, it has no dependency, so containing this object wouldn't cause an issue.
     */
    memory::GlobalPageResolver  global_page_resolver_;

    ThreadPimpl*    pimpl_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_HPP_
