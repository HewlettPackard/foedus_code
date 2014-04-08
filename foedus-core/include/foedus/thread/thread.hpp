/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_HPP_
#define FOEDUS_THREAD_THREAD_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/thread/fwd.hpp>
namespace foedus {
namespace thread {
/**
 * @brief Represents one thread running on one NUMA core.
 * @ingroup THREAD
 * @details
 */
class Thread : public virtual Initializable {
 public:
    Thread() CXX11_FUNC_DELETE;
    explicit Thread(Engine* engine, ThreadGroup* group, ThreadId id);
    ~Thread();
    ErrorStack  initialize() CXX11_OVERRIDE CXX11_FINAL;
    bool        is_initialized() const CXX11_OVERRIDE CXX11_FINAL;
    ErrorStack  uninitialize() CXX11_OVERRIDE CXX11_FINAL;

 private:
    ThreadPimpl*    pimpl_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_HPP_
