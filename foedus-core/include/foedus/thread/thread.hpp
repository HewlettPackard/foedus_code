/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_HPP_
#define FOEDUS_THREAD_THREAD_HPP_
#include <foedus/initializable.hpp>
#include <foedus/thread/fwd.hpp>
namespace foedus {
namespace thread {
/**
 * @brief Brief description of this class.
 * @ingroup THREAD
 * @details
 * Detailed description of this class.
 */
class Thread : public virtual Initializable {
 public:
    /**
     * Description of constructor.
     */
    Thread();
    /**
     * Description of destructor.
     */
    ~Thread();

 private:
    ThreadPimpl*    pimpl_;
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_HPP_
