/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_GROUP_HPP_
#define FOEDUS_THREAD_THREAD_GROUP_HPP_
#include <foedus/initializable.hpp>
namespace foedus {
namespace thread {
/**
 * @brief Brief description of this class.
 * @ingroup THREAD
 * @details
 * Detailed description of this class.
 */
class ThreadGroup : public virtual Initializable {
 public:
    /**
     * Description of constructor.
     */
    ThreadGroup();
    /**
     * Description of destructor.
     */
    ~ThreadGroup();
 private:
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_GROUP_HPP_
