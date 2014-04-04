/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_INITIALIZABLE_HPP_
#define FOEDUS_INITIALIZABLE_HPP_

#include <foedus/error_stack.hpp>
namespace foedus {
/**
 * @defgroup INITIALIZABLE Initialize/Uninitialize
 * @brief Defines a uniform class interface to initialize/uninitialize non-trivial resources.
 * @details
 * @par Constructor/Destructor vs initialize()/uninitialize()
 * Constructor should not do complicated initialization as it can't return errors.
 * Instead, we universally use initialize()/uninitialize() semantics for all long-living objects.
 * By long-living, we mean at least seconds. Small and short-living objects do not have to
 * follow this semantics as it might cause unnecessary complexity or overhead.
 *
 * Same applies to Destructor vs uninitialize(). Explicitly call uninitialize() rather than
 * relying destructor. If you didn't call uninitialize(), destructor might complain about it
 * in logs.
 */

/**
 * The pure-virtual interface to initialize/uninitialize non-trivial resources.
 * @ingroup INITIALIZABLE
 */
class Initializable {
 public:
    virtual ~Initializable() {}

    /**
     * @brief Acquires resources in this object, usually called right after constructor.
     * @pre is_initialized() == FALSE
     * @details
     * If the return value was not an error, is_initialized() will return TRUE.
     * This method is usually not idempotent, but some implementation can choose to be. In that
     * case, the implementation class should clarify that it's idempotent.
     * This method itself is NOT thread-safe. Do not call this in a racy situation.
     */
    virtual ErrorStack  initialize() = 0;

    /** Returns whether the engine is currently running. */
    virtual bool        is_initialized() const = 0;

    /**
     * @brief An \e idempotent method to release all resources of this object, if any.
     * @details
     * After this method, is_initialized() will return FALSE.
     * \b Whether this method encounters an error or not, the implementation should make the best
     * effort to release as many resources as possible. In other words, Do not leak \b all resources
     * because of \b one issue.
     * This method itself is NOT thread-safe. Do not call this in a racy situation.
     * @attention This method would be also automatically called from the destructor if you did not
     * call it, but it's not a good practice as destructor can't return errors either.
     * Explicitly call this method as soon as you are done, checking the returned value.
     * @return information of the \e first error this method encounters.
     */
    virtual ErrorStack  uninitialize() = 0;
};

}  // namespace foedus
#endif  // FOEDUS_INITIALIZABLE_HPP_
