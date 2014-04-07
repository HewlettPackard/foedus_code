/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_INITIALIZABLE_HPP_
#define FOEDUS_INITIALIZABLE_HPP_

#include <foedus/cxx11.hpp>
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
 *
 * @par Macros
 * For most classes, you can use INITIALIZABLE_DEFAULT macro to save repetitive code.
 * For example, declare your class like the following:
 * @code{.cpp}
 * class YourClass {
 *  public:
 *     YourClass(const ConfigurationForYourClass &conf, int other_params, ...);
 *     ...
 *     INITIALIZABLE_DEFAULT; // which declares/defines overwritten methods for typical classes.
 *     ...
 *  private:
 *     bool  initialized_;
 * };
 * @endcode
 * Then, define initialize_once()/uninitialize_once() as follows in cpp.
 * @code{.cpp}
 * ErrorStack  YourClass::initialize_once() {
 *    .... // one-time initialization for this object
 *    return RET_OK;
 * }
 * ErrorStack  YourClass::uninitialize_once() {
 *    // one-time uninitialization for this object. continue releasing resources even when
 *    // there is some error. So, use ErrorStackBatch to wrap more than one errors.
 *    ErrorStackBatch batch;
 *    batch.emplace_back(some_uninitialization());
 *    batch.emplace_back(another_uninitialization());
 *    batch.emplace_back(yet_another_uninitialization());
 *    return batch.summarize();
 * }
 * @endcode
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
     * If and only if the return value was not an error, is_initialized() will return TRUE.
     * This method is usually not idempotent, but some implementation can choose to be. In that
     * case, the implementation class should clarify that it's idempotent.
     * This method itself is NOT thread-safe. Do not call this in a racy situation.
     */
    virtual ErrorStack  initialize() = 0;

    /** Returns whether the object has been already initialized or not. */
    virtual bool        is_initialized() const = 0;

    /** Sets whether the object has been already initialized or not. */
    virtual void        set_initialized(bool value) = 0;

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
     * In case there are multiple errors while uninitialization, the implementation should use
     * ErrorStackBatch to produce the ErrorStack object.
     */
    virtual ErrorStack  uninitialize() = 0;
};

/**
 * @brief Typical implementation of Initializable#initialize()
 * that provides initialize-once semantics.
 * @ingroup INITIALIZABLE
 * @tparam T the class that implements Initializable. It must define "ErrorStack initialize_once()"
 * to implement class-dependent initialization.
 * @details
 * These inlines functions not provided by Initializable itself to make it a pure interface class.
 */
template <class T>
inline ErrorStack initialize_once_guard(T *target) {
    if (target->is_initialized()) {
        return ERROR_STACK(ERROR_CODE_ALREADY_INITIALIZED);
    }
    CHECK_ERROR(target->initialize_once());
    target->set_initialized(true);
    return RET_OK;
}

/**
 * Typical implementation of Initializable#uninitialize() that provides uninitialize-once semantics.
 * @ingroup INITIALIZABLE
 * @tparam T the class that implements Initializable. It must define
 * "ErrorStack uninitialize_once()" to implement class-dependent uninitialization.
 */
template <class T>
inline ErrorStack uninitialize_once_guard(T *target) {
    if (!target->is_initialized()) {
        return RET_OK;
    }
    CHECK_ERROR(target->uninitialize_once());
    target->set_initialized(false);
    return RET_OK;
}

}  // namespace foedus


/**
 * @def INITIALIZABLE_DEFAULT
 * @brief This macro provides a skeleton of the most typical initialize/uninitialize-once objects.
 * @ingroup INITIALIZABLE
 * @details
 * The implementation must define "bool initializable_", "ErrorStack initialize_once()",
 * and "ErrorStack uninitialize_once()".
 */
#define INITIALIZABLE_DEFAULT \
ErrorStack  initialize() CXX11_OVERRIDE { return initialize_once_guard(this); }\
bool        is_initialized() const CXX11_OVERRIDE { return initialized_; }\
ErrorStack  uninitialize() CXX11_OVERRIDE { return uninitialize_once_guard(this); }\
void        set_initialized(bool initialized) CXX11_OVERRIDE { initialized_ = initialized; }\
ErrorStack  initialize_once();\
ErrorStack  uninitialize_once();

#endif  // FOEDUS_INITIALIZABLE_HPP_
