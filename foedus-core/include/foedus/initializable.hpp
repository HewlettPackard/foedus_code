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
 * @ingroup IDIOMS
 * @brief Defines a uniform class interface to initialize/uninitialize non-trivial resources.
 * @details
 * @par Constructor/Destructor vs initialize()/uninitialize()
 * Constructor should not do complicated initialization as it can't return errors.
 * Instead, we universally use initialize()/uninitialize() semantics for all long-living objects.
 * By long-living, we mean at least seconds. Small and short-living objects do not have to
 * follow this semantics as it might cause unnecessary complexity or overhead.
 *
 * Same applies to Destructor vs uninitialize(). Furthermore, C++ doesn't allow calling
 * virtual functions (uninitialize()) in destructor.. the class information was already torn down!
 * So, make sure you always explicitly call uninitialize().
 * If you didn't call uninitialize(), you actually leak the resource and
 * the destructor will complain about it in logs.
 *
 * @par DefaultInitializable
 * For most classes, you can use DefaultInitializable class to save repetitive code.
 * For example, declare your class like the following:
 * @code{.cpp}
 * class YourClass : public DefaultInitializable {
 *  public:
 *     YourClass(const ConfigurationForYourClass &conf, int other_params, ...);
 *     ...
 *     ErrorStack  initialize_once() CXX11_OVERRIDE;
 *     ErrorStack  uninitialize_once() CXX11_OVERRIDE;
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
 *    return SUMMARIZE_ERROR_BATCH(batch);
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
 * @brief Typical implementation of Initializable as a skeleton base class.
 * @ingroup INITIALIZABLE
 * @details
 * The implementation of Initializable can either derive from this class or define by its own
 * (eg. to avoid diamond inheritance).
 * If it chooses to use this class, it must define "ErrorStack initialize_once()",
 * and "ErrorStack uninitialize_once()".
 * @par Defined methods
 * This macro declares and defines the following methods:
 *  \li Destructor to do sanity check (uninitialize() called before destruction)
 *  \li set_initialized()/is_initialized()
 *  \li initialize()/uninitialize() that calls initialize_once_guard()/uninitialize_once_guard().
 * @par Declared methods
 * This macro declares the following methods, which the class has to define in cpp:
 *  \li initialize_one()/uninitialize_once().
 * @par Disabled methods
 * This macro disables the following methods, which don't fly with Initializable:
 *  \li default copy constructor.
 *  \li default copy assignment operator.
 */
class DefaultInitializable : public virtual Initializable {
 public:
     DefaultInitializable() : initialized_(false) {}
    /**
     * @brief Typical destructor for Initializable.
     * @details
     * It first checks if the object is already initialized. If so, it does nothing.
     * If not, it complains about it as a bug.
     */
    virtual ~DefaultInitializable();

    DefaultInitializable(const DefaultInitializable&) CXX11_FUNC_DELETE;
    DefaultInitializable& operator=(const DefaultInitializable&) CXX11_FUNC_DELETE;

    /**
     * Typical implementation of Initializable#initialize()
     * that provides initialize-once semantics.
     */
    ErrorStack  initialize() CXX11_OVERRIDE CXX11_FINAL {
        if (is_initialized()) {
            return ERROR_STACK(ERROR_CODE_ALREADY_INITIALIZED);
        }
        CHECK_ERROR(initialize_once());
        initialized_ = true;
        return RET_OK;
    }

    /**
     * Typical implementation of Initializable#uninitialize() that provides
     * uninitialize-once semantics.
     */
    ErrorStack  uninitialize() CXX11_OVERRIDE CXX11_FINAL {
        if (!is_initialized()) {
            return RET_OK;
        }
        CHECK_ERROR(uninitialize_once());
        initialized_ = false;
        return RET_OK;
    }

    bool        is_initialized() const CXX11_OVERRIDE CXX11_FINAL {
        return initialized_;
    }

    virtual ErrorStack  initialize_once() = 0;
    virtual ErrorStack  uninitialize_once() = 0;

 private:
    bool    initialized_;
};

}  // namespace foedus
#endif  // FOEDUS_INITIALIZABLE_HPP_
