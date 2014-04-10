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
 * @defgroup INITIALIZABLE Initialize/Uninitialize Resources
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
 * virtual functions (uninitialize()) in destructor as the class information was already torn down!
 * So, make sure you always explicitly call uninitialize().
 * If you didn't call uninitialize(), you actually leak the resource.
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
 *
 * @par UninitializeGuard, or how RAII fails in real world.
 * The code that instantiates an Initializable object must call uninitialize() for the reasons
 * above. Even if there is some error, which causes an early return from the function
 * by CHECK_ERROR() in many cases. Note that try-catch(...) does not work because this is NOT an
 * exception (and we never use exceptions in our code). \e YOU have to make sure uninitialize()
 * is called.
 * UninitializeGuard addresses this issue, \b but \b imperfectly. For example, use it like this:
 * @code{.cpp}
 * ErrorStack your_func() {
 *     YourInitializable object;
 *     CHECK_ERROR(object.initialize());
 *     {
 *         UninitializeGuard guard(object, UninitializeGuard::WARN_IF_UNINITIALIZE_ERROR);
 *         CHECK_ERROR(object.do_something());
 *         CHECK_ERROR(object.uninitialize());
 *     }
 * }
 * @endcode
 * The UninitializeGuard object automatically calls object.uninitialize() when do_something()
 * fails and we return from your_func(). \b HOWEVER, C++'s destructor can't propagate any errors,
 * in our case ErrorStack. The second argument (UninitializeGuard::WARN_IF_UNINITIALIZE_ERROR)
 * says that if it encounters an error while uninitialize() call, it just warns it in debug log.
 * Logging the error is not a perfect solution at all. So, this is just an imperfect safety net.
 * Unfortunately, this is all what we can do in C++ for objects that have non-trivial release.
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
     * This method is responsible for releasing all acquired resources when initialization fails.
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
     * @attention This method is NOT automatically called from the destructor.
     * This is due to the fundamental limitation in C++.
     * Explicitly call this method as soon as you are done, checking the returned value.
     * You can also use UninitializeGuard to ameliorate the issue, but it's not perfect.
     * @return The error this method encounters, if any.
     * In case there are multiple errors while uninitialization, the implementation should use
     * ErrorStackBatch to produce a batched ErrorStack object.
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
 * This class declares and defines the following methods:
 *  \li is_initialized()
 *  \li initialize()/uninitialize() that calls initialize_once()/uninitialize_once().
 * @par Declared methods
 * This class declares the following methods, which the class has to define in cpp:
 *  \li initialize_one()/uninitialize_once().
 * @par Disabled methods
 * This class disables the following methods, which don't fly with Initializable:
 *  \li default copy constructor.
 *  \li default copy assignment operator.
 */
class DefaultInitializable : public virtual Initializable {
 public:
    DefaultInitializable() : initialized_(false) {}
    virtual ~DefaultInitializable() {}

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
        ErrorStack init_error = initialize_once();
        if (init_error.is_error()) {
            // if error happes in the middle of initialization, we release resources we acquired.
            CHECK_ERROR(uninitialize_once());
            return init_error;
        }
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

/**
 * @brief Calls Initializable#uninitialize() automatically when it gets out of scope.
 * @ingroup INITIALIZABLE
 * @details
 * \b NOT \b A \b SILVER \b BULLET!
 * This is a scope guard object to help release Initializable objects, but there are
 * inherent difficulties to handle errors out of uninitialization. eg:
 * http://stackoverflow.com/questions/159240/raii-vs-exceptions
 *
 * The only correct solution is for every code to make sure calling uninitialize() explicitly
 * and handling the returned ErrorStack responsively.
 * This object is just an imperfect safety net.
 */
class UninitializeGuard {
 public:
    /**
     * Defines the behavior of this scope guard.
     */
    enum Policy {
        /**
         * Terminates the entire program if uninitialize() wasn't called when it gets out of scope.
         * This is used to look for coding issues where we overlook chances of early returns.
         * Although this is stringent, recommended.
         */
        ABORT_IF_NOT_EXPLICITLY_UNINITIALIZED = 0,
        /**
         * Automatically calls if uninitialize() wasn't called when it gets out of scope,
         * and terminates the entire program when uninitialize() actually returns an error.
         * In either case, we warn about the lack of explicit uninitialize() call in debug log.
         * This is the default.
         */
        ABORT_IF_UNINITIALIZE_ERROR,
        /**
         * Automatically calls if uninitialize() wasn't called when it gets out of scope,
         * and just complains when uninitialize() actually returns an error in debug log.
         */
        WARN_IF_UNINITIALIZE_ERROR,
        /**
         * Automatically calls if uninitialize() wasn't called when it gets out of scope,
         * and does nothing even when uninitialize() actually returns an error in debug log.
         * NOT RECOMMENDED.
         */
        SILENT,
    };
    UninitializeGuard(Initializable *target, Policy policy = ABORT_IF_UNINITIALIZE_ERROR)
        : target_(target), policy_(policy) {}
    ~UninitializeGuard();

 private:
    Initializable*  target_;
    Policy          policy_;
};

}  // namespace foedus
#endif  // FOEDUS_INITIALIZABLE_HPP_
