/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_HPP_
#define FOEDUS_ENGINE_HPP_

#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
namespace foedus {

/**
 * @defgroup ENGINE Database Engine
 * @brief Database engine, the top-level component of foedus.
 * @details
 * bluh
 */

class EnginePimpl;
class EngineOptions;

/**
 * @brief Database engine object that holds all resources.
 * @ingroup ENGINE
 * @details
 * Detailed description of this class.
 */
class Engine {
 public:
    /**
     * @brief Instantiates an engine object which is \b NOT started yet.
     * @details
     * To start the engine, call run() afterwards.
     * This constructor dose nothing but instantiation.
     */
    explicit Engine(const EngineOptions &options);

    /**
     * @brief Destructs all resources of this object \b IF they were not released yet.
     * @details
     * Most resources should be released by shutdown(). If this destructor is called before
     * the call of shutdown(), there was something wrong.
     * So, this destructor complains about it in stderr if that's the case.
     * Remember, destructor is not the best place to do complex things. Always use shutdown() for
     * better handling of unexpected errors.
     */
    ~Engine();

    /**
     * @brief Starts up the database engine. This is the first method to call.
     * @pre is_running() == FALSE
     * @details
     * If the return value was not an error, is_running() will return TRUE.
     */
    ErrorStack  start();

    /** Returns whether the engine is currently running. */
    bool        is_running() const;

    /**
     * @brief Terminates the database engine. This is the last method to call.
     * @pre is_running() == TRUE
     * @details
     * Whether the return value was error or not, is_running() will return FALSE.
     * @attention This method is also automatically called from the destructor if you did not
     * call it, but it's not a good practice. Explicitly call this method as soon as you are done.
     */
    ErrorStack shutdown();

 private:
    EnginePimpl* pimpl_;
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_HPP_
