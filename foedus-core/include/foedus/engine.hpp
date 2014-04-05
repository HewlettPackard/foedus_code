/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_HPP_
#define FOEDUS_ENGINE_HPP_

#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/initializable.hpp>
#include <foedus/fs/fwd.hpp>
#include <foedus/memory/fwd.hpp>
namespace foedus {

// forward declarations
class EnginePimpl;
class EngineOptions;

/**
 * @defgroup COMPONENTS FOEDUS Components
 * @brief Main modules of libfoedus.
 */

/**
 * @defgroup ENGINE \b Database \b Engine
 * @ingroup COMPONENTS
 * @brief Database engine, the top-level component of foedus.
 * @details
 * bluh
 */

/**
 * @brief Database engine object that holds all resources and provides APIs.
 * @ingroup ENGINE
 * @details
 * Detailed description of this class.
 */
class Engine : public virtual Initializable {
 public:
    /**
     * @brief Instantiates an engine object which is \b NOT initialized yet.
     * @details
     * To start the engine, call initialize() afterwards.
     * This constructor dose nothing but instantiation.
     */
    explicit Engine(const EngineOptions &options);

    /**
     * @brief Destructs all resources of this object \b IF they were not released yet.
     * @details
     * Most resources should be released by uninitialize(). If this destructor is called before
     * the call of uninitialize(), there was something wrong.
     * So, this destructor complains about it in stderr if that's the case.
     * Remember, destructor is not the best place to do complex things. Always use uninitialize()
     * for better handling of unexpected errors.
     */
    ~Engine();

    // Disable default constructors
    Engine() CXX11_FUNC_DELETE;
    Engine(const Engine &) CXX11_FUNC_DELETE;
    Engine& operator=(const Engine &) CXX11_FUNC_DELETE;

    /**
     * Starts up the database engine. This is the first method to call.
     * @see Initializable#initialize()
     */
    ErrorStack  initialize() CXX11_OVERRIDE;

    /**
     * Returns whether the engine is currently running.
     * @see Initializable#is_initialized()
     */
    bool        is_initialized() const CXX11_OVERRIDE;

    /**
     * Terminates the database engine. This is the last method to call.
     * @see Initializable#uninitialize()
     */
    ErrorStack  uninitialize() CXX11_OVERRIDE;

    const EngineOptions&    get_options() const;
    fs::Filesystem*         get_filesystem() const;
    memory::EngineMemory*   get_memory() const;

 private:
    EnginePimpl* pimpl_;
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_HPP_
