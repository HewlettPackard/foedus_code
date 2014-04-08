/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_pimpl.hpp>
namespace foedus {
Engine::Engine(const EngineOptions& options) {
    pimpl_ = new EnginePimpl(this, options);
}
Engine::~Engine() {
    delete pimpl_;
}

// simply forward to pimpl object
const EngineOptions& Engine::get_options() const    { return pimpl_->options_; }

debugging::DebuggingSupports& Engine::get_debug() const  { return pimpl_->debug_; }
fs::Filesystem&         Engine::get_filesystem() const  { return pimpl_->filesystem_; }
log::LogManager&        Engine::get_log() const     { return pimpl_->log_manager_; }
memory::EngineMemory&   Engine::get_memory() const  { return pimpl_->memory_; }
thread::ThreadPool&     Engine::get_thread_pool() const { return pimpl_->thread_pool_; }
bool                Engine::is_initialized() const  { return pimpl_->is_initialized(); }
ErrorStack          Engine::initialize()            { return pimpl_->initialize(); }
ErrorStack          Engine::uninitialize()          { return pimpl_->uninitialize(); }


}  // namespace foedus
