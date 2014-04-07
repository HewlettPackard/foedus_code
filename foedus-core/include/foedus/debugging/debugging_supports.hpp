/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_DEBUGGING_DEBUGGING_SUPPORTS_HPP_
#define FOEDUS_DEBUGGING_DEBUGGING_SUPPORTS_HPP_
#include <foedus/initializable.hpp>
#include <foedus/debugging/debugging_options.hpp>
#include <string>
namespace foedus {
namespace debugging {
/**
 * @brief APIs to support debugging functionalities.
 * @ingroup DEBUGGING
 */
class DebuggingSupports : public virtual Initializable {
 public:
    /**
     * Description of constructor.
     */
    explicit DebuggingSupports(const DebuggingOptions& options);
    ~DebuggingSupports();

    // Disable default constructors
    DebuggingSupports() = delete;
    DebuggingSupports(const DebuggingSupports&) = delete;
    DebuggingSupports& operator=(const DebuggingSupports&) = delete;

    INITIALIZABLE_DEFAULT;

    /** @copydoc DebuggingOptions#debug_log_to_stderr_ */
    void                set_debug_log_to_stderr(bool value);
    /** @copydoc DebuggingOptions#debug_log_stderr_threshold_ */
    void                set_debug_log_stderr_threshold(DebuggingOptions::DebugLogLevel level);
    /** @copydoc DebuggingOptions#debug_log_min_threshold_ */
    void                set_debug_log_min_threshold(DebuggingOptions::DebugLogLevel level);
    /** @copydoc DebuggingOptions#verbose_log_level_ */
    void                set_verbose_log_level(int verbose);
    /** @copydoc DebuggingOptions#verbose_modules_ */
    void                set_verbose_modules(const std::string &modules);

 private:
    /**
     * Initialize Google-logging only once. This is called at the beginning of initialize_once()
     * so that all other initialization can use glog.
     */
    void                initialize_glog();
    /**
     * Uninitialize Google-logging only once.  This is called at the end of uninitialize_once()
     * so that all other uninitialization can use glog.
     */
    void                uninitialize_glog();
    void                spin_lock_glog();
    void                unlock_glog();

    const DebuggingOptions&     options_;

    bool                        initialized_;
};
}  // namespace debugging
}  // namespace foedus
#endif  // FOEDUS_DEBUGGING_DEBUGGING_SUPPORTS_HPP_
