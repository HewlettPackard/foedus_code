/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_DEBUGGING_NAMESPACE_INFO_HPP_
#define FOEDUS_DEBUGGING_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::debugging
 * @brief \b Debug-Support functionalities.
 * @details
 * The engine provides API to turn on/of various debug-support functionalities.
 *
 * @par Debug Logging with glog
 * We use the flexible debug-logging framework,
 * <a href="http://google-glog.googlecode.com/svn/trunk/doc/glog.html">Google-logging</a>,
 * or \e glog for short.
 * Everyone who writes code in libfoedus should be familiar with glog.
 * For example, use it as follows.
 * @code{.cpp}
 * #include <glog/logging.h>
 * void your_func() {
 *     VLOG(1) << "Entered your_func()";  // verbose debug log with level 1
 *     LOG(INFO) << "some info. some_var=" << some_var;  // debug log in INFO level
 *     if (some_error_happened) {
 *        LOG(ERROR) << "error!";  // debug log in ERROR level
 *     }
 * }
 * @endcode
 *
 * @par Where Debug Logs are output
 * With the default settings, all logs are written to /tmp/libfoedus.'level',
 * such as /tmp/libfoedus.INFO. LOG(ERROR) and LOG(FATAL) are also copied to stderr.
 * Logs of specific execution are written to another file,
 * /tmp/libfoedus.'hostname'.'user name'.log.'severity level'.'date'.'time'.'pid',
 * such as:
 * /tmp/libfoedus.hkimura-z820.kimurhid.log.INFO.20140406-215444.26946
 *
 * @par DLOG
 * DLOG() is completely disabled and wiped from our binary if our cmake build level is
 * RELEASE or RELWITHDBGINFO. Most of our code each user transaction goes through should
 * use only DLOG in its critical path.
 *
 * @par Debug Logging Configurations
 * Although we don't expose glog classes themselves in our APIs, we provide our own APIs
 * to configure them at both start-time and run-time for easier development. The start-time
 * configuration is foedus::debugging::DebuggingOptions (foedus::EngineOptions#debug_).
 * The run-time configuration APIs are provided by foedus::debugging::DebuggingSupports,
 * which you can obtain via foedus::Engine::get_debug().
 *
 * @par Performance Counters
 * TODO implement
 */

/**
 * @defgroup DEBUGGING Debug-Support functionalities
 * @ingroup COMPONENTS IDIOMS
 * @copydoc foedus::debugging
 */

#endif  // FOEDUS_DEBUGGING_NAMESPACE_INFO_HPP_
