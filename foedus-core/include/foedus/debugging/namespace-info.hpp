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
 * @par DLOG and DVLOG
 * DLOG() and DVLOG() are completely disabled and wiped from our binary if our cmake build level is
 * RELEASE or RELWITHDBGINFO. Most of our code each user transaction goes through should
 * use only DLOG or DVLOG in its critical path.
 *
 * @par Debug Logging Configurations
 * Although we don't expose glog classes themselves in our APIs, we provide our own APIs
 * to configure them at both start-time and run-time for easier development. The start-time
 * configuration is foedus::debugging::DebuggingOptions (foedus::EngineOptions#debug_).
 * The run-time configuration APIs are provided by foedus::debugging::DebuggingSupports,
 * which you can obtain via foedus::Engine::get_debug().
 *
 * @par Debug Logging Level House-Rules
 *  \li LOG(FATAL) is used where we are not ready for graceful shutdown. Eventually there should
 * be none except asserting code bugs.
 *  \li LOG(ERROR) is the default level for problemetic cases. Most error logging would be this.
 * Do not use DLOG(ERROR). Instead always use LOG(ERROR).
 *  \li LOG(WARNING) should be used only for expected and minor user-triggered errors. If it's
 * either unexpected or major (eg out-of-disk is a user error, but worth raising as an error),
 * use LOG(ERROR). You can use DLOG(WARNING) to avoid affecting performance in critical places.
 *  \li For normal events that happen only once or a few times over very \e long time (seconds or
 * more), then use LOG(INFO). For example, module initializations logs.
 *  \li For more frequent normal events (hundreds/thousands per second), use DLOG(INFO).
 *  \li For debug-messages that \e YOU wouldn't want to see unless you are writing code
 * in that module, use VLOG. VLOG level 0: ~tens per second, 1: ~hundreds per second,
 * 2: ~thousand per second. If it's more than thousand per second, do NOT use VLOG.
 * It means (at least) that many level checkings per second! Use DLOG then.
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
