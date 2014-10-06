/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/test_common.hpp"

#include <execinfo.h>
#include <signal.h>
#include <tinyxml2.h>
#include <unistd.h>

#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "foedus/engine_options.hpp"
#include "foedus/assorted/rich_backtrace.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/fs/path.hpp"

namespace foedus {
  std::string get_random_name() {
    // to further randomize the name, we use hash of executable's path and its parameter.
    // we run many concurrent testcases, but all of them have different executable or parameters.

    std::string seed;
    std::ifstream in;
    in.open("/proc/self/cmdline", std::ios_base::in);
    if (!in.is_open()) {
      // there are cases where /proc/self/cmdline doesn't work. in that case just executable path
      seed = assorted::get_current_executable_path();
    } else {
      std::getline(in, seed);
      in.close();
    }

    std::hash<std::string> h1;
    uint64_t differentiator = h1(seed);
    return fs::unique_name("%%%%_%%%%_%%%%_%%%%", differentiator);
  }

  EngineOptions get_randomized_paths() {
    EngineOptions options;
    std::string uniquefier = get_random_name();
    std::cout << "test uniquefier=" << uniquefier << std::endl;
    {
      std::stringstream str;
      str << "tmp_logs/" << uniquefier << "/node_$NODE$/logger_$LOGGER$";
      options.log_.folder_path_pattern_.assign(str.str());
    }

    {
      std::stringstream str;
      str << "tmp_snapshots/" << uniquefier << "/node_$NODE$";
      options.snapshot_.folder_path_pattern_.assign(str.str());
    }

    options.savepoint_.savepoint_path_.assign(std::string("tmp_savepoints/") + uniquefier + ".xml");

    return options;
  }

  EngineOptions get_tiny_options() {
    EngineOptions options = get_randomized_paths();
    options.debugging_.debug_log_min_threshold_ = debugging::DebuggingOptions::kDebugLogInfo;
    options.debugging_.debug_log_stderr_threshold_
      = debugging::DebuggingOptions::kDebugLogInfo;
    options.debugging_.verbose_log_level_ = 1;
    options.debugging_.verbose_modules_ = "*";

    options.log_.log_buffer_kb_ = 1 << 8;
    options.memory_.page_pool_size_mb_per_node_ = 2;
    options.memory_.private_page_pool_initial_grab_ = 32;
    options.cache_.snapshot_cache_size_mb_per_node_ = 2;
    options.cache_.private_snapshot_cache_initial_grab_ = 32;
    options.thread_.group_count_ = 1;
    options.thread_.thread_count_per_group_ = 2;
    options.snapshot_.snapshot_interval_milliseconds_ = 1 << 26;  // never
    options.snapshot_.log_reducer_buffer_mb_ = 2;
    options.snapshot_.snapshot_writer_page_pool_size_mb_ = 2;
    options.storage_.max_storages_ = 128;
    return options;
  }

  void remove_files_start_with(const fs::Path &folder, const fs::Path &prefix) {
    if (fs::exists(folder)) {
      std::vector< fs::Path > child_paths(folder.child_paths());
      for (fs::Path child : child_paths) {
        if (child.string().find(prefix.string()) == 0) {
          fs::remove(child);
        }
      }
    }
  }
  void cleanup_test(const EngineOptions& options) {
    fs::remove(fs::Path(options.savepoint_.savepoint_path_.str()));
    fs::remove(fs::Path("tmp_logs"));
    fs::remove(fs::Path("tmp_snapshots"));
  }

  bool is_multi_nodes() {
    EngineOptions options;
    if (options.thread_.group_count_ > 1U) {
      return true;
    } else {
      std::cerr << "These tests inherently require multi NUMA nodes! skipping them." << std::endl;
      return false;
    }
  }


  std::string to_signal_name(int sig) {
    switch (sig) {
    case SIGHUP    : return "Hangup (POSIX).";
    case SIGINT    : return "Interrupt (ANSI).";
    case SIGQUIT   : return "Quit (POSIX).";
    case SIGILL    : return "Illegal instruction (ANSI).";
    case SIGTRAP   : return "Trace trap (POSIX).";
    case SIGABRT   : return "Abort (ANSI).";
    case SIGBUS    : return "BUS error (4.2 BSD).";
    case SIGFPE    : return "Floating-point exception (ANSI).";
    case SIGKILL   : return "Kill, unblockable (POSIX).";
    case SIGUSR1   : return "User-defined signal 1 (POSIX).";
    case SIGSEGV   : return "Segmentation violation (ANSI).";
    case SIGUSR2   : return "User-defined signal 2 (POSIX).";
    case SIGPIPE   : return "Broken pipe (POSIX).";
    case SIGALRM   : return "Alarm clock (POSIX).";
    case SIGTERM   : return "Termination (ANSI).";
    case SIGSTKFLT : return "Stack fault.";
    case SIGCHLD   : return "Child status has changed (POSIX).";
    case SIGCONT   : return "Continue (POSIX).";
    case SIGSTOP   : return "Stop, unblockable (POSIX).";
    case SIGTSTP   : return "Keyboard stop (POSIX).";
    case SIGTTIN   : return "Background read from tty (POSIX).";
    case SIGTTOU   : return "Background write to tty (POSIX).";
    case SIGURG    : return "Urgent condition on socket (4.2 BSD).";
    case SIGXCPU   : return "CPU limit exceeded (4.2 BSD).";
    case SIGXFSZ   : return "File size limit exceeded (4.2 BSD).";
    case SIGVTALRM : return "Virtual alarm clock (4.2 BSD).";
    case SIGPROF   : return "Profiling alarm clock (4.2 BSD).";
    case SIGWINCH  : return "Window size change (4.3 BSD, Sun).";
    case SIGIO   : return "I/O now possible (4.2 BSD).";
    case SIGPWR    : return "Power failure restart (System V).";
    case SIGSYS    : return "Bad system call.";
    default:
      return "UNKNOWN";
    }
  }
  std::string gtest_xml_path;
  std::string gtest_individual_test;
  std::string gtest_test_case_name;
  std::string gtest_package_name;
  std::string generate_failure_xml(const std::string& type, const std::string& details) {
    // The XML must be in JUnit format
    // https://svn.jenkins-ci.org/trunk/hudson/dtkit/dtkit-format/dtkit-junit-model/src/main/resources/com/thalesgroup/dtkit/junit/model/xsd/junit-4.xsd
    // http://windyroad.com.au/dl/Open%20Source/JUnit.xsd
    tinyxml2::XMLDocument doc;
    tinyxml2::XMLElement* root = doc.NewElement("testsuites");
    root->SetAttribute("name", "AllTests");
    root->SetAttribute("tests", 1);
    root->SetAttribute("failures", 1);
    root->SetAttribute("errors", 0);
    root->SetAttribute("time", 0);
    doc.InsertFirstChild(root);

    tinyxml2::XMLElement* suite = doc.NewElement("testsuite");
    suite->SetAttribute("name", (gtest_package_name + "." + gtest_test_case_name).c_str());
    suite->SetAttribute("tests", 1);
    suite->SetAttribute("failures", 1);
    suite->SetAttribute("errors", 0);
    suite->SetAttribute("disabled", 0);
    suite->SetAttribute("time", 0);
    root->InsertFirstChild(suite);

    tinyxml2::XMLElement* testcase = doc.NewElement("testcase");
    testcase->SetAttribute("name", gtest_individual_test.c_str());
    testcase->SetAttribute("status", "run");
    testcase->SetAttribute("classname", (gtest_package_name + "." + gtest_test_case_name).c_str());
    testcase->SetAttribute("time", 0);
    suite->InsertFirstChild(testcase);

    tinyxml2::XMLElement* test = doc.NewElement("failure");
    test->SetAttribute("type", type.c_str());
    test->SetAttribute("message", details.c_str());
    testcase->InsertFirstChild(test);

    tinyxml2::XMLPrinter printer;
    doc.Print(&printer);
    return printer.CStr();
  }
  std::string generate_failure_xml(int sig, const std::string& details) {
    return generate_failure_xml(to_signal_name(sig), details);
  }
  static void handle_signals(int sig, siginfo_t* si, void* /*unused*/) {
    std::stringstream str;
    str << "================================================================" << std::endl;
    str << "====   SIGNAL Received While Running Testcase" << std::endl;
    str << "====   SIGNAL Code=" << sig << "("<< to_signal_name(sig) << ")" << std::endl;
    str << "====   At address=" << si->si_addr << std::endl;
    str << "================================================================" << std::endl;

    std::vector<std::string> traces = assorted::get_backtrace(true);

    str << "=== Stack frame (length=" << traces.size() << ")" << std::endl;
    for (uint16_t i = 0; i < traces.size(); ++i) {
      str << "- [" << i << "/" << traces.size() << "] " << traces[i] << std::endl;
    }

    std::string details = str.str();
    std::cerr << details;

    if (gtest_xml_path.size() == 0) {
      std::cerr << "XML Output file was not specified, so we exit as a usual crash" << std::endl;
      ::exit(1);
    } else {
      std::cerr << "Converting the signal to a testcase failure in " << gtest_xml_path << std::endl;
      // We report this error in the result XML.
      std::string xml = generate_failure_xml(sig, details);
      std::cerr << "Xml content: " << std::endl << xml << std::endl;

      std::ofstream out;
      out.open(gtest_xml_path, std::ios_base::out | std::ios_base::trunc);
      if (!out.is_open()) {
        std::cerr << "Couldn't open xml file. os_error= " << assorted::os_error() << std::endl;
        ::exit(1);
      }
      out << xml;
      out.flush();
      out.close();
      std::cerr << "Wrote out result xml file. Now exitting.." << std::endl;
      ::exit(1);
    }
  }
  void register_signal_handlers(
    const char* test_case_name,
    const char* package_name,
    int argc,
    char** argv) {

    std::cout << "****************************************************************" << std::endl;
    std::cout << "*****  Started FOEDUS Unit Testcase " << std::endl;
    std::cout << "*****  Testcase name: " << test_case_name << std::endl;
    std::cout << "*****  Test Package name: " << package_name << std::endl;
    std::cout << "*****  Arguments (argc=" << argc << "): " << std::endl;

    gtest_test_case_name = test_case_name;
    gtest_package_name = package_name;
    gtest_xml_path = "";
    gtest_individual_test = "";
    for (int i = 0; i < argc; ++i) {
      std::cout << "*****    argv[" << i << "]: " << argv[i] << std::endl;
      std::string str(argv[i]);
      if (str.find("--gtest_output=xml:") == 0) {
        gtest_xml_path = str.substr(std::string("--gtest_output=xml:").size());
      } else if (str.find("--gtest_filter=*.") == 0) {
        gtest_individual_test = str.substr(std::string("--gtest_filter=*.").size());
      }
    }
    if (gtest_xml_path.size() > 0) {
      std::cout << "*****  XML Output: " << gtest_xml_path << std::endl;
    } else {
      std::cout << "*****  XML Output file was not specified. Executed manually?" << std::endl;
    }
    if (gtest_individual_test.size() > 0) {
      std::cout << "*****  Running an individual test: " << gtest_individual_test << std::endl;
    } else {
      std::cout << "*****  Individual test was not specified. Executed manually?" << std::endl;
    }
    std::cout << "****************************************************************" << std::endl;

    struct sigaction sa;
    sa.sa_flags = SA_SIGINFO;
    ::sigemptyset(&sa.sa_mask);
    sa.sa_sigaction = handle_signals;

    // we do not capture all signals. Only the followings are considered as 'expected'
    // testcase failures.
    ::sigaction(SIGABRT, &sa, nullptr);
    ::sigaction(SIGBUS, &sa, nullptr);
    ::sigaction(SIGFPE, &sa, nullptr);
    ::sigaction(SIGSEGV, &sa, nullptr);
    // Not surprisingly, SIGKILL/SIGSTOP cannot be captured:
    //  http://man7.org/linux/man-pages/man2/sigaction.2.html
    // This means we cannot capture timeout-kill by ctest which uses STOP (see kwsys/ProcessUNIX.c).
    // as we ignore exit-code of ctest in jenkins, this means timeout is silent. mm...
    // As a compromise, we pre-populate result xml as follows.
  }

  void pre_populate_error_result_xml() {
    if (gtest_xml_path.size() > 0) {
      std::string xml = generate_failure_xml(
        std::string("Pre-populated Error. Test timeout happned?"),
        std::string("This is an initially written gtest xml before test execution."
        " If you are receiving this result, most likely the process has disappeared without trace."
        " This can happen when ctest kills the process via SIGSTOP, or someone killed the process"
        " via SIGKILL, etc."));

      std::ofstream out;
      out.open(gtest_xml_path, std::ios_base::out | std::ios_base::trunc);
      if (out.is_open()) {
        out << xml;
        out.flush();
        out.close();
      }
    }
  }
}  // namespace foedus
