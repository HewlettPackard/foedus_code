/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/assorted/rich_backtrace.hpp"

#include <backtrace.h>
#include <execinfo.h>
#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
namespace assorted {
/*
std::string demangle_backtrace_line(const std::string& line, void* raw_address) {
  // Case 1: method in executable
  //   /foo/hoge/test_dummy(_ZN6foedus5func3Ev+0x9) [0x43e479]
  // In this case, we want:
  // addr2line -C -f -p -i -e /foo/hoge/test_dummy 0x43e479

  // Case 2: method in shared library
  //   /foo/hoge/libfoedus-core.so(+0x3075a5) [0x7f1b6b8b05a5]
  // In this case, we want:
  // addr2line -C -f -p -i -e /foo/hoge/libfoedus-core.so 0x3075a5

  // Case 3: method in shared library without offset
  //   /lib64/libpthread.so.0() [0x3d1ee0ef90]
  // In this case, we want:
  // addr2line -C -f -p -i -e /lib64/libpthread.so.0 0x3d1ee0ef90

  // Case 4: method in shared library without offset
  //   /lib64/libc.so.6(abort+0x148) [0x3d1e6370f8]
  // In this case, we want:
  // addr2line -C -f -p -i -e /lib64/libc.so.6 0x3d1e6370f8

  // In sum, here's the rule:
  // If content of "()" starts with "+", use it as offset.
  // If not, use the content of "[]" as offset.

  std::size_t pos = line.find("(");
  std::size_t pos2 = line.find(")");
  if (pos == std::string::npos || pos2 == std::string::npos || pos >= pos2) {
    return line + " (Failed to demangle)";
  }
  std::string binary = line.substr(0, pos);
  std::string address;
  if (line[pos + 1] == '+') {
    address = line.substr(pos + 2, pos2 - pos - 2);
  } else {
    std::size_t bra = line.rfind("[");
    std::size_t bra2 = line.rfind("]");
    if (bra == std::string::npos || bra2 == std::string::npos || bra >= bra2) {
      return line + " [Failed to demangle]";
    }
    address = line.substr(bra + 1, bra2 - bra - 1);
  }

  std::stringstream cmd;
  cmd << "addr2line -C -f -p -i -e \"" << binary << "\" " << address;
  std::string cmd_str = cmd.str();

  FILE* pipe = ::popen(cmd_str.c_str(), "r");
  if (!pipe) {
    return line + " (Failed to run addr2line)";
  }
  std::string converted;
  while (true) {
    char buffer[1024];
    if (::fgets(buffer, sizeof(buffer), pipe)) {
      converted += buffer;
    } else {
      break;
    }
  }
  ::pclose(pipe);
  Dl_info info;
  struct link_map *map;
  _dl_addr(raw_address, &info, &map, NULL);
  Dl_info info;
  if (::dladdr(raw_address, &info) && info.dli_sname && info.dli_saddr) {
    std::cout << "dladdr(" << raw_address << ")=" << info.dli_sname
      << ", " << info.dli_fname << "," << info.dli_fbase << "," << info.dli_saddr << std::endl;
  }
  return converted + "(raw backtrace: " + line + ")";
}
*/

void on_backtrace_create_state_error(void* /*data*/, const char* msg, int errnum) {
  // We don't use glog here, it might be not initialized.
  std::cerr << "[FOEDUS] libbacktrace failed to create state. msg=" << msg
    << ", err=" << os_error(errnum) << std::endl;
}

void on_backtrace_full_error(void* /*data*/, const char* msg, int errnum) {
  std::cerr << "[FOEDUS] libbacktrace failed to obtain full backtrace. msg=" << msg
    << ", err=" << os_error(errnum) << std::endl;
}

int on_backtrace_full_callback(
  void *data,
  uintptr_t pc,
  const char *filename,
  int lineno,
  const char *function) {
  if (data == nullptr) {
    std::cerr << "[FOEDUS] wtf?? on_backtrace_full_callback received null data" << std::endl;
    return 1;
  }

  std::vector<std::string>* ret = reinterpret_cast< std::vector<std::string>* >(data);
  std::stringstream str;
  str << " in ";
  if (function) {
    str << function << "() ";
  } else {
    str << "??? ";
  }
  if (filename) {
    str << filename;
  } else {
    str << "???";
  }
  str << ":" << lineno << "  [" << assorted::Hex(pc) << "]";
  ret->emplace_back(str.str());
  return 0;  // keep processing
}

std::vector<std::string> get_backtrace(bool rich) {
  std::vector<std::string> ret;

  if (rich) {
    // try to use the shiny new libbacktrace
    backtrace_state* state = ::backtrace_create_state(
      nullptr,  // let libbacktrace to figure out this executable
      0,  // only this thread accesses it
      on_backtrace_create_state_error,
      nullptr);
    if (state) {
      int full_result = ::backtrace_full(
        state,
        1,  // skip this method itself
        on_backtrace_full_callback,
        on_backtrace_full_error,
        &ret);
      if (full_result == 0) {
        return ret;
      }
    }
    std::cerr << "[FOEDUS] libbacktrace failed, so falling back to good old"
      << " backtrace/backtrace_symbols" << std::endl;
  }

  void *array[32];
  int size = ::backtrace(array, 32);
  char **traces = ::backtrace_symbols(array, size);
  for (int i = 1; i < size; ++i) {  // skip the first one as it's this method
    std::string str(traces[i]);
    ret.emplace_back(str);
  }
  ::free(traces);
  return ret;
}

}  // namespace assorted
}  // namespace foedus
