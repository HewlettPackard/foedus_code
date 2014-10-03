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
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "foedus/assorted/assorted_func.hpp"

namespace foedus {
namespace assorted {
struct BacktraceContext {
  enum Constants {
    kMaxDepth = 64,
  };
  struct GlibcBacktraceInfo {
    void*         address_;
    std::string   symbol_;
    std::string   function_;
    std::string   binary_path_;
    std::string   function_offset_;
    void parse_symbol();
  };
  struct LibBacktraceInfo {
    uintptr_t   address_;
    std::string srcfile_;
    int         srclineno_;
    std::string function_;
  };

  BacktraceContext() {}
  ~BacktraceContext() { release(); }
  void release() {
    error_.clear();
    glibc_bt_info_.clear();
    libbt_info_.clear();
  }

  void call_glibc_backtrace() {
    release();
    void* addresses[kMaxDepth];
    int depth = ::backtrace(addresses, kMaxDepth);
    char** symbols = ::backtrace_symbols(addresses, depth);
    for (int i = 1; i < depth; ++i) {  // start from 1 to skip this method (call_glibc_backtrace)
      GlibcBacktraceInfo info;
      info.address_ = addresses[i];
      info.symbol_ = symbols[i];
      info.parse_symbol();
      glibc_bt_info_.emplace_back(info);
    }
    ::free(symbols);
  }
  void on_libbt_create_state_error(const char* msg, int errnum) {
    std::stringstream message;
    message << "libbacktrace failed to create state. msg=" << msg << ", err=" << os_error(errnum);
    error_ = message.str();
  }

  void on_libbt_full_error(const char* msg, int errnum) {
    std::stringstream message;
    std::cerr << "libbacktrace failed to obtain backtrace. msg=" << msg
      << ", err=" << os_error(errnum);
    error_ = message.str();
  }

  void on_libbt_full(
    uintptr_t pc,
    const char *filename,
    int lineno,
    const char *function) {
    if (pc == -1ULL && filename == nullptr && function == nullptr) {
      // not sure why this happens, but libbacktrace occasionally gives such a dummy entry
      return;
    }
    LibBacktraceInfo info;
    info.address_ = pc;
    if (filename) {
      info.srcfile_ = filename;
    }
    info.srclineno_ = lineno;
    if (function) {
      info.function_ = function;
    }
    libbt_info_.emplace_back(info);
  }

  std::vector<std::string> get_results(uint16_t skip);

  std::string error_;
  std::vector<GlibcBacktraceInfo> glibc_bt_info_;
  std::vector<LibBacktraceInfo>   libbt_info_;
};
void libbt_create_state_error(void* data, const char* msg, int errnum) {
  if (data == nullptr) {
    std::cerr << "[FOEDUS] wtf. libbt_create_state_error received null" << std::endl;
  } else {
    reinterpret_cast<BacktraceContext*>(data)->on_libbt_create_state_error(msg, errnum);
  }
}
void libbt_full_error(void* data, const char* msg, int errnum) {
  if (data == nullptr) {
    std::cerr << "[FOEDUS] wtf. libbt_full_error received null" << std::endl;
  } else {
    reinterpret_cast<BacktraceContext*>(data)->on_libbt_full_error(msg, errnum);
  }
}
int libbt_full(void* data, uintptr_t pc, const char *filename, int lineno, const char *function) {
  if (data == nullptr) {
    std::cerr << "[FOEDUS] wtf. libbt_full received null" << std::endl;
    return 1;
  } else {
    reinterpret_cast<BacktraceContext*>(data)->on_libbt_full(pc, filename, lineno, function);
    return 0;
  }
}

void BacktraceContext::GlibcBacktraceInfo::parse_symbol() {
  // Case 1: /foo/hoge/test_dummy(_ZN6foedus5func3Ev+0x9) [0x43e479]
  // Case 2: /foo/hoge/libfoedus-core.so(+0x3075a5) [0x7f1b6b8b05a5]
  // Case 3: /lib64/libpthread.so.0() [0x3d1ee0ef90]
  // Case 4: /lib64/libc.so.6(abort+0x148) [0x3d1e6370f8]
  std::size_t pos = symbol_.find("(");
  std::size_t pos2 = symbol_.find(")");
  if (pos == std::string::npos || pos2 == std::string::npos || pos >= pos2) {
    return;
  }

  binary_path_ = symbol_.substr(0, pos);
  if (pos2 == pos + 1) {  // "()"
  } else if (symbol_[pos + 1] == '+') {
    function_offset_ = symbol_.substr(pos + 2, pos2 - pos - 2);
  } else {
    std::size_t plus = symbol_.find("+", pos);
    if (plus == std::string::npos || plus > pos2) {
    } else {
      std::string mangled = symbol_.substr(pos + 1, plus - pos - 1);
      function_ = demangle_type_name(mangled.c_str());
      function_offset_ = symbol_.substr(plus + 1, pos2 - plus - 1);
    }
  }
}

/* we have tried addr2line via popen, but libbacktrace should be better especially for shared lib.
std::string demangle_backtrace_line(const std::string& line, void* raw_address) {
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

std::vector< std::string > BacktraceContext::get_results(uint16_t skip) {
  std::vector< std::string > ret;
  if (!error_.empty()) {
    // If error happened, add it as the first entry.
    ret.emplace_back(error_);
    // still outputs the following as best-effort
  }

  for (uint16_t i = skip; i < glibc_bt_info_.size(); ++i) {
    std::stringstream str;
    const GlibcBacktraceInfo& libc = glibc_bt_info_[i];
    bool has_libbt = i < libbt_info_.size();
    str << "in ";
    std::string function;
    if (has_libbt && !libbt_info_[i].function_.empty()) {
      const LibBacktraceInfo& libbt = libbt_info_[i];
      // libbacktrace successfully got the information. it can provide the most helpful info
      str << demangle_type_name(libbt.function_.c_str())
        << " " << libbt.srcfile_ << ":" << libbt.srclineno_
        << "  (" << libc.binary_path_ << ")"
        << "  [" << assorted::Hex(libbt.address_) << "]";
      // also append glibc backtrace info.
      str << " (glibc: " << assorted::Hex(reinterpret_cast<uintptr_t>(libc.address_)) << ")";
    } else {
      // sometimes libbacktrace fails to parse a stack even glibc can parse.. probably a bug
      if (!libc.function_.empty()) {
        str << demangle_type_name(libc.function_.c_str());
      } else {
        str << "???";
      }
      str << " : " << libc.symbol_;
      if (has_libbt) {
        // also append libbacktrace info.
        const LibBacktraceInfo& libbt = libbt_info_[i];
        str << " (libbacktrace:";
        if (!libbt.srcfile_.empty() || libbt.srclineno_ != 0) {
          str << " " << libbt.srcfile_ << ":" << libbt.srclineno_;
        }
        str << " [" << assorted::Hex(libbt.address_) << "]" << ")";
      }
    }
    ret.emplace_back(str.str());
  }
  return ret;
}

std::vector<std::string> get_backtrace(bool rich) {
  BacktraceContext context;
  context.call_glibc_backtrace();
  if (rich) {
    // try to use the shiny new libbacktrace
    backtrace_state* state = ::backtrace_create_state(
      nullptr,  // let libbacktrace to figure out this executable
      0,  // only this thread accesses it
      libbt_create_state_error,
      &context);
    if (state) {
      int full_result = ::backtrace_full(
        state,
        0,
        libbt_full,
        libbt_full_error,
        &context);
      if (full_result != 0) {
        // return ret;
        std::cerr << "[FOEDUS] libbacktrace backtrace_full failed: " << full_result << std::endl;
      }
    }
  }

  return context.get_results(1);  // skip the first (this method itself)
}

}  // namespace assorted
}  // namespace foedus
