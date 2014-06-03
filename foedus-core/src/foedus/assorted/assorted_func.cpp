/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assorted/assorted_func.hpp>
#ifdef __GNUC__  // for get_pretty_type_name()
#include <cxxabi.h>
#endif  // __GNUC__
#include <stdint.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sstream>
namespace foedus {
namespace assorted {

int64_t int_div_ceil(int64_t dividee, int64_t dividor) {
    std::ldiv_t result = std::div(dividee, dividor);
    return result.rem != 0 ? (result.quot + 1) : result.quot;
}

std::string replace_all(const std::string& target, const std::string& search,
                           const std::string& replacement) {
    std::string subject = target;
    while (true) {
        std::size_t pos = subject.find(search);
        if (pos != std::string::npos) {
            subject.replace(pos, search.size(), replacement);
        } else {
            break;
        }
    }
    return subject;
}

std::string replace_all(const std::string& target, const std::string& search,
                           int replacement) {
    std::stringstream str;
    str << replacement;
    std::string rep = str.str();
    return replace_all(target, search, rep);
}

std::string os_error() {
    return os_error(errno);
}

std::string os_error(int error_number) {
    if (error_number == 0) {
        return "[No Error]";
    }
    std::stringstream str;
    // TODO(Hideaki) is std::strerror thread-safe? Thre is no std::strerror_r. Windows, mmm.
    str << "[Errno " << error_number << "] " << std::strerror(error_number);
    return str.str();
}


std::string demangle_type_name(const char* mangled_name) {
#ifdef __GNUC__
    int status;
    char* demangled = abi::__cxa_demangle(mangled_name, nullptr, nullptr, &status);
    if (demangled) {
        std::string ret(demangled);
        ::free(demangled);
        return ret;
    }
#endif  // __GNUC__
    return mangled_name;
}


}  // namespace assorted
}  // namespace foedus

