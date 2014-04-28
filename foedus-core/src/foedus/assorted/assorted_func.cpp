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
#include <string>
namespace foedus {
namespace assorted {

int64_t int_div_ceil(int64_t dividee, int64_t dividor) {
    std::ldiv_t result = std::div(dividee, dividor);
    return result.rem != 0 ? (result.quot + 1) : result.quot;
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

