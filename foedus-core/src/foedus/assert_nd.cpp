/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <execinfo.h>
#include <iostream>
#include <cstdlib>
namespace foedus {
void print_backtrace() {
    void *array[16];
    int size = ::backtrace(array, 16);
    char** strings = ::backtrace_symbols(array, size);

    std::cout << "================== Assertion fired up. " << size << " stack frames." << std::endl;
    for (int i = 0; i < size; i++) {
        std::cout << "  " << strings[i] << std::endl;
    }

    std::free(strings);
}
}  // namespace foedus
