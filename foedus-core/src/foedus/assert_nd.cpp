/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <execinfo.h>
#include <unistd.h>
#include <iostream>
#include <cstdlib>
namespace foedus {
void print_backtrace() {
    void *array[16];
    int size = ::backtrace(array, 16);
    std::cerr << "================== Dumping " << size << " stack frames..." << std::endl;
    // to avoid malloc issue while backtrace_symbols(), we use backtrace_symbols_fd.
    ::backtrace_symbols_fd(array, size, STDERR_FILENO);
}
}  // namespace foedus
