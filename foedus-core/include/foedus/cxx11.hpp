/*
 * Copyright (c), Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef _FOEDUS_CXX11_SHIMS_HPP
#define _FOEDUS_CXX11_SHIMS_HPP

/**
 * @defgroup CXX11
 * @brief Defines macros for hiding C++11 features in headers for client programs that use C++98.
 * @details
 * We basically \b do \b assume \b C++11 and our library provides the best flexibility when the
 * client program enables C++11. For example, the client program can simply contain foedus-core
 * as a subfolder and statically link to it if C++11 is enabled.
 * However, some client program might have to stick to C++98. In that case, we provide our library
 * as an external shared library which comes with public headers that at least compile in C++98.
 * Thus, we will make sure C++11 keywords and classes do not directly appear in public header files.
 * The macros defined in this file are for that switching.
 * Remember, this is only for public headers. We anyway compile our library with C++11.
 */

#if __cplusplus < 201103L
#pragma message("C++11 is disabled. libfoedus-core can be used without C++11,"
                " but enabling C++11 allows .")
#define DISABLE_CXX11_IN_PUBLIC_HEADERS
#endif  // __cplusplus < 201103L

#ifdef DISABLE_CXX11_IN_PUBLIC_HEADERS
#define CXX11_FUNC_DELETE
#define CXX11_FUNC_DEFAULT
#else   // DISABLE_CXX11_IN_PUBLIC_HEADERS
#define CXX11_FUNC_DELETE = delete
#define CXX11_FUNC_DEFAULT = default
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

#endif  // _FOEDUS_CXX11_SHIMS_HPP
