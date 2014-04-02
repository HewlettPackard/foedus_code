/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_CXX11_HPP_
#define FOEDUS_CXX11_HPP_

/**
 * @defgroup CXX11 C++11 Keywords in Public Headers
 * @brief Defines macros for hiding C++11 features in public headers for clients that use C++98.
 * @details
 * We basically \b do \b assume \b C++11 and our library provides the best flexibility when the
 * client program enables C++11. For example, the client program can simply contain foedus-core
 * as a subfolder and statically link to it if C++11 is enabled.
 * However, some client program might have to stick to C++98. In that case, we provide our library
 * as an external shared library which comes with public headers that at least compile in C++98.
 * Thus, we will make sure C++11 keywords and classes do not directly appear in public header files.
 * The macros defined in this file are for that switching.
 *
 * Remember, this is only for public headers. We anyway compile our library with C++11.
 * We can freely use C++11 keywords/features in cpp and non-public header files, such as
 * xxx-inl.hpp and xxx_pimpl.hpp. In other words, client programs must not include them unless
 * they turn on C++11.
 */

/**
 * @def DISABLE_CXX11_IN_PUBLIC_HEADERS
 * @ingroup CXX11
 * @brief If defined, our public headers must hide all C++11 dependent APIs.
 */
#if __cplusplus < 201103L
#pragma message("C++11 is disabled. libfoedus-core can be used without C++11,"
                " but enabling C++11 allows more flexible use of the library.")
#define DISABLE_CXX11_IN_PUBLIC_HEADERS
#endif  // __cplusplus < 201103L

/**
 * @def CXX11_FUNC_DELETE
 * @ingroup CXX11
 * @brief Used in public headers in place of " = delete" of C++11.
 * @note C++98 : nothing.
 */
/**
 * @def CXX11_FUNC_DEFAULT
 * @ingroup CXX11
 * @brief Used in public headers in place of " = default" of C++11.
 * @note C++98 : nothing.
 */
/**
 * @def CXX11_CONSTEXPR
 * @ingroup CXX11
 * @brief Used in public headers in place of "constexpr" of C++11.
 * @note C++98 : nothing.
 */
/**
 * @def CXX11_NULLPTR
 * @ingroup CXX11
 * @brief Used in public headers in place of "nullptr" of C++11.
 * @note C++98 : NULL.
 */
/**
 * @def CXX11_NOEXCEPT
 * @ingroup CXX11
 * @brief Used in public headers in place of "noexcept" of C++11.
 * @note C++98 : nothing.
 */
/**
 * @def CXX11_STATIC_ASSERT
 * @ingroup CXX11
 * @brief Used in public headers in place of "static_assert" of C++11.
 * @note C++98 : nothing.
 */
#ifdef DISABLE_CXX11_IN_PUBLIC_HEADERS
#define CXX11_FUNC_DELETE
#define CXX11_FUNC_DEFAULT
#define CXX11_CONSTEXPR
#define CXX11_NULLPTR NULL
#define CXX11_NOEXCEPT
#define CXX11_STATIC_ASSERT(expr, message)
#else   // DISABLE_CXX11_IN_PUBLIC_HEADERS
#define CXX11_FUNC_DELETE = delete
#define CXX11_FUNC_DEFAULT = default
#define CXX11_CONSTEXPR constexpr
#define CXX11_NULLPTR nullptr
#define CXX11_NOEXCEPT noexcept
#define CXX11_STATIC_ASSERT(expr, message) static_assert(expr, message)
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

#endif  // FOEDUS_CXX11_HPP_
