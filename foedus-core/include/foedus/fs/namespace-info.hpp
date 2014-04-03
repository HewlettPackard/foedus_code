/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_FSNAMESPACE_INFO_HPP_
#define FOEDUS_FSNAMESPACE_INFO_HPP_

/**
 * @namespace foedus::fs
 * @brief Filesystem wrapper.
 * @details
 * These classes abstract accesses to filesystems like boost::filesystem and std::filesystem in
 * C++1z(?). We should not directly call POSIX or Windows filesystem APIs in other modules.
 * Instead, all of them should go through this package.
 *
 * @par Why this package exists
 * The best case scenario for us is the standard C++ library provides filesystem abstraction.
 * However, the spec (based on Boost filesystem ver2) didn't get into even C++14, let alone
 * its implmentations. Thus, we can't rely on it at all.
 * Boost filesystem has a few issues, too. First, it is changing and has some issue when C++11
 * is enabled. Furthermore, The filesystem package is NOT a header-only module in boost.
 * We do not want to introduce additional dependencies to the gigantic boost shared library,
 * which might not be available or has versioning issues in some environment.
 * Rather, we just need only a small subset of functionalities in those libraries because
 * our usage of filesystem is quite minimal and simplistic. Hence, we do it ourselves.
 *
 * @par Direct File I/O
 * Another goal to have this package is to abstract Direct I/O (O_DIRECT in linux and
 * FILE_FLAG_NO_BUFFERING in Windows). We need them to bypass filesystem-level caching, but
 * these are not in C++ standards at all and won't be (if it happens, Linus would get a
 * heart attack: https://lkml.org/lkml/2007/1/10/233 ).
 *
 * @par Class/method designs
 * Basically, we clone Boost filesystem's class/method into this package when we find a need for
 * some filesystem API access in our code. Due to our specialized needs, we might simplify the API.
 * Our goal is not to duplicate all Boost filesystem classes. Just add what we need.
 */

/**
 * @defgroup FILESYSTEM Filesystem wrapper
 * @copydoc foedus::fs
 * @copydetails foedus::fs
 */

#endif  // FOEDUS_FSNAMESPACE_INFO_HPP_
