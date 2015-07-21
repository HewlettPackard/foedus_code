/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
#ifndef FOEDUS_FS_NAMESPACE_INFO_HPP_
#define FOEDUS_FS_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::fs
 * @brief \b Filesystem wrapper, an analogue of boost::filesystem.
 * @details
 * These methods abstract accesses to filesystems like boost::filesystem and std::filesystem in
 * C++1z(?). We should not directly call POSIX or Windows filesystem APIs in other modules.
 * Instead, all of them should go through this package.
 *
 * @par Why this package exists
 * The best case scenario for us is the standard C++ library provides filesystem abstraction.
 * However, the spec (based on Boost filesystem ver2) didn't get into even C++14, let alone
 * its implmentations. Thus, we can't rely on it at all.
 * Boost filesystem has a few issues, too. First, it is changing and has some issue when C++11
 * is enabled. Furthermore, the filesystem package is NOT a header-only module in boost.
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
 * @ingroup IDIOMS
 * @copydoc foedus::fs
 */

#endif  // FOEDUS_FS_NAMESPACE_INFO_HPP_
