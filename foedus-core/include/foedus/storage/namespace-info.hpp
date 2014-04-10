/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_NAMESPACE_INFO_HPP_
#define FOEDUS_STORAGE_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::storage
 * @brief \b Storage \b Manager, which implements a couple of key/value stores.
 * @details
 * This package contains core classes that store key/value pairs.
 * This layer has no idea about what is stored, thus no notion of columns either.
 *
 * @section TYPES Types of Storage
 * So far we provide four types of storages; \ref ARRAY, \ref HASH, \ref HEAP, and \ref MASSTREE.
 * Choose the type of storage based on the data to store and access patterns.
 *  \li \ref ARRAY is extremely simple, fast, and compact (doesn't even store keys), but it only
 * works for arrays that are fix-sized, dense, and regular.
 *  \li \ref HASH is also simple, fast, and yet general except it can't process range accesses.
 *  \li \ref HEAP is very simple and fast, but it can do only appends and full scans.
 *  \li \ref MASSTREE is the most general, addressing all the issues above. But, it's not as
 * simple/fast as others (though optimized as much as possible for many-cores).
 *
 * @par General Recommendation
 * In general, you should use \ref HASH or \ref MASSTREE for most tables/indexes.
 * If the access pattern contains range accesses, equality on prefix, and non-equality, then
 * pick \ref HASH. Otherwise, \ref MASSTREE.
 * Use \ref ARRAY and \ref HEAP where they shine; when you literally needs
 * arrays and append-only data.
 */

/**
 * @defgroup STORAGE Storage Manager
 * @ingroup COMPONENTS
 * @copydoc foedus::storage
 */

#endif  // FOEDUS_STORAGE_NAMESPACE_INFO_HPP_
