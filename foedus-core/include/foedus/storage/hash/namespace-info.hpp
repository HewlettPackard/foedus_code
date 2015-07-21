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
#ifndef FOEDUS_STORAGE_HASH_NAMESPACE_INFO_HPP_
#define FOEDUS_STORAGE_HASH_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::storage::hash
 * @brief \b Hashtable Storage, a concurrent hashtable.
 * @details
 * What we have here is a quite simple hash storage. Allthemore simple, more robust and scalable.
 *
 * @section HASH_PAGES Hash Interemediate page and Data page
 * The hash storage has two types of pages: HashIntermediatePage and HashDataPage.
 * HashIntermediatePage, starting from the root page, points down to other intermediate pages,
 * and the level-0 intermediate pages point to HashDataPage.
 * HashDataPage has a next-pointer.
 * One entry (pointer) in a level-0 intermediate page corresponds to a hash \e bin.
 * Usually one hash bin corresponds to a very small number of data pages, zero, one or rarely two.
 *
 * @ref foedus::storage::hash::HashIntermediatePage
 * @ref foedus::storage::hash::HashDataPage
 *
 * @section HASH_LINK Singly-linked data pages, hopefully rarely used
 * When a hash bin receives many records or very long key or values,
 * a data page contains a link to \e next page that forms a singly-linked list of pages.
 * Although this is a dual page pointer, we use only one of them.
 * Snapshot hash pages of course point to only snapshot hash pages.
 * Volatile hash pages, to simplify, also point to only volatile hash pages.
 *
 * In other words, when we make a volatile version of any page in a hash bin, we
 * instantiate volatile pages for all pages in the hash bin. This might sound costly,
 * but there should be only 1 or only occasionaly a few pages in the bin,
 * assuming 1) bin-bits that is large enough, and 2) good hash function.
 *
 * @section HASH_LOCKS Locking Protocol
 * Intermediate pages are completely lock-free. Data pages do have per-page and per-tuple locks.
 * Hopefully most locks are per-tuple, meaning just taking the TID of the tuple
 * as read-set or write-set. However, there are a few cases the thread must take
 * an exclusive lock in page-header.
 *
 * \li Inserting a new tuple. A system-transaction takes a page-lock, places a logically-deleted
 * tuple with the hash/key, modifies the page header, releases the lock.
 * \li Installing a next-page pointer. A system-transaction creates an empty next-page,
 * takes a page-lock, installs the pointer to the new page, modifies the page header,
 * releases the lock.
 * \li Migrating a tuple for expansion. Also a system transaction. Detailed in next section.
 * \li Miss-search. A user-transaction adds the page header of the tail page to PageVersionSet
 * and verifies it at pre-commit.
 *
 * All of the above are quite similar to masstree package's insertion/splits/miss-search.
 * Like masstree package, the full-hash/key of a physical tuple is immutable once installed.
 * We only use logical deletion flags and "moved" bits to allow record expansion.
 * Also like masstree, other operations proceed without locks or in many cases even atomic
 * operations, just a few memory barriers. For example, deletion just puts a logical deletion
 * flag to the record without page-lock. Reading is completely atomics-free as far as you find
 * the exact record (miss-search is a bit more complicated as above).
 *
 * @section HASH_FLAGS Flags in TID of Hash storages
 * To implement the above protocol, the \e moved bit in TID has a bit different meaning from
 * masstree package. It means that the corresponding record for the full-hash/key is somewhere
 * later in the list.
 * When we need to expand a record space that is not at the end, we insert a new physical tuple
 * for the record with large space, then put the "moved" bit in the original physical record.
 * If threads see the flag while processing, it skips such a record. If pre-commit sees the flag
 * because of concurrent migration, it tracks the moved record just like Master-Tree protocol.
 * The idea is almost the same, but here the tracking is much simpler. Just follow the
 * linked list.
 *
 * @par History note, or a tombstone
 * We initially considered a bit more fancy hash storages (namely Cuckoo Hashing), but
 * we didn't see enough benefits to justify its limitations (eg, how to structure snapshot pages
 * if a tuple might be placed in two bins!). The only case it is useful is when the hash
 * bins are really close to full, but we'll just have larger hash storages to avoid such a case.
 * On huge NVRAM (whose density will also quickly grow), 100% fill-factor doesn't buy us anything
 * significant over 50%. Hence discarded that plan. A sad war story, but ain't gonna win all wars.
 * Keep it simple stupid, men.
 */

/**
 * @defgroup HASH Hashtable Storage
 * @ingroup STORAGE
 * @copydoc foedus::storage::hash
 */

#endif  // FOEDUS_STORAGE_HASH_NAMESPACE_INFO_HPP_
