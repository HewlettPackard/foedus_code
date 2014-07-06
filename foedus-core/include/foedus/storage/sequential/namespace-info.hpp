/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_NAMESPACE_INFO_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::storage::sequential
 * @brief \b Sequential Storage, an append/scan only data structure.
 * @details
 * This data structure is also called \e Heap in database literature, but probably not
 * a good naming (Heap usually means min/max Heap, which is totally different).
 * So, we call this data structure as Sequential Storage.
 *
 * @section SEQ_BASICS Basic Structure and Supported Operations
 * A sequential storage is \b VERY simple thus efficient.
 * The only access pattern is \e append and \e scan, which makes everything simple.
 * Further, the scan does not provide any guarantee in ordering (similar to Heap in DBMS).
 * It just returns records in the order the underlying storage natively stores them.
 *
 * @section SEQ_HIE Page Hierarchy
 * There are only two types of pages;
 * \b root pages (foedus::storage::sequential::SequentialRootPage)
 * and \b data pages (foedus::storage::sequential::SequentialPage).
 *
 * Root pages are merely a set of pointers to \e head pages,
 * which are the beginnings of singly-linked list of data pages.
 *
 * All contents of root pages are stable. They are never dynamically changed.
 * The volatile part of \ref SEQUENTIAL is maintained as a set of singly-linked list pointed
 * directly from the storage object, so there is no root page for it.
 *
 * Data pages contain records that are contiguously placed each other.
 * They form a singly linked list starting from a head page.
 *
 * For each snapshotting, we add a new set of head pages (one for each node) and pointers
 * to them from the root page. As root pages are singly-linked list,
 * we re-write entire root pages for every sequential storage that had some change for
 * each snapshotting (*). Unless there are huge number of head pages, this should be a negligible
 * overhead.
 *
 * (*) We only are placing a few head page pointers at the last root page only, but that means
 * we have to install a new version of the last root page, which requires installing a new
 * version of the second-to-last root page just for changing the next pointer, which requires...
 * But, again, most sequential storage's root page should be just one or two pages.
 * A similar overhead happens to all other storage types, too.
 *
 * @section SEQ_LAYOUT Page Layout
 * Both data pages and root pages form a singly linked list of pages.
 * The next pointer is stored in the header part.
 *
 * @subsection SEQ_LAYOUT_DATA Layout of Data Page (foedus::storage::sequential::SequentialPage)
 * <table>
 *  <tr><th>Fix-Sized HEADER (kHeaderSize bytes)</th></tr>
 *  <tr><td>Record Data part, which grows forward</td></tr>
 *  <tr><th>Unused part</th></tr>
 *  <tr><td>Record Lengthes part, which grows backawrd</td></tr>
 * </table>
 *
 * @subsection SEQ_LAYOUT_ROOT Layout of Root Page (foedus::storage::sequential::SequentialRootPage)
 * <table>
 *  <tr><th>Fix-Sized HEADER (kRootPageHeaderSize bytes)</th></tr>
 *  <tr><td>Pointers to head pages..</td></tr>
 * </table>
 *
 * @section SEQ_INMEM In-memory List
 * As described above, the volatile part of sequential storage is a set of singly-linked list
 * pointed directly from the storage object. The storage object maintains such in-memory list
 * for each epoch. When snapshotting happens, it retires those lists whose epochs are upto
 * the snapshot's "until" epoch.
 *
 * Because we have to efficiently append to the in-memory list, we maintain head and tail pointers
 * to each list. Updating the tail page and replacing tail pointers involve a few atomic operations,
 * but no expensive locks as done in other storages. For more details, see SequentialVolatileList.
 */

/**
 * @defgroup SEQUENTIAL Sequential Storage
 * @ingroup STORAGE
 * @copydoc foedus::storage::sequential
 */

#endif  // FOEDUS_STORAGE_SEQUENTIAL_NAMESPACE_INFO_HPP_
