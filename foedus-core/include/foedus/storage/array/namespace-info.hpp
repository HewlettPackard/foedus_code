/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_NAMESPACE_INFO_HPP_
#define FOEDUS_STORAGE_ARRAY_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::storage::array
 * @brief \b Array Storage, a dense and regular array.
 * @details
 * @section BASICS Basic Structure
 * Our array-storage is an extremely simple data structure at the cost
 * of limited applicability (fix-sized, regular, dense arrays).
 * The page layout has little per-page information that is dynamic, meaning that
 * most data never changes after the array storage is created.
 * Thus, little synchronization hassles.
 *
 * @section OPS Supported Operations
 * Array storage allows very few data operations.
 * \li \b Reads a record or a range of records.
 * \li \b Overwrites a record.
 * \li Reads a \b range of records in batched fashion. (Not implemented so far, just loop over it.
 * This might be useful for a big array of really small records to avoid function call overheads.)
 *
 * In other words, the following operations are \b NOT supported.
 * \li \b Inserts or \b Deletes a record because all records always exist in an array storage
 * (dense array). Client programs can achieve the same thing by storing one-bit in
 * payload as a deletion-flag and overwrites it.
 * \li \b Resize individual records or the entire array. All records are fix-sized and
 * pre-allocated when the array storage is initialized.
 *
 * If these limitations do not cause an issue, array storage is a best choice as it's extremely
 * efficient and scalable thanks to the simplicity.
 *
 * @section HIE Page Hierarchy
 * Interior and leaf. bluh
 *
 * @section LAYOUT Page Layout
 * @par Header and Data
 * <table>
 *  <tr><th>Fix-Sized HEADER (HEADER_SIZE bytes)</th><th>DATA</th></tr>
 * </table>
 *
 * @par Interior Node
 * <table>
 *  <tr><th>InteriorRecord</th><th>InteriorRecord</th><th>...</th></tr>
 * </table>
 * See foedus::storage::array::ArrayPage#InteriorRecord.
 *
 * @par Leaf Node
 * <table>
 *  <tr><th>LeafRecord</th><th>LeafRecord</th><th>...</th></tr>
 * </table>
 * See foedus::storage::array::ArrayPage#LeafRecord.
 *
 * @section VER Concurrency Control
 * We do a bit special concurrency control for this storage type.
 * Because everything is pre-allocated and no split/physical-delete/merge whatsoever,
 * we can do the ModCount concurrency control \b per \b Record, not per page.
 * We store ModCount for each record, instead stores no ModCount in page header.
 * Thus, individual transactions that update records in same page have no contention except they
 * update the exact same record. We even don't track node-set; only record-set is checked at commit.
 *
 * This simplifies and increases concurrency for most data accesses, but we have to be careful when
 * the page eviction thread drops the volatile page.
 * We need to know the \b largest Epoch of records in the page at the point, but concurrent
 * transactions might be modifying records at the same time. Thus, we do two-path optimistic
 * concurrency control similar to our commit protocol for \ref MASSTREE.
 *  \li The eviction thread takes a look at all Epoch in the page, remembering the largest one.
 *  \li Fence
 *  \li Then, it puts a mark on the pointer from the parent page to the evicted page to announce
 * that the volatile page is about to be evicted.
 *  \li Fence
 *  \li Finally, it takes a look at all Epoch in the page again and completes the eviction
 * only if it sees the same largest Epoch. If not, gives up (because the volatile page is
 * now newer, no chance to drop it with the latest snapshot version).
 *
 * @section REF References
 * \li [SCIDB] Cudre-Mauroux, P. and Kimura, H. and Lim, K.-T. and Rogers, J. and Simakov, R. and
 *   Soroush, E. and Velikhov, P. and Wang, D. L. and Balazinska, M. and Becla, J. and DeWitt, D.
 *   and Heath, B. and Maier, D. and Madden, S. and Patel, J. and Stonebraker, M. and Zdonik, S.
 *   "A demonstration of SciDB: a science-oriented DBMS.", VLDB, 2009.
 * @todo IMPLEMENT
 */

/**
 * @defgroup ARRAY Array Storage
 * @ingroup STORAGE
 * @copydoc foedus::storage::array
 */

#endif  // FOEDUS_STORAGE_ARRAY_NAMESPACE_INFO_HPP_
