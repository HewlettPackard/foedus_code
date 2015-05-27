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
#ifndef FOEDUS_XCT_NAMESPACE_INFO_HPP_
#define FOEDUS_XCT_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::xct
 * @brief \b Transaction \b Manager, which provides APIs to begin/commit/abort transactions.
 * @details
 * This package is the implementation of the commit protocol, the gut of concurrency control.
 *
 * @section PRIMER Get Started
 * First thing first. Here's a minimal example to start \e one transaction in the engine.
 * @code{.cpp}
 * // Example to start and commit one transaction
 * foedus::ErrorStack run_my_task(foedus::thread::Thread* context, ...) {
 *   foedus::Engine *engine = context->get_engine();
 *   foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
 *   WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
 *   ... // read/modify data. See storage module's document for examples.
 *   foedus::Epoch commit_epoch;
 *   WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
 *   WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
 *   return foedus::kRetOk;
 * }
 * @endcode
 *
 * Notice the \e wait_for_commit(commit_epoch) call. Without invoking the method, you should \b not
 * consider that your transactions are committed. That's why the name of the method invoked above
 * is "precommit_xct".
 *
 * Here's a minimal example to start \e several transactions and commit them together,
 * or \e group-commit, which is the primary usecase our engine is optimized for.
 * @code{.cpp}
 * // Example to start and commit several transactions
 * foedus::Epoch highest_commit_epoch;
 * for (int i = 0; i < 1000; ++i) {
 *   WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
 *   ... // read/modify data. See storage module's document for examples.
 *   foedus::Epoch commit_epoch;
 *   WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
 *   highest_commit_epoch.store_max(commit_epoch);
 * }
 * CHECK_ERROR(xct_manager->wait_for_commit(highest_commit_epoch));
 * @endcode
 * In this case, we invoke wait_for_commit() for the largest commit epoch just once at the end.
 * This dramatically improves the throughput at the cost of latency of individual transactions.
 *
 * @section OCC Optimistic Concurrency Control
 * Our commit protocol is based on [TU13], except that we have handling of non-volatile pages.
 * [TU13]'s commit protocol completely avoids writes to shared memory by read operations.
 * [LARSON11] is also an optimistic concurrency control, but it still has "read-lock" bits which
 * has to be written by read operations. In many-core NUMA environment, this might cause
 * scalability issues, thus we employ [TU13]'s approach.
 *
 * @section EPOCH Epoch-based commit protocol
 * The engine maintains two global foedus::Epoch; \e current Epoch and
 * \e durable Epoch. foedus::xct::XctManager keeps advancing current epoch periodically
 * while the log module advances durable epoch when it confirms that all log entries up to the epoch
 * becomes durable and also that the log module durably writes a savepoint ( \ref SAVEPOINT ) file.
 *
 * @subsection EPOCH_CHIME Epoch Chime
 * \e Epoch \e Chime advances the current global epoch when a configured interval elapses or the
 * user explicitly requests it. The chime checks whether it can safely advance an epoch so that
 * the following invariant always holds.
 *  \li A newly started transaction will always commit with current global epoch or larger.
 *  \li All running transactions will always commit with at least current global epoch - 1,
 * called \e grace-period epoch, or larger.
 *
 * In many cases, the invariants are trivially achieved. However, there are a few tricky cases.
 *  \li There is a long running transaction that already acquired a commit-epoch but not yet
 * exit from the pre-commit stage.
 *  \li There is a worker thread that has been idle for a while.
 *
 * Whenever the chime advances the epoch, we have to safely detect whether there is any transaction
 * that might violate the invariant \b without causing expensive synchronization.
 * This is done via the in-commit epoch guard. For more details, see the following class.
 * @see foedus::xct::InCommitEpochGuard
 *
 * @section ISOLATION Isolation Levels
 * See foedus::xct::IsolationLevel
 *
 * @section READONLY Read-Only Transactions
 * bluh
 *
 * @section XCT_REF References
 * \li [LARSON11] Perake Larson, Spyros Blanas, Cristian Diaconu, Craig Freedman, Jignesh M. Patel,
 * and Mike Zwilling. "High-Performance Concurrency Control Mechanisms for Main-Memory Databases."
 * VLDB, 2011.
 *
 * \li [TU13] Stephen Tu, Wenting Zheng, Eddie Kohler, Barbara Liskov, and Samuel Madden.
 *   "Speedy transactions in multicore in-memory databases.", SOSP, 2013.
 */

/**
 * @defgroup XCT Transaction Manager
 * @ingroup COMPONENTS
 * @copydoc foedus::xct
 */

#endif  // FOEDUS_XCT_NAMESPACE_INFO_HPP_
