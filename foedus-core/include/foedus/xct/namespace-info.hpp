/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
 * class MyTask : public foedus::threadImpersonateTask {
 *  public:
 *     MyTask() {}
 *     foedus::ErrorStack run(foedus::threadThread* context) {
 *         foedus::Engine *engine = context->get_engine();
 *         foedus::xctXctManager& xct_manager = engine->get_xct_manager();
 *         CHECK_ERROR(xct_manager.begin_xct(context, foedus::xct::SERIALIZABLE));
 *         ... // read/modify data. See storage module's document for examples.
 *         foedus::xct::Epoch commit_epoch;
 *         CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
 *         CHECK_ERROR_CODE(xct_manager.wait_for_commit(commit_epoch));
 *         return foedus::RET_OK;
 *     }
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
 * foedus::xct::Epoch highest_commit_epoch;
 * for (int i = 0; i < 1000; ++i) {
 *   CHECK_ERROR(xct_manager.begin_xct(context, foedus::xct::SERIALIZABLE));
 *   ... // read/modify data. See storage module's document for examples.
 *   foedus::xct::Epoch commit_epoch;
 *   CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
 *   if (highest_commit_epoch < commit_epoch) {
 *     highest_commit_epoch = commit_epoch;
 *   }
 * }
 * CHECK_ERROR_CODE(xct_manager.wait_for_commit(highest_commit_epoch));
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
 * foedus::xct::XctManager maintains two global foedus::xct::Epoch; \e current Epoch and
 * \e durable Epoch. foedus::xct::XctManager keeps advancing current epoch periodically.
 * The log module    advances durable epoch when it confirms that all log entries up to the epoch
 * becomes durable and also that the log module durably writes a savepoint ( \ref SAVEPOINT ) file.
 *
 * @section ISOLATION Isolation Levels
 * See foedus::xct::IsolationLevel
 *
 * @section READONLY Read-Only Transactions
 * bluh
 *
 * @section REF References
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
