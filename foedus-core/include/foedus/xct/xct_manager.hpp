/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_MANAGER_HPP_
#define FOEDUS_XCT_XCT_MANAGER_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/xct/fwd.hpp>
#include <foedus/xct/xct_id.hpp>
#include <foedus/thread/fwd.hpp>
namespace foedus {
namespace xct {
/**
 * @brief Xct Manager class that provides API to begin/abort/commit transaction.
 * @ingroup XCT
 * @details
 * Client programs should first call begin_xct(), then either call abort_xct() or precommit_xct().
 * In the latter case, the client programs should either keep running other transactions without
 * returning the results to users, or call wait_for_commit() to immediately return the results.
 */
class XctManager CXX11_FINAL : public virtual Initializable {
 public:
  explicit XctManager(Engine* engine);
  ~XctManager();

  // Disable default constructors
  XctManager() CXX11_FUNC_DELETE;
  XctManager(const XctManager&) CXX11_FUNC_DELETE;
  XctManager& operator=(const XctManager&) CXX11_FUNC_DELETE;

  ErrorStack  initialize() CXX11_OVERRIDE;
  bool        is_initialized() const CXX11_OVERRIDE;
  ErrorStack  uninitialize() CXX11_OVERRIDE;

  /**
   * @brief Returns the current global epoch.
   */
  Epoch       get_current_global_epoch() const;
  Epoch       get_current_global_epoch_weak() const;

  /**
   * @brief Requests to advance the current global epoch as soon as possible and blocks until
   * it actually does.
   * @details
   * This method is used when some thread immediately needs the next epoch for some reason,
   * eg transactional threads ran out of ordinal (per-thread/epoch identifier) in the epoch.
   */
  void        advance_current_global_epoch();

  /**
   * @brief Begins a new transaction on the thread.
   * @param[in,out] context Thread context
   * @param[in] isolation_level concurrency isolation level of the new transaction
   * @pre context->is_running_xct() == false
   */
  ErrorStack  begin_xct(thread::Thread* context, IsolationLevel isolation_level);

  /**
   * @brief Begins a special transaction for modifying schema on the thread.
   * @param[in,out] context Thread context
   * @pre context->is_running_xct() == false
   * @details
   * A schema transaction is a special transaction that issues only
   * storage create/drop/alter etc operations. It cannot issue data modification operations,
   * and sometimes has a strong restriction, such as having to execute in its own epoch.
   * Schema transactions are the only transactions that can modify schema.
   */
  ErrorStack  begin_schema_xct(thread::Thread* context);

  /**
   * @brief Prepares the currently running transaction on the thread for commit.
   * @pre context->is_running_xct() == true
   * @param[in,out] context Thread context
   * @param[out] commit_epoch When successfully prepared, this value indicates the commit
   * epoch of the prepared transaction. When the global epoch reaches this value, the
   * transaction is deemed as committed.
   * @details
   * As the name of this method implies, this method is \b NOT a commit yet.
   * The transaction is deemed as committed only when the durable global epoch reaches
   * the returned commit epoch.
   * This method merely \e prepares this transaction to be committed so that the caller
   * can \b choose either moving on to other transactions in the meantime or to immediately
   * wait for the commit using wait_for_commit().
   * @see wait_for_commit()
   */
  ErrorStack  precommit_xct(thread::Thread* context, Epoch *commit_epoch);


  /**
   * @copydoc foedus::log::LogManager::wait_until_durable()
   */
  ErrorStack  wait_for_commit(Epoch commit_epoch, int64_t wait_microseconds = -1);

  /**
   * @brief Aborts the currently running transaction on the thread.
   * @param[in,out] context Thread context
   * @pre context->is_running_xct() == true
   */
  ErrorStack  abort_xct(thread::Thread* context);

 private:
  XctManagerPimpl *pimpl_;
};
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_MANAGER_HPP_
