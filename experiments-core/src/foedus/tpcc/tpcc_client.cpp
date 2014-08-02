/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_client.hpp"

#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/numa_node_memory.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace tpcc {
ErrorStack TpccClientTask::run(thread::Thread* context) {
  context_ = context;
  engine_ = context->get_engine();
  CHECK_ERROR(
    context->get_thread_memory()->get_node_memory()->allocate_numa_memory(
      kRandomCount * sizeof(uint32_t), &numbers_));
  rnd_.fill_memory(&numbers_);
  // const uint32_t *randoms = reinterpret_cast<const uint32_t*>(numbers_.get_block());

  processed_ = 0;
  xct::XctManager& xct_manager = context->get_engine()->get_xct_manager();

  start_rendezvous_->wait();
  while (!stop_requrested_) {
    // currently we change wid for each transaction.
    Wid wid = rnd_.uniform_within(0, kWarehouses - 1);
    uint16_t transaction_type = rnd_.uniform_within(1, 100);
    // remember the random seed to repeat the same transaction on abort/retry.
    uint64_t rnd_seed = rnd_.get_current_seed();

    // abort-retry loop
    while (!stop_requrested_) {
      rnd_.set_current_seed(rnd_seed);
      ErrorCode ret;
      if (transaction_type <= kXctNewOrderPercent) {
        ret = do_neworder(wid);
      } else if (transaction_type <= kXctPaymentPercent) {
        ret = do_payment(wid);
      } else if (transaction_type <= kXctOrderStatusPercent) {
        ret = do_order_status(wid);
      } else if (transaction_type <= kXctDelieveryPercent) {
        ret = do_delivery(wid);
      } else {
        ret = do_stock_level(wid);
      }

      if (ret == kErrorCodeXctUserAbort) {
        // Fine. This is as defined in the spec.
        increment_user_requested_aborts();
        break;
      } else if (ret == kErrorCodeXctRaceAbort) {
        // early abort for some reason.
        // this one must be retried just like usual race abort in precommit.
        WRAP_ERROR_CODE(xct_manager.abort_xct(context));
        continue;
      } else if (ret != kErrorCodeOk) {
        return ERROR_STACK(ret);  // unexpected error
      }

      Epoch ep;
      ErrorCode commit_ret = xct_manager.precommit_xct(context, &ep);
      if (commit_ret == kErrorCodeXctRaceAbort) {
        WRAP_ERROR_CODE(xct_manager.abort_xct(context));
        continue;
      } else if (commit_ret != kErrorCodeOk) {
        return ERROR_STACK(ret);  // unexpected error
      } else {
        // okay, committed
        break;
      }
    }

    ++processed_;
  }

  return kRetOk;
}


ErrorCode TpccClientTask::lookup_customer_by_id_or_name(Wid wid, Did did, Cid *cid) {
  // 60% by name, 40% by ID
  bool by_name = rnd_.uniform_within(1, 100) <= 60;
  if (by_name) {
    char lastname[17];
    generate_lastname(rnd_.non_uniform_within(255, 0, kLnames - 1), lastname);
    CHECK_ERROR_CODE(lookup_customer_by_name(wid, did, lastname, cid));
  } else {
    *cid = rnd_.non_uniform_within(1023, 0, kCustomers - 1);
  }
  return kErrorCodeOk;
}

ErrorCode TpccClientTask::lookup_customer_by_name(Wid wid, Did did, const char* lname, Cid *cid) {
  char low_be[sizeof(Wid) + sizeof(Did) + 17];
  std::memcpy(low_be + sizeof(Wid) + sizeof(Did), lname, 17);

  char high_be[sizeof(Wid) + sizeof(Did) + 17];
  std::memcpy(high_be, low_be, sizeof(high_be));

  // this increment never overflows because it's NULL character (see tpcc_schema.hpp).
  ASSERT_ND(high_be[sizeof(high_be) - 1] == '\0');
  ++high_be[sizeof(high_be) - 1];

  storage::masstree::MasstreeCursor cursor(engine_, storages_.customers_secondary_, context_);
  CHECK_ERROR_CODE(cursor.open(low_be, sizeof(low_be), high_be, sizeof(high_be)));

  // on average only 3. surely won't be more than this number
  const uint8_t kMaxCidsPerLname = 16;
  uint8_t cid_count = 0;
  Cid cids[kMaxCidsPerLname];
  const uint16_t offset = sizeof(Wid) + sizeof(Did) + 34;
  while (cursor.is_valid_record()) {
    const char* key_be = cursor.get_key();
    ASSERT_ND(assorted::betoh<Wid>(*reinterpret_cast<const Wid*>(key_be) == wid));
    ASSERT_ND(assorted::betoh<Did>(*reinterpret_cast<const Did*>(key_be + sizeof(Wid)) == did));
    ASSERT_ND(std::memcmp(key_be, low_be, sizeof(low_be)) == 0);
    Cid cid = assorted::betoh<Cid>(*reinterpret_cast<const Cid*>(key_be + offset));
    if (UNLIKELY(cid_count >= kMaxCidsPerLname)) {
      return kErrorCodeInvalidParameter;
    }
    cids[cid_count] = cid;
    ++cid_count;
    CHECK_ERROR_CODE(cursor.next());
  }

  if (UNLIKELY(cid_count == 0)) {
    return kErrorCodeStrKeyNotFound;
  }

  // take midpoint
  *cid = cids[cid_count / 2];
  return kErrorCodeOk;
}

}  // namespace tpcc
}  // namespace foedus
