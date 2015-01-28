/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_schema.hpp"

#include <cstring>

#include "foedus/engine.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_manager.hpp"

namespace foedus {
namespace tpcc {

TpccStorages::TpccStorages() {
  std::memset(this, 0, sizeof(TpccStorages));
}
void TpccStorages::assert_initialized() {
  ASSERT_ND(customers_static_.exists());
  ASSERT_ND(customers_dynamic_.exists());
  ASSERT_ND(customers_history_.exists());
  ASSERT_ND(customers_secondary_.exists());
  ASSERT_ND(districts_static_.exists());
  ASSERT_ND(districts_ytd_.exists());
  ASSERT_ND(districts_next_oid_.exists());
  ASSERT_ND(histories_.exists());
  ASSERT_ND(neworders_.exists());
  ASSERT_ND(orders_.exists());
  ASSERT_ND(orders_secondary_.exists());
  ASSERT_ND(orderlines_.exists());
  ASSERT_ND(items_.exists());
  ASSERT_ND(stocks_.exists());
  ASSERT_ND(warehouses_static_.exists());
  ASSERT_ND(warehouses_ytd_.exists());

  ASSERT_ND(customers_static_.get_name().str() == "customers_static");
  ASSERT_ND(customers_dynamic_.get_name().str() == "customers_dynamic");
  ASSERT_ND(customers_history_.get_name().str() == "customers_history");
  ASSERT_ND(customers_secondary_.get_name().str() == "customers_secondary");
  ASSERT_ND(districts_static_.get_name().str() == "districts_static");
  ASSERT_ND(districts_ytd_.get_name().str() == "districts_ytd");
  ASSERT_ND(districts_next_oid_.get_name().str() == "districts_next_oid");
  ASSERT_ND(histories_.get_name().str() == "histories");
  ASSERT_ND(neworders_.get_name().str() == "neworders");
  ASSERT_ND(orders_.get_name().str() == "orders");
  ASSERT_ND(orders_secondary_.get_name().str() == "orders_secondary");
  ASSERT_ND(orderlines_.get_name().str() == "orderlines");
  ASSERT_ND(items_.get_name().str() == "items");
  ASSERT_ND(stocks_.get_name().str() == "stocks");
  ASSERT_ND(warehouses_static_.get_name().str() == "warehouses_static");
  ASSERT_ND(warehouses_ytd_.get_name().str() == "warehouses_ytd");
}

bool TpccStorages::has_snapshot_versions() {
#ifndef OLAP_MODE
  if (customers_static_.get_metadata()->root_snapshot_page_id_ == 0) {
    return false;
  } else if (customers_static_.get_metadata()->root_snapshot_page_id_ == 0) {
    return false;
  } else if (customers_secondary_.get_metadata()->root_snapshot_page_id_ == 0) {
    return false;
  } else if (districts_static_.get_metadata()->root_snapshot_page_id_ == 0) {
    return false;
  } else if (items_.get_metadata()->root_snapshot_page_id_ == 0) {
    return false;
  } else if (warehouses_static_.get_metadata()->root_snapshot_page_id_ == 0) {
    return false;
  }
#endif  // OLAP_MODE
  return true;
}

void TpccStorages::initialize_tables(Engine* engine) {
  storage::StorageManager* st = engine->get_storage_manager();
  customers_static_ = st->get_array("customers_static");
  customers_dynamic_ = st->get_array("customers_dynamic");
  customers_history_ = st->get_array("customers_history");
  customers_secondary_ = st->get_masstree("customers_secondary");
  districts_static_ = st->get_array("districts_static");
  districts_ytd_ = st->get_array("districts_ytd");
  districts_next_oid_ = st->get_array("districts_next_oid");
  histories_ = st->get_sequential("histories");
  neworders_ = st->get_masstree("neworders");
  orders_ = st->get_masstree("orders");
  orders_secondary_ = st->get_masstree("orders_secondary");
  orderlines_ = st->get_masstree("orderlines");
  items_ = st->get_array("items");
  stocks_ = st->get_array("stocks");
  warehouses_static_ = st->get_array("warehouses_static");
  warehouses_ytd_ = st->get_array("warehouses_ytd");
  assert_initialized();
}

}  // namespace tpcc
}  // namespace foedus
