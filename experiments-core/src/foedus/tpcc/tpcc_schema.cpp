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

void TpccStorages::initialize_tables(Engine* engine) {
  storage::StorageManager& st = engine->get_storage_manager();
  customers_static_ = st.get_array("customers_static");
  customers_dynamic_ = st.get_array("customers_dynamic");
  customers_history_ = st.get_array("customers_history");
  customers_secondary_ = st.get_masstree("customers_secondary");
  districts_static_ = st.get_array("districts_static");
  districts_ytd_ = st.get_array("districts_ytd");
  districts_next_oid_ = st.get_array("districts_next_oid");
  histories_ = st.get_sequential("histories");
  neworders_ = st.get_masstree("neworders");
  orders_ = st.get_masstree("orders");
  orders_secondary_ = st.get_masstree("orders_secondary");
  orderlines_ = st.get_masstree("orderlines");
  items_ = st.get_array("items");
  stocks_ = st.get_array("stocks");
  warehouses_static_ = st.get_array("warehouses_static");
  warehouses_ytd_ = st.get_array("warehouses_ytd");
  assert_initialized();
}

}  // namespace tpcc
}  // namespace foedus
