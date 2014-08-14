/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/tpcc/tpcc_schema.hpp"

#include <cstring>

#include "foedus/storage/array/array_storage.hpp"

namespace foedus {
namespace tpcc {

TpccStorages::TpccStorages() {
  customers_static_ = nullptr;
  customers_dynamic_ = nullptr;
  customers_history_ = nullptr;
  customers_secondary_ = nullptr;
  districts_static_ = nullptr;
  districts_ytd_ = nullptr;
  districts_next_oid_ = nullptr;
  histories_ = nullptr;
  neworders_ = nullptr;
  orders_ = nullptr;
  orders_secondary_ = nullptr;
  orderlines_ = nullptr;
  items_ = nullptr;
  stocks_ = nullptr;
  warehouses_static_ = nullptr;
  warehouses_ytd_ = nullptr;
}

TpccStorages::TpccStorages(const TpccStorages& other) {
  operator=(other);
}


TpccStorages& TpccStorages::operator=(const TpccStorages& other) {
  // copy storages.
  customers_static_ = other.customers_static_;
  customers_dynamic_ = other.customers_dynamic_;
  customers_history_ = other.customers_history_;
  customers_secondary_ = other.customers_secondary_;
  districts_static_ = other.districts_static_;
  districts_ytd_ = other.districts_ytd_;
  districts_next_oid_ = other.districts_next_oid_;
  histories_ = other.histories_;
  neworders_ = other.neworders_;
  orders_ = other.orders_;
  orders_secondary_ = other.orders_secondary_;
  orderlines_ = other.orderlines_;
  items_ = other.items_;
  stocks_ = other.stocks_;
  warehouses_static_ = other.warehouses_static_;
  warehouses_ytd_ = other.warehouses_ytd_;

  // but, reconstruct local caches.
  customers_static_cache_ = storage::array::ArrayStorageCache(customers_static_);
  customers_dynamic_cache_ = storage::array::ArrayStorageCache(customers_dynamic_);
  customers_history_cache_ = storage::array::ArrayStorageCache(customers_history_);
  districts_static_cache_ = storage::array::ArrayStorageCache(districts_static_);
  districts_ytd_cache_ = storage::array::ArrayStorageCache(districts_ytd_);
  districts_next_oid_cache_ = storage::array::ArrayStorageCache(districts_next_oid_);
  items_cache_ = storage::array::ArrayStorageCache(items_);
  stocks_cache_ = storage::array::ArrayStorageCache(stocks_);
  warehouses_static_cache_ = storage::array::ArrayStorageCache(warehouses_static_);
  warehouses_ytd_cache_ = storage::array::ArrayStorageCache(warehouses_ytd_);
  return *this;
}


}  // namespace tpcc
}  // namespace foedus
