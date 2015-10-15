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
#include "foedus/sssp/sssp_client_chime.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstring>

#include "foedus/assert_nd.hpp"

namespace foedus {
namespace sssp {

void SsspAnalyticChime::start_chime(
  AnalyticEpochPtr analytic_chime_epoch_address,
  AnalyticEpochPtr* target_clean_since_addresses,
  AnalyticEpochPtr* target_clean_upto_addresses,
  uint32_t target_count,
  std::atomic<bool>* query_ended_address) {
  analytic_chime_epoch_address_ = analytic_chime_epoch_address;
  target_clean_since_addresses_.reset(new AnalyticEpochPtr[target_count]);
  target_clean_upto_addresses_.reset(new AnalyticEpochPtr[target_count]);
  target_count_ = target_count;
  query_ended_address_ = query_ended_address;
  ASSERT_ND(!query_ended_address_->load());
  stop_requested_.store(false);

  std::memcpy(
    target_clean_since_addresses_.get(),
    target_clean_since_addresses,
    sizeof(AnalyticEpochPtr) * target_count);

  std::memcpy(
    target_clean_upto_addresses_.get(),
    target_clean_upto_addresses,
    sizeof(AnalyticEpochPtr) * target_count);

  ASSERT_ND(!chime_thread_.joinable());
  std::thread new_thread(&SsspAnalyticChime::chime_handler, this);
  ASSERT_ND(new_thread.joinable());
  chime_thread_ = std::move(new_thread);
  ASSERT_ND(chime_thread_.joinable());
  ASSERT_ND(!new_thread.joinable());
}

void SsspAnalyticChime::stop_chime() {
  if (chime_thread_.joinable()) {
    LOG(INFO) << "waiting for chime thread to end...";
    stop_requested_.store(true);
    chime_thread_.join();
    LOG(INFO) << "confirmed that chime thread has ended.";
  } else {
    LOG(WARNING) << "chime thread has already ended or has not started?!!";
  }
  analytic_chime_epoch_address_ = nullptr;
  target_clean_since_addresses_.reset(nullptr);
  target_clean_upto_addresses_.reset(nullptr);
}


void SsspAnalyticChime::chime_handler() {
  LOG(INFO) << "chime thread has started.";
  while (!stop_requested_.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    // periodically announce a new epoch
    analytic_chime_epoch_address_->fetch_add(1U);
    bool ended = chime_check();
    if (ended) {
      LOG(INFO) << "waiting for stop request...";
      while (!stop_requested_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
        continue;
      }
    }
  }
  LOG(INFO) << "chime thread has ended.";
}

bool SsspAnalyticChime::chime_check() {
  AnalyticEpochPtr* clean_sinces = target_clean_since_addresses_.get();
  AnalyticEpochPtr* clean_uptos = target_clean_upto_addresses_.get();

  AnalyticEpoch max_since = 0;
  AnalyticEpoch min_upto = 0xFFFFFFFFU;
  for (uint32_t i = 0; i < target_count_; ++i) {
    AnalyticEpoch since = clean_sinces[i]->load(std::memory_order_consume);
    AnalyticEpoch upto = clean_uptos[i]->load(std::memory_order_consume);
    if (since == kNullAnalyticEpoch || upto == kNullAnalyticEpoch || since >= upto) {
      // oops, someone is definitely dirty. exit
      return false;
    }
    max_since = std::max<AnalyticEpoch>(max_since, since);
    min_upto = std::min<AnalyticEpoch>(min_upto, upto);
    if (max_since >= min_upto) {
      // oops, no chance to have a full overlap
      return false;
    }
  }

  if (max_since + 1U < min_upto) {
    LOG(INFO) << "Yay, query ended";
    query_ended_address_->store(true);
    return true;
  } else {
    return false;
  }
}

}  // namespace sssp
}  // namespace foedus
