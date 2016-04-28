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
#include <gtest/gtest.h>

#include <string>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"
#include "foedus/log/log_manager.hpp"
#include "foedus/snapshot/snapshot_manager.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/xct/xct_manager.hpp"


/**
 * @file test_simple_bringup.cpp
 * Testcases for metadata log handling in restart.
 * No data operation, just metadata logs.
 */
namespace foedus {
namespace restart {
DEFINE_TEST_CASE_PACKAGE(SimpleBringupTest, foedus.restart);

  // Create a masstree, quit while it is still empty, then try to
  // open it again.
  //
  // This code results from attempting to follow the bare example given
  // in the doxygen.  The goal was to just create a storage, see that
  // there were files in the FS, then see that the storage was already
  // created on restart.
void test(bool wait_durable) {
  EngineOptions options = get_tiny_options();
  options.log_.flush_at_shutdown_ = wait_durable;
  {
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      foedus::storage::masstree::MasstreeMetadata mst_meta("my_masstree");
      if (!engine.get_storage_manager()->get_storage("my_masstree")->exists()) {
        foedus::Epoch ep;
        std::cout << "++++++++~~~~~~~~~~make the storage\n\n\n";
        COERCE_ERROR(engine.get_storage_manager()->create_storage(&mst_meta, &ep));
        // Taking a snapshot doesn't help (or hurt).
        // engine.get_snapshot_manager()->trigger_snapshot_immediate(true);
        if (wait_durable) {
          COERCE_ERROR_CODE(engine.get_xct_manager()->wait_for_commit(ep));
        }
      }

      COERCE_ERROR(engine.uninitialize());
    }
  }

  for (uint16_t i = 0; i < 3U; ++i) {
    // Absolutely certain this is a different engine
    Engine engine(options);

    std::cout << "++++++++~~~~~~~~ we do get here\n\n\n";
    COERCE_ERROR(engine.initialize());
    {
      std::cout << "++++++++~~~~~~~~ we never get here\n\n\n";

      UninitializeGuard guard(&engine);
      foedus::storage::masstree::MasstreeMetadata mst_meta("my_masstree");
      // We never get here.
      if (!engine.get_storage_manager()->get_storage("my_masstree")->exists()) {
        // Should never get here.
        foedus::Epoch ep;
        std::cout << "++++++++~~~~~~~~~~makeymakey storage\n\n\n";
        COERCE_ERROR(engine.get_storage_manager()->create_storage(&mst_meta, &ep));
        if (wait_durable) {
          COERCE_ERROR_CODE(engine.get_xct_manager()->wait_for_commit(ep));
          engine.get_snapshot_manager()->trigger_snapshot_immediate(true);
        }
      }

      COERCE_ERROR(engine.uninitialize());
    }
  }
  cleanup_test(options);
}
TEST(SimpleBringupTest, Durable) { test(true); }
TEST(SimpleBringupTest, NonDurable) { test(false); }


}  // namespace restart
}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(SimpleBringupTest, foedus.restart);
