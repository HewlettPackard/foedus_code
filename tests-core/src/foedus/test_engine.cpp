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

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/test_common.hpp"

namespace foedus {
DEFINE_TEST_CASE_PACKAGE(EngineTest, foedus);

TEST(EngineTest, Instantiate) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
}

TEST(EngineTest, Initialize) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  COERCE_ERROR(engine.uninitialize());
  cleanup_test(options);
}

TEST(EngineTest, IdempotentUninitialize) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);
  COERCE_ERROR(engine.initialize());
  COERCE_ERROR(engine.uninitialize());
  COERCE_ERROR(engine.uninitialize());
  cleanup_test(options);
}

TEST(EngineTest, Restart) {
  EngineOptions options = get_tiny_options();
  for (int i = 0; i < 2; ++i) {
    Engine engine(options);
    COERCE_ERROR(engine.initialize());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

// this is a good test to see if initialize/uninitialize pair is complete.
// eg: aren't we incorrectly relying on constructor of some object, rather than initialize()?
// eg: aren't we lazy to skip some uninitialization, which makes next initialize() crash?
// again, don't do anything in constructor! there is no point to do so. do it in initialize().
TEST(EngineTest, RestartManyTimes) {
  EngineOptions options = get_tiny_options();
  Engine engine(options);  // initialize the same engine many times.
  for (int i = 0; i < 2; ++i) {
    COERCE_ERROR(engine.initialize());
    COERCE_ERROR(engine.uninitialize());
  }
  cleanup_test(options);
}

}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(EngineTest, foedus);
