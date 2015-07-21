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

#include <cstdlib>
#include <iostream>

#include "foedus/assert_nd.hpp"
#include "foedus/test_common.hpp"

namespace foedus {
DEFINE_TEST_CASE_PACKAGE(DummyTest, foedus);

void func3() {
  // Disabled usually. Enable only when to test Jenkins.
  // std::abort();
}

void func2() {
  func3();
}

void func1() {
  func2();
}

/**
 * Just to see if Jenkins can pick up aborted testcases.
 * This is a bit trickier than it should be.
 * I'm not sure if I should blame ctest, jenkins, or gtest (or all of them).
 * Related URLs:
 *   https://groups.google.com/forum/#!topic/googletestframework/NK5cAEqsioY
 *   https://code.google.com/p/googletest/issues/detail?id=342
 *   https://code.google.com/p/googletest/issues/detail?id=311
 */
TEST(DummyTest, Abort) {
  func1();
}
TEST(DummyTest, NotAbort) {
}

}  // namespace foedus

TEST_MAIN_CAPTURE_SIGNALS(DummyTest, foedus);
