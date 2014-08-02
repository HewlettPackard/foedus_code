/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
/**
 * @file foedus/tpcc/tpcc.cpp
 * @brief TPC-C experiment
 * @author kimurhid
 * @date 2014/08/01
 * @details
 * This implements the popular TPC-C benchmark.
 *
 * @section ENVIRONMENTS Environments
 *
 * @section OTHER Other notes
 *
 * @section RESULTS Latest Results
 */
#include "foedus/tpcc/tpcc.hpp"

#include "foedus/tpcc/tpcc_driver.hpp"

int main(int argc, char **argv) {
  return foedus::tpcc::driver_main(argc, argv);
}
