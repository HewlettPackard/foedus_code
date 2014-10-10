set(test_masstree_basic_individuals
  Create
  CreateAndQuery
  CreateAndInsert
  CreateAndInsertAndRead
  Overwrite
  NextLayer
  CreateAndDrop
  )
add_foedus_test_individual(test_masstree_basic "${test_masstree_basic_individuals}")

add_foedus_test_individual(test_masstree_cursor "Empty;OnePage;OneLayer;TwoLayers")

add_foedus_test_individual(test_masstree_random "InsertManyNormalized;InsertManyNormalizedMt;InsertMany")

add_foedus_test_individual(test_masstree_split "SplitBorder;SplitBorderNormalized;SplitInNextLayer")

set(test_masstree_tpcb_individuals
  SingleThreadedNoContention
  TwoThreadedNoContention
  FourThreadedNoContention
  SingleThreadedContended
  TwoThreadedContended
  FourThreadedContended
  SingleThreadedNoContentionPrimitive
  TwoThreadedNoContentionPrimitive
  FourThreadedNoContentionPrimitive
  SingleThreadedContendedPrimitive
  TwoThreadedContendedPrimitive
  FourThreadedContendedPrimitive
  SingleThreadedNoContentionInc
  TwoThreadedNoContentionInc
  FourThreadedNoContentionInc
  SingleThreadedContendedInc
  TwoThreadedContendedInc
  FourThreadedContendedInc
  )
add_foedus_test_individual(test_masstree_tpcb "${test_masstree_tpcb_individuals}")

set(test_masstree_tpcc_individuals
  FullscanCustomersSecondary
  FullscanNeworders
  FullscanOrders
  FullscanOrdersSecondary
  FullscanOrderlines
  DistrictScanCustomersSecondary
  DistrictScanNeworders
  DistrictScanOrders
  DistrictScanOrdersSecondary
  DistrictScanOrderlines
  )
add_foedus_test_individual(test_masstree_tpcc "${test_masstree_tpcc_individuals}")

add_foedus_test_individual(test_masstree_partitioner "Empty;PartitionBasic;SortBasic")
