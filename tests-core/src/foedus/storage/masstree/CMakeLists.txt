set(test_masstree_basic_individuals
  Create
  CreateAndQuery
  CreateAndInsert
  CreateAndInsertAndRead
  CreateAndInsertLong
  Overwrite
  NextLayer
  CreateAndDrop
  ExpandInsert
  ExpandInsertNextLayer
  ExpandInsertNormalized
  ExpandUpdate
  ExpandUpdateNextLayer
  ExpandUpdateNormalized
  )
add_foedus_test_individual(test_masstree_basic "${test_masstree_basic_individuals}")

add_foedus_test_individual(test_masstree_cursor "Empty;OnePage;OneLayer;TwoLayers")
add_foedus_test_individual(test_masstree_cursor_nrsbug "Nrs;NoNrs")

add_foedus_test_individual(test_masstree_grow_race "Contended")
add_foedus_test_individual(test_masstree_grow_sorted_race "Contended")

add_foedus_test_individual(test_masstree_scan_insert_race "CreateAndInsertAndScan")

add_foedus_test_individual(test_masstree_peek "OneLayer;TwoLayers")

add_foedus_test_individual(test_masstree_random "InsertManyNormalized;InsertManyNormalizedMt;InsertMany")

set(test_masstree_split_individuals
  SplitBorder
  SplitBorderNormalized
  SplitInNextLayer
  SplitInNextLayerWithHint
  SplitIntermediateSequential
  SplitIntermediateSequentialWithHint
  )
add_foedus_test_individual(test_masstree_split "${test_masstree_split_individuals}")

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
add_foedus_test_individual(test_masstree_split_nrsbug "InOrder;Reverse")

add_foedus_test_individual(test_masstree_tpcb "${test_masstree_tpcb_individuals}")

set(test_masstree_tpcc_individuals
  FullscanCustomersSecondary
  FullscanNeworders
  FullscanOrders
  FullscanOrdersSecondary
  FullscanOrderlines
  ParallelLoadCustomersSecondary
  ParallelLoadNeworders
  ParallelLoadOrders
  ParallelLoadOrdersSecondary
  ParallelLoadOrderlines
  DistrictScanCustomersSecondary
  DistrictScanNeworders
  DistrictScanOrders
  DistrictScanOrdersSecondary
  DistrictScanOrderlines
  FullscanCustomersSecondaryWithHint
  FullscanNewordersWithHint
  FullscanOrdersWithHint
  FullscanOrdersSecondaryWithHint
  FullscanOrderlinesWithHint
  ParallelLoadCustomersSecondaryWithHint
  ParallelLoadNewordersWithHint
  ParallelLoadOrdersWithHint
  ParallelLoadOrdersSecondaryWithHint
  ParallelLoadOrderlinesWithHint
  DistrictScanCustomersSecondaryWithHint
  DistrictScanNewordersWithHint
  DistrictScanOrdersWithHint
  DistrictScanOrdersSecondaryWithHint
  DistrictScanOrderlinesWithHint
  )
add_foedus_test_individual(test_masstree_tpcc "${test_masstree_tpcc_individuals}")

add_foedus_test_individual(test_masstree_partitioner "Empty;PartitionBasic;SortBasic")
