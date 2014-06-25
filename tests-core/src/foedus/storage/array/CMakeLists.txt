add_foedus_test_individual(test_array_basic "CreateAndQuery;CreateAndDrop;CreateAndWrite;CreateAndReadWrite")

add_foedus_test_individual(test_array_partitioner "Clone;PartitionBasic;SortBasic;SortCompact;SortNoCompact")

set(test_array_tpcb_individuals
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
add_foedus_test_individual(test_array_tpcb "${test_array_tpcb_individuals}")
