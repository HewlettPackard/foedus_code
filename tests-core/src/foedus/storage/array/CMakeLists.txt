add_foedus_test_individual(test_array_basic "RangeCalculation;RangeCalculation2;Create;CreateAndQuery;CreateAndDrop;CreateAndWrite;CreateAndReadWrite")

add_foedus_test_individual(test_array_partitioner "InitialPartition;Empty;PartitionBasic;SortBasic;SortCompact;SortNoCompact")

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
  SingleThreadedNoContentionInc1S
  TwoThreadedNoContentionInc1S
  FourThreadedNoContentionInc1S
  SingleThreadedContendedInc1S
  TwoThreadedContendedInc1S
  FourThreadedContendedInc1S
  )
add_foedus_test_individual(test_array_tpcb "${test_array_tpcb_individuals}")
