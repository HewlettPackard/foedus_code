add_foedus_test_individual(test_sequential_basic "Create;CreateAndDrop;CreateAndWrite")

set(test_sequential_cursor_individuals
  IteratorRawPage
  Volatile1Node
  Snapshot1Node
  Both1Node
  Volatile2Node
  Snapshot2Node
  Both2Node
  )
add_foedus_test_individual(test_sequential_cursor "${test_sequential_cursor_individuals}")

add_foedus_test_individual(test_sequential_volatile_list "Empty;SingleThread;TwoThreads;FourThreads")

set(test_sequential_tpcb_individuals
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
add_foedus_test_individual(test_sequential_tpcb "${test_sequential_tpcb_individuals}")
