set(test_hash_basic_individuals
  Create
  CreateAndQuery
  CreateAndInsert
  CreateAndInsertAndRead
  Overwrite
  CreateAndDrop
  ExpandInsert
#  ExpandUpdate
  )
add_foedus_test_individual(test_hash_basic "${test_hash_basic_individuals}")

set(test_hash_hashinate_individuals
  Primitives
  SequentialCollisions64
  RandomCollisions64
  SequentialCollisions32
  RandomCollisions32
  SequentialBloomFilter64
  RandomBloomFilter64
  SequentialBloomFilter32
  RandomBloomFilter32
  )
add_foedus_test_individual(test_hash_hashinate "${test_hash_hashinate_individuals}")

add_foedus_test_individual(test_hash_partitioner "Empty;EmptyMany;PartitionBasic;PartitionBasicMany;SortBasic")

set(test_hash_tpcb_individuals
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
add_foedus_test_individual(test_hash_tpcb "${test_hash_tpcb_individuals}")

set(test_hash_tmpbin_individuals
  Sequential
  SequentialResize
  SequentialQuickClean
  SequentialResizeQuickClean
#  RandomNoCollision
#  RandomCollision
  )
add_foedus_test_individual(test_hash_tmpbin "${test_hash_tmpbin_individuals}")
