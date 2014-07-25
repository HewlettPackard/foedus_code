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

add_foedus_test_individual(test_masstree_split "SplitBorder;SplitBorderNormalized;SplitInNextLayer")

