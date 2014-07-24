set(test_masstree_basic_individuals
  Create
  CreateAndQuery
  CreateAndInsert
  CreateAndInsertAndRead
  Overwrite
  NextLayer
  SplitBorder
  SplitBorderNormalized
  SplitInNextLayer
  CreateAndDrop
  )
add_foedus_test_individual(test_masstree_basic "${test_masstree_basic_individuals}")
