# Mmm, there is a weird test failure (infinite loop) that happens only when
# this testcase is run on concurrent valgrinds. Quite difficult to debug.
# For now disabled valgrind. Let's fix it when we get more easily reproducible situation.
add_foedus_test_individual_without_valgrind(test_snapshot_basic "Empty;OneArrayCreate;TwoArrayCreate")

set(test_snapshot_array_individuals
  OverwritesOneLogger
  OverwritesTwoLoggers
  OverwritesTwoPartitions
  IncrementsOneLogger
  IncrementsTwoLoggers
  IncrementsTwoPartitions
  IncrementsTwiceOneLogger
  IncrementsTwiceTwoLoggers
  IncrementsTwiceTwoPartitions
  TwoArraysOneLogger
  TwoArraysTwoLoggers
  TwoArraysTwoPartitions
  HolesOneLogger
  HolesTwoLoggers
  HolesTwoPartitions
  OverwritesOneLogger2Lv
  OverwritesTwoLoggers2Lv
  OverwritesTwoPartitions2Lv
  IncrementsOneLogger2Lv
  IncrementsTwoLoggers2Lv
  IncrementsTwoPartitions2Lv
  IncrementsTwiceOneLogger2Lv
  IncrementsTwiceTwoLoggers2Lv
  IncrementsTwiceTwoPartitions2Lv
  TwoArraysOneLogger2Lv
  TwoArraysTwoLoggers2Lv
  TwoArraysTwoPartitions2Lv
  HolesOneLogger2Lv
  HolesTwoLoggers2Lv
  HolesTwoPartitions2Lv
  OverwritesOneLogger3Lv
  OverwritesTwoLoggers3Lv
  OverwritesTwoPartitions3Lv
  IncrementsOneLogger3Lv
  IncrementsTwoLoggers3Lv
  IncrementsTwoPartitions3Lv
  IncrementsTwiceOneLogger3Lv
  IncrementsTwiceTwoLoggers3Lv
  IncrementsTwiceTwoPartitions3Lv
  TwoArraysOneLogger3Lv
  TwoArraysTwoLoggers3Lv
  TwoArraysTwoPartitions3Lv
  HolesOneLogger3Lv
  HolesTwoLoggers3Lv
  HolesTwoPartitions3Lv
  )
add_foedus_test_individual(test_snapshot_array "${test_snapshot_array_individuals}")

add_foedus_test_individual(test_snapshot_array_issue_127 "Reproduce")

add_foedus_test_individual(test_snapshot_sequential "AppendsOneLogger;AppendsTwoLoggers;AppendsTwoPartitions")

set(test_snapshot_hash_individuals
  InsertsFixedLenOneLogger1Lv
  InsertsFixedLenOneLogger2Lv
  InsertsFixedLenTwoLoggers1Lv
  InsertsFixedLenTwoLoggers2Lv
  InsertsFixedLenTwoPartitions1Lv
  InsertsFixedLenTwoPartitions2Lv
  InsertsVarlenOneLogger1Lv
  InsertsVarlenOneLogger2Lv
  InsertsVarlenTwoLoggers1Lv
  InsertsVarlenTwoLoggers2Lv
  InsertsVarlenTwoPartitions1Lv
  InsertsVarlenTwoPartitions2Lv
  )
add_foedus_test_individual(test_snapshot_hash "${test_snapshot_hash_individuals}")

set(test_snapshot_masstree_individuals
  InsertsNormalizedOneLogger
  InsertsNormalizedTwoLoggers
  InsertsNormalizedTwoPartitions
  InsertsVarlenOneLogger
  InsertsVarlenTwoLoggers
  InsertsVarlenTwoPartitions
  )
add_foedus_test_individual(test_snapshot_masstree "${test_snapshot_masstree_individuals}")

set(test_merge_sort_individuals
  SingleInputDistinctKeyArray
  SingleInputDistinctEpochArray
  SingleInputDistinctOrdinalArray
  SingleInputDuplicatesArray
  MultiInputsDistinctKeyArray
  MultiInputsDistinctEpochArray
  MultiInputsDistinctOrdinalArray
  MultiInputsDuplicatesArray
  SingleInputDistinctKeyHashFixlen
  SingleInputDistinctEpochHashFixlen
  SingleInputDistinctOrdinalHashFixlen
  SingleInputDuplicatesHashFixlen
  MultiInputsDistinctKeyHashFixlen
  MultiInputsDistinctEpochHashFixlen
  MultiInputsDistinctOrdinalHashFixlen
  MultiInputsDuplicatesHashFixlen
  SingleInputDistinctKeyHashVarlen
  SingleInputDistinctEpochHashVarlen
  SingleInputDistinctOrdinalHashVarlen
  SingleInputDuplicatesHashVarlen
  MultiInputsDistinctKeyHashVarlen
  MultiInputsDistinctEpochHashVarlen
  MultiInputsDistinctOrdinalHashVarlen
  MultiInputsDuplicatesHashVarlen
  SingleInputDistinctKeyMasstreeNormalized
  SingleInputDistinctEpochMasstreeNormalized
  SingleInputDistinctOrdinalMasstreeNormalized
  SingleInputDuplicatesMasstreeNormalized
  MultiInputsDistinctKeyMasstreeNormalized
  MultiInputsDistinctEpochMasstreeNormalized
  MultiInputsDistinctOrdinalMasstreeNormalized
  MultiInputsDuplicatesMasstreeNormalized
  SingleInputDistinctKeyMasstreeVarlen
  SingleInputDistinctEpochMasstreeVarlen
  SingleInputDistinctOrdinalMasstreeVarlen
  SingleInputDuplicatesMasstreeVarlen
  MultiInputsDistinctKeyMasstreeVarlen
  MultiInputsDistinctEpochMasstreeVarlen
  MultiInputsDistinctOrdinalMasstreeVarlen
  MultiInputsDuplicatesMasstreeVarlen
  )
add_foedus_test_individual(test_merge_sort "${test_merge_sort_individuals}")

add_foedus_test_individual(test_mapper_io "OneIteration;TwoIterations;OneIterationUnlucky;TwoIterationsUnlucky")
