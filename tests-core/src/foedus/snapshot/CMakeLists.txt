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
  )
add_foedus_test_individual(test_snapshot_array "${test_snapshot_array_individuals}")

add_foedus_test_individual(test_snapshot_sequential "AppendsOneLogger;AppendsTwoLoggers;AppendsTwoPartitions")

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
