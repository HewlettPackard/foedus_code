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
  )
add_foedus_test_individual(test_snapshot_array "${test_snapshot_array_individuals}")

add_foedus_test_individual(test_snapshot_sequential "AppendsOneLogger;AppendsTwoLoggers;AppendsTwoPartitions")

set(test_snapshot_masstree_individuals
  InsertsNormalizedOneLogger
  InsertsNormalizedTwoLoggers
  InsertsNormalizedTwoPartitions
  )
add_foedus_test_individual(test_snapshot_masstree "${test_snapshot_masstree_individuals}")
