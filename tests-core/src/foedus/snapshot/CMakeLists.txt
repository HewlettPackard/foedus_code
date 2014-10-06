# Mmm, there is a weird test failure (infinite loop) that happens only when
# this testcase is run on concurrent valgrinds. Quite difficult to debug.
# For now disabled valgrind. Let's fix it when we get more easily reproducible situation.
add_foedus_test_individual_without_valgrind(test_snapshot_basic "Empty;OneArrayCreate;TwoArrayCreate")
