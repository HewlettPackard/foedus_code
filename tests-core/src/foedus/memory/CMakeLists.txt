add_foedus_test_individual(test_aligned_memory "Instantiate;Instantiate2;Move;Slice")
add_foedus_test_individual(test_engine_memory "SingleNode;TwoNodes")

# because we do fork/exec and exit(), we can't run valgrind on this test.
add_foedus_test_individual_without_valgrind(test_shared_memory "Alone;ShareFork;ShareSpawn")
