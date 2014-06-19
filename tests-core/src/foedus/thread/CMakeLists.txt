set(test_thread_pool_individual
  ImpersonateOneOne
  ImpersonateTwoOne
  ImpersonateFourOne
  ImpersonateFourFour
  ImpersonateTenFour
  ImpersonateManyFour
  ImpersonateManyMany)
add_foedus_test_individual(test_thread_pool "${test_thread_pool_individual}")

add_foedus_test_individual(test_stoppable_thread "Minimal;Wakeup;Many")
add_foedus_test_individual(test_rendezvous "Instantiate;Signal;Simple;Many")
