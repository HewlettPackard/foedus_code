add_foedus_test_individual(test_array_basic "CreateAndQuery;CreateAndWrite;CreateAndReadWrite")
set(test_array_tpcb_individuals
    SingleThreadedNoContention
    TwoThreadedNoContention
    FourThreadedNoContention
    SingleThreadedContended
    TwoThreadedContended
    FourThreadedContended)
add_foedus_test_individual(test_array_tpcb "${test_array_tpcb_individuals}")
