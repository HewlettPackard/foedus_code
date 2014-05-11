set(test_direct_io_file_individuals
    Create
    CreateDevShm
    CreateTmp
    CreateAppend
    CreateWrite
    WriteWithLogBuffer
    WriteWithLogBufferPad
)
add_foedus_test_individual(test_direct_io_file "${test_direct_io_file_individuals}")
