Perforamnce Experiments for libfoedus-core
=================================

What these are
--------
All kinds of performance experiments, for paper writing, for performance regression tests, etc.
These must be run on beefy machines unlike unit test cases.


How to run
-----------
Each experiment is built as an independent executable. Just build in release mode and run.
Each executable should provide --help or -h to describe runtime parameters.
Also, if more than just launching the executable, each experiment's main cpp file should have a
detailed description of the series of steps to run the experiment.


House rule: Describe your experiments
-----------
It's very often that other people or yourself after half a year have a hard time to understand
what each experiment does, ending up with re-implementing the experiment to make sure. Absurd.

To avoid it, each experiment's main cpp file should note at least the following stuffs.

* Author: Your name (@author)
* Date: Created time and last updated time (@date)
* Title: One-line description of the experiment. (@brief)
* Details: What they do, why you made it, etc. (@details)
* Environments: Assumed environments. (@section ENVIRONMENTS Environments)
* Other notes: Special build/config/tuning procedures, etc. (@section OTHER Other notes)
* Latest experimental result: See foedus_results/README.markdown. (@section RESULTS Latest Results)

For example, foedus/storage/array/readonly_experiment.cpp has a following comment at beginning.

    /*
     * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
     * The license and distribution terms for this file are placed in LICENSE.txt.
     */
    /**
     * @file foedus/storage/array/readonly_experiment.cpp
     * @brief Read-only uniform-random accesses on array storage
     * @author kimurhid
     * @date 2014/04/14
     * @details
     * This is the first experiment to see the speed-of-light, where we use the simplest
     * data structure, array, and run read-only queries. This is supposed to run VERY fast.
     * Actually, we observed more than 300 MQPS in a desktop machine if the payload is small (16 bytes).
     *
     * @section ENVIRONMENTS Environments
     * At least 1GB of available RAM.
     *
     * @section OTHER Other notes
     * No special steps to build/run this expriment. This is self-contained.
     *
     * @section RESULTS Latest Results
     * foedus_results/20140414_kimurhid_array_readonly
     *
     * @todo PAYLOAD/DURATION_MICRO/RECORDS are so far hard-coded constants, not program arguments.
     */

Again, do *NOT* upload the results to main foedus_code repo.
