TPC-C Experiments
=================================
This folder implements the popular TPC-C benchmark.
Like many other databases, we intensively use TPCC for performance comparison in papers
and checking performance regression. Further, we provide several variants of TPC-C
to evaluate different aspects of FOEDUS.


Goal of This Document
-----------------
We must keep this document up-to-date and super detailed for two purposes.

* Allow someone else (but with reasonable experiences on linux/C++/DB etc) to re-run our
experiments. For example, **conference reproducibility committees** and **DB-vendors** to compare
against FOEDUS.
* Allow **ourselves** to easily re-run and/or extend our experiments later unless we get
non-volatile memory installed to our brain.

Specifically, we annotate ALL figures/tables in
the [SIGMOD'15 paper](http://www.hpl.hp.com/techreports/2015/HPL-2015-37.pdf)
with detailed steps to reproduce them. Unless individually noted,
*Figure-X* / *Table-Y* below means corresponding results in the paper.


Target Systems
-----------------
Under this folder, we have experiments to run three systems.

* FOEDUS. Scripts/programs in this folder.
* H-Store. Files under [hstore_related](hstore_related).
* SILO. Files under [silo_related](silo_related).

This document focuses on FOEDUS experiments. Go to sub folders for H-Store/SILO experiments.

Actually, we have fourth system in the paper, Shore-MT.
What we used was HP's internal version with Foster B-tree and a few other improvements.
It is not OSS-ed. Most likely the EPFL's version of Shore-MT has a similar performance, though.

Machine Sizing and Scripts
-----------------
Most experimental scripts under this folder has a machine name in its file name,
such as run_dragonhawk.sh, run_nvm_dl580.sh. These files are pre-configured for
our machines. Most likely you don't have the exact same machine,
so **you have to create a new script by modifying numbers in them** according to the size of your
machine.

Most scripts have three versions: Z820, DL580, and DragonHawk.

* Our Z820 is a *desktop* machine with 128 GB DRAM and 2-sockets * 8 cores.
* Our DL580 is a *high-end server* with 3 TB DRAM and 4-sockets * 15 cores.
* Our DragonHawk is a *monster server* with 12 TB DRAM and 16-sockets * 15 cores.

Most likely, your machine fits somewhere between them.
Configure the numbers by interpolating, or by extrapolating if you are trying to run it on a
laptop or on a 256-socket godzilla SGI; oh let me play with it if you do.

* [2015 May] We have also added a _mid-low_ server configuration per request.
It assumes 4-sockets * 8 cores and 1 TB DRAM machine. The suffix is "_mid".

Of course you have to understand what each parameter means. Go on to individual details below.


Completely Disabling Gperftools
-----------------
Gperftools causes a severe (8x-9x) performance degradation on DragonHawk.
This happens even if FOEDUS does NOT use it. Just dynamic-linking causes it.
We figured out this issue after struggling for several months!
  https://code.google.com/p/gperftools/issues/detail?id=133

In short, the only way to work around it is to 1) not have gperftools in your system so that
FOEDUS does not link to it, or 2) run the following.

    export CPUPROFILE_FREQUENCY=1 # Thanks to a bug, this completely disables gperftools.
    # or, prepend the following to all commands
    env CPUPROFILE_FREQUENCY=1

Yes, it's a dirty hack, but works. On large machines, make sure you do either of them.


Simplest FOEDUS TPC-C Execution
-----------------
To get started, or just to do quick performance regression test, run the following:

    # Compile FOEDUS in release mode. Assume you are in foedus_code now.
    # Make sure you read and applied the documents on compilation/dependency.
    mkdir release
    cd release
    cmake ../ -DCMAKE_BUILD_TYPE=Release
    make -j10

    # run it!
    cd experiments-core/src/foedus/tpcc/
    ./tpcc -warehouses=1 -take_snapshot=false -fork_workers=false \
      -nvm_folder=/dev/shm -high_priority=false -null_log_device=false \
      -loggers_per_node=1 -thread_per_node=1 -numa_nodes=1 -log_buffer_mb=512 \
      -neworder_remote_percent=1 -payment_remote_percent=15 \
      -volatile_pool_size=1 -snapshot_pool_size=1 -reducer_buffer_size=1 \
      -duration_micro=1000000

Yeah, a lengthy command line. To see the meaning of each parameter and its default value, run:

    ./tpcc --help

If it runs successfully, you will get something like this at the end:

    I0521 19:14:50.098899 16713 tpcc_driver.cpp:638] final result:<total_result><duration_sec_>1.00021</duration_sec_><worker_count_>1</worker_count_><processed_>119881</processed_><MTPS>0.119856</MTPS><user_requested_aborts_>547</user_requested_aborts_><race_aborts_>0</race_aborts_><largereadset_aborts_>0</largereadset_aborts_><unexpected_aborts_>0</unexpected_aborts_><snapshot_cache_hits_>0</snapshot_cache_hits_><snapshot_cache_misses_>0</snapshot_cache_misses_></total_result>

Look for "MTPS". It's the million-transactions per-second.

If you haven't setup the environment, such as shared-memory and hugepages, you will
get errors like the following:

    [FOEDUS] Failed to allocate node shared memory for node-0. kErrorCodeSocShmAllocFailed...

If this happens, read the *Environment Setup* section
[here](https://github.com/hkimura/foedus_code/tree/master/foedus-core).
The additional message in the error dump often tells the possible cause of it. Check it out, too.


FOEDUS TPC-C on DRAM (Figure 7, Table 2, Figure 9)
-----------------
Summary:

* TPC-C with varying remote-ratio.
* Only volatile pages. No snapshot pages.
* Logging could be disabled/on-DRAM/on-NVRAM.
* Logs on DRAM: run_common.sh, run_z820.sh, run_dl580.sh, run_dragonhawk.sh.
* Logs on NVRAM: run_withlog.sh, run_withlog_z820.sh, run_withlog_dl580.sh, run_withlog_dragonhawk.sh.
For these experiments, refer to the NVRAM experiment in next section for how to setup the NVRAM
folder.

Steps to run the experiments:

    # Example on dragonhawk
    ./run_dragonhawk.sh
    # For Figure 9, run it with "-profile=true". Make sure you have gperftools installed and
    # you do NOT have the CPUPROFILE_FREQUENCY=1 env to disable it.
    # Note that the run with gperftools is significantly slower overall.

Steps to extract the results:

* Result files are in this format: "result_tpcc_$machine_shortname.n$remote_percent.r$rep.log"
* grep -r "final result" > some_file
* Exercise your regex skill[^1] to convert it to a matrix form, then copy-paste to libreoffice or
whatever.
* For Figure 9, "pprof --pdf tpcc tpcc.prof > prof.pdf"

[^1] I really should have a script to do these. I feel sorry for the reproducibility committee.
We will add scripts when we have time.


FOEDUS TPC-C on NVRAM (Figure 8, Table 3, Figure 9)
-----------------
Summary:

* TPC-C with fixed remote-ratio (remote=1).
* Both volatile pages and snapshot pages.
* Both log/data folders are in the emulated NVRAM folder.
* run_nvm.sh, run_nvm_z820.sh, run_nvm_dl580.sh, run_nvm_dragonhawk.sh.


First, you must install the emulated NVRAM filesystem.
Download the upstream linux kernel code, put our NVRAM filesystem code, compile and deploy.
The details of this step are provided in [another repository](https://github.com/hkimura/dummy_nvmfs).

Steps to run the experiments:

    # Example on dragonhawk
    # You must have a no-password sudo permission to create/mount "/testnvm".
    ./run_nvm_dragonhawk.sh
    # For NoSC line, disable_snapshot_cache=true in run_nvm.sh

Steps to extract the results:

* For Figure 8/9, same as DRAM experiment.
* For Table 3, look for log messages like "Done sort/dump ... bytes in ....ms" (reducer),
"LogMapper-... processed ... log entries in ...s" (mapper), and
"Logger-... wrote out ... bytes for epoch-... in ... ms" (logger).


FOEDUS OLAP TPC-C (Figure 10, Figure 11)
-----------------

Summary:

* A modified TPC-C with read-only queries.
* Either only volatile pages or only snapshot pages.
* run_olap.sh, run_olap_z820.sh, run_olap_dragonhawk.sh.


Steps to run the experiments: Read [run_olap_memo.txt](run_olap_memo.txt).
You need to modify constant values and recompile.

Steps to extract the results: Same as DRAM experiments.
