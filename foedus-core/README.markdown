FOEDUS Core Library
=================================

Introduction
--------
This project is the gut of FOEDUS as a transactional key-value storage system.
Your client program can use this library by containing the CMakeLists.txt or linking to the shared
library.

For more introductory stuffs, visit [our github site](https://github.com/hkimura/foedus).
and/or take a look at the [overview paper](http://www.hpl.hp.com/techreports/2015/HPL-2015-37.html).


Hardware/Compiler/OS Requirements
--------

* We support only 64-bits CPUs. More specifically, x86_64 and ARMv8.
* We assume Linux/Unix so far. No MacOS, Windows, nor Solaris.
* We strongly recommend linux kernel 3.16 or later. [^1]
* We assume fsync(2) penetrates all the way through the device. [^2]
* We require reasonably modern C++ compilers, namely gcc 4.8.2 or later. [^3]
* We depend on CMake and several other toolsets/libraries. But, we guarantee that
all of them are commonly available from default package repositories.
Otherwise, we include the dependency in our code.

[^1]: In older linux kernel, page-fault of hugepages was almost single-threaded (!!!).
This issue was fixed in 3.15 along with many other scalability improvements contributed by HP's
linux developers.

[^2]: If this is not the case, check your write-cache settings in the filesystem and device driver.
Unfortunately, even if users configure it right, some storage device sacrifices durability for the
sake of performance. So is some file system. For those environments, we cannot guarantee ACID.

[^3]: [2015 Jul] We have added an _experimental_ support for clang.


Verified Environments
--------
The Linux kernel, distros, compilers, and libraries are quickly moving forward.
Which is great, but we often see breaking issues due to version differences.
If you want to minimize unexpected bumps, we recommend to use one of the following
**verified environments** where we continuously test our code on Jenkins services.

* (Primary) Fedora 21, x86_64. [Our Jenkins Server](http://foedus-build.hpl.hp.com:8080/).
* Ubuntu 15.04, x86_64. [Our Jenkins Server](http://foedus-build-ub.hpl.hp.com:8080/).
* Ubuntu 14.04, aarch64. [Our Jenkins Server](http://ms01915-003.hpl.hp.com:8080/).

We will occasionally switch to semi-latest versions of Fedora (or some Redhat-flavor) and Ubuntu
(or some Debian-flavor). In other words, we do not have enough resource to support
versions that are several years old or less-popular distros.


Environment Setup (ATTENTION! Read this before using FOEDUS!)
--------
Unfortunately, there are a few things you have to setup in your environment.
These setups require sudo permission, so FOEDUS does not set them up itself.
If you get any error, make sure you setup the followings.

* Allocate enough hugepages.

FOEDUS uses non-transparent hugepages for most memory allocations.
If the machine does not have enough hugepages, it will not even start up.
Check your current configuration as follows.

    # Check your current Hugepage setting
    more /proc/sys/vm/nr_hugepages
    more /proc/meminfo

Depending on the size of volatile pool and logger buffer you specify in the configuration,
adjust the number of hugepages as follows.

    # Change the number depending on hugepage size and required memory size.
    # The following command allocates 2GB if Hugepagesize is 2048 kB.
    sudo sh -c 'echo 1000 > /proc/sys/vm/nr_hugepages'
    # 1000 is a minimal number. It's just for running testcases. Increase the number according to your configuration.

**CAUTION: PLEASE REMEMBER! THE ABOVE NUMBER IS TO JUST PASS TESTCASES. YOU MUST ALLOCATE MANY MORE HUGEPAGES DEPENDING ON YOUR CONFIGURATION.**

Note that we no longer rely on transparent hugepages (THP), every hugepage allocation is via
mmap and shmget. Thus, the user has to explicitly pre-allocate hugepages as above.
We made this decision for a few reasons. First, a rumor says future linux will purge THP.
Second, there are several performance issues in THP, including lack of 1GB hugepages and compaction.
Third, many other large-scale software uses non-transparent hugepages. Mixing THP and
non-transparent hugepages is known to have several issues, so we do not use THP to be a good
citizen. Hence, we now recommend to disable THP.


* Increase maximum shared memory size.

The default maximum of shared memory is ridiculously small.
We recommend to set an infinitely large number as follows:

    sudo sysctl -w kernel.shmmax=9223372036854775807
    sudo sysctl -w kernel.shmall=1152921504606846720
    sudo sysctl -w kernel.shmmni=409600
    sudo sysctl -w vm.max_map_count=2147483647
    sudo sysctl -p

The above command is effective only until reboot. In order to make it permanent, add
the following entries to /etc/sysctl.conf:

    # Add these entries to /etc/sysctl.conf.
    kernel.shmmax = 9223372036854775807
    kernel.shmall = 1152921504606846720
    kernel.shmmni = 409600
    vm.max_map_count = 2147483647

If you have some reason to set a smaller number, carefully calculate required sizes and
replace the above numbers.


* Configure ulimit

There are a few ulimit settings you have to increase.
Some of these settings has a ridiculously small default value, so FOEDUS will not start up
otherwise.

    # Check your current ulimit setting
    ulimit -a

    # Add these entries to /etc/security/limits.conf.
    <your_user_name>  - nofile 655360
    <your_user_name>  - nproc 655360
    <your_user_name>  - rtprio 99
    <your_user_name>  - memlock -1

This will not take effect until you log-in again.
If you want to immediately apply the values without log out, one workaround is:

    # Note, even this does not work for already-running sessions/GUIs. re-login to make sure.
    sudo -i -u <your_user_name>


* Configure hugetlb_shm_group

We are not sure when this one is needed, but we did observe shmget failures in some environment,
namely Ubuntu. On Fedora, we never failed a hugepage allocation via shmget even without
configuring this parameter.

    # Create a user group, say hugeshm, then add yourself to the group
    sudo su
    groupadd hugeshm
    usermod -a -G hugeshm <your_account>
    id <your_account> # this tells the group ID

    # then write the group ID to /proc/sys/vm/hugetlb_shm_group. To make this
    # configuration permanent, add "vm.hugetlb_shm_group" entry in sysctl.conf.
    echo <Group ID> > /proc/sys/vm/hugetlb_shm_group

Finally, **we strongly recommend rebooting** the machine after configuring all of the above.
sysctl -p is supposed to be enough, but do make sure by rebooting.

* Error messages you will see

If the above configurations are not effective, you will get kErrorCodeSocShmAllocFailed error.
Our ErrorStack object often contains additional details of the cause, for example:

    kErrorCodeSocShmAllocFailed(3073):SOC    : Failed to allocate a shared memory. This is usually
    caused by a misconfigured environment. Check the following:
    - Too small kernel.shmmax/kernel.shmmin. sudo sysctl -w kernel.shmmax=9223372036854775807;sudo sysctl -w kernel.shmall=1152921504606846720;sudo sysctl -w kernel.shmmni=409600;sudo sysctl -p
    - Too few hugepages. sudo sh -c 'echo 1000 > /proc/sys/vm/nr_hugepages'
    - Too small mlock. Check ulimit -l. Configure /etc/security/limits.conf
    - Too small nofile. Check ulimit -a. Configure /etc/security/limits.conf
    - Simply larger than DRAM in the machine.
    - (Very rare) shm key conflict.
    (Latest system call error=[Errno 1] Operation not permitted)
    (Additional message=shmget() failed! size=4194304, os_error=[Errno 1] Operation not permitted, ...

In the above example, "[Errno 1] Operation not permitted" strongly implies that
there is a limits/permissions issue on hugepages and/or shared memory.
Unfortunately, this is one of the nastiest configuration in linux.

Compilation
--------
(If you get a compilation error for missing libraries, refer to Dependencies section.)
Suppose you want to contain the CMakeLists.txt in your CMake project.
Add the following lines in your CMakeLists.txt:

    add_subdirectory(path_to_foedus-core ${CMAKE_CURRENT_BINARY_DIR}/foedus-core)
    include_directories(path_to_foedus-core/include)
    add_executable(your_program your_program_x.cpp your_program_y.cpp ...)
    target_link_libraries(your_program foedus-core)

Compile your program to see if it is correctly linked to libfoedus-core.so.

Alternatively, you can install foedus-core into either your local directory or standard directories,
such as /usr/lib /usr/lib64 etc. In that case, add the following lines in your CMakeLists.txt:

    # Copy the FindFoedusCore.cmake file (placed under cmake) into your cmake folder beforehand:
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
    find_package(FoedusCore REQUIRED) # This sets FOEDUS_CORE_INCLUDE_DIR and FOEDUS_CORE_LIBRARIES
    include_directories(${FOEDUS_CORE_INCLUDE_DIR})
    add_executable(your_program your_program_x.cpp your_program_y.cpp ...)
    target_link_libraries(your_program ${FOEDUS_CORE_LIBRARIES})

We recommend your program to turn on C++11, but not mandatory. You can link to and use
libfoedus-core from C++98/03 projects without problems although some APIs are not exposed then.
If you are to enable C++11, on the other hand, add the following in your CMakeLists.

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

Get Started
-----------
Here is a minimal example program to create a key-value storage and query on it.
**NOTE: Complete Environment Setup Beforehand!**

    #include <iostream>

    #include "foedus/engine.hpp"
    #include "foedus/engine_options.hpp"
    #include "foedus/epoch.hpp"
    #include "foedus/proc/proc_manager.hpp"
    #include "foedus/storage/storage_manager.hpp"
    #include "foedus/storage/array/array_metadata.hpp"
    #include "foedus/storage/array/array_storage.hpp"
    #include "foedus/thread/thread.hpp"
    #include "foedus/thread/thread_pool.hpp"
    #include "foedus/xct/xct_manager.hpp"

    const uint16_t kPayload = 16;
    const uint32_t kRecords = 1 << 20;
    const char* kName = "myarray";
    const char* kProc = "myproc";

    foedus::ErrorStack my_proc(const foedus::proc::ProcArguments& args) {
      foedus::Engine* engine = args.engine_;
      foedus::storage::array::ArrayStorage array(engine, kName);
      foedus::xct::XctManager* xct_manager = engine->get_xct_manager();
      WRAP_ERROR_CODE(xct_manager->begin_xct(context, foedus::xct::kSerializable));
      char buf[kPayload];
      WRAP_ERROR_CODE(array.get_record(context, 123, buf));
      foedus::Epoch commit_epoch;
      WRAP_ERROR_CODE(xct_manager->precommit_xct(context, &commit_epoch));
      WRAP_ERROR_CODE(xct_manager->wait_for_commit(commit_epoch));
      return foedus::kRetOk;
    }

    int main(int argc, char argv) {
      foedus::EngineOptions options;
      foedus::Engine engine(options);
      engine.get_proc_manager()->pre_register(kProc, my_proc);
      COERCE_ERROR(engine.initialize());

      foedus::UninitializeGuard guard(&engine);
      foedus::Epoch create_epoch;
      foedus::storage::array::ArrayMetadata meta(kName, kPayload, kRecords);
      COERCE_ERROR(engine.get_storage_manager()->create_storage(&meta, &create_epoch));
      foedus::ErrorStack result = engine.get_thread_pool()->impersonate_synchronous(kProc);
      std::cout << "result=" << result << std::endl;
      COERCE_ERROR(engine.uninitialize());

      return 0;
    }


API Documents
-----------
For more details, start from <a href="modules.html">Module List</a> and
<a href="namespaces.html">Namespace List</a>.

Dependencies
-----------
We try hard to minimize library dependency so that at least libfoedus-core works in various
environments. We statically link most of the libraries we internally use, thus they are not
exposed as library dependency. The only exceptions are standard c++ library, libpthread and libnuma.

Standard C++ library is avaialble in most environments, so most likely you have already installed
them. If not, run the following:

    sudo yum install libstdc* # RedHat/Fedora

pthread is a fundamental library to execute multi-threaded programs.
Note that you have to link to libpthread.so even if you use C++11. C++11 threading merely invokes
libpthread, so you need to link to libpthread.so. Otherwise, libstdc will throw an error
at *runtime* (ouch!) when our engine invokes it. Run the following:

    sudo yum install glibc glibc-devel    # RedHat/Fedora
    sudo apt-get install build-essential  # Debian/Ubuntu

Then, include the following in your CMakeLists.txt:

    find_package(Threads REQUIRED)
    target_link_libraries(your_program ${CMAKE_THREAD_LIBS_INIT})

Optimizing for NUMA architecture is also too essential to miss in our project.
Thus, we link to libnuma. And, (to my knowledge) there is no good way to statically link to
libnuma (libnuma is under LGPL). Hence, your client program must also link to libnuma.so.
Run the following:

    sudo yum install numactl numactl-devel  # RedHat/Fedora
    sudo apt-get install libnuma-dev        # Debian/Ubuntu

Copy FindNuma.cmake in this cmake folder, then add the following in your CMakeLists.txt:

    find_package(Numa REQUIRED)
    target_link_libraries(your_program ${NUMA_LIBRARY})

Although not mandatory, libfoedus-core provides additional functionalities if there is
google-perftools-devel.

    sudo yum install google-perftools google-perftools-devel    # RedHat/Fedora

FOEDUS outputs a detailed backtrace to logs/traces if you have libdwarf installed.
Whenever you report some bugs/crashes, we appreciate installing it to get informative stacktrace.

    sudo yum install libdwarf libdwarf-devel    # RedHat/Fedora
    sudo apt-get install libdwarf-dev           # Debian/Ubuntu

Another optional library is [PAPI](http://icl.cs.utk.edu/trac/papi/), with which FOEDUS can provide
additional performance counters.

    sudo yum install papi papi-devel papi-static    # RedHat/Fedora

We use none of boost libraries. We might consider using some of the header-only boost libraries,
but we will surely stay away from non-header-only ones (eg filesystem).

One last optional package is tmpwatch to clean up /tmp where we output glog log files.

    sudo yum install tmpwatch
    sudo /usr/bin/tmpwatch -am 24 /tmp  # If you want, set this up as a cron job


Licensing
--------
See [LICENSE.txt](LICENSE.txt) for the license term of libfoedus-core itself.

In short, FOEDUS is open-sourced under **GPL** with **classpath exception that covers every file**.
Yes, every file is exception though it might sound weird.
This means you can link to libfoedus, either dynamically or statically, from an arbitrary
program: GPL, Apache/BSD, or even proprietary program. However, if you modify FOEDUS itself,
you need to provide your modification in a GPL-fashion. libstdc++ in gcc employs a similar license.
We believe this style of GPL is more flexible/simple for users than LGPL.

We internally had intensive discussions on Apache/BSD vs GPL/LGPL, and this is our *final* decision.
Almost no chance of later switching to Apache/BSD or other permissive licenses.

All manuals/documents (doxygen-generated HTMLs) are in Creative Commons.

libfoedus-core uses a few open source libraries listed below.

|    Library   | License |    Linking/Distribution in libfoedus-core    |
|:------------:|---------|----------------------------------------------|
| glog         | BSD     | Static-link. Contains source code.           |
| tinyxml2     | ZLIB    | Static-link. Contains source code.           |
| libbacktrace | BSD     | Static-link.  Contains source code.          |
| gperftools   | BSD     | Optional dynamic-link. Distributes nothing.  |
| papi         | BSD(?)  | Optional dynamic-link. Distributes nothing.  |
| libnuma      | LGPL    | Dynamic-link. Distributes nothing.           |
| glibc/stdc++ | GPL+Exp | Dynamic-link. Distributes nothing.           |
| valgrind     | BSD     | Header-only.  Contains valgrind.h only.      |
| xxHash       | BSD     | Static-link.  Contains source code.          |

For more details, see COPYING/LICENSE.txt/etc in the third-party folder.
