[Super-build] FOEDUS: Fast Optimistic Engine for Data Unification Services
=================================

Overview
--------
FOEDUS is a new transactional key-value library developped at Hewlett-Packard Labs that is optimized
for a large number of CPU cores and NVRAM storage. It is a handy C++ library you can
either include in your source code (by invoking CMake script) or dynamically link to.
In a nutshell, it is something like BerkeleyDB, but it is much more efficient on new hardware.

Folder Structure (For Everyone)
--------
This root project contains a few sub-projects.
Some of them are *NOT* supposed to be directly linked from client programs (_your_ programs).

* [foedus-core](foedus-core) : Key-value store library.
* [foedus-util](foedus-util) : A series of utility programs to help use libfoedus.
* foedus-rdb : Relational database engine on top of foedus-core.
* tests-[core/util/rdb] : Unit testcase projects.
* experiments-[core/util/rdb] : Performance experiments projects.
* [third_party](third_party) : Third party source code used in our programs.

You are supposed to link only to *foedus-core* and *foedus-rdb*.
Other projects are for internal use or to provide executables, rather than libraries.
You can still contain all projects (or this folder's CMakeLists.txt) in your source code,
but note that some restrictions on compiler options apply if you do so.

libfoedus-core (For FOEDUS Users)
-----------
For more details of how your client program links to and uses our library,
start from [foedus-core](foedus-core) and
[its API document](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-doxygen/doxygen/).
The sections below are for people developping FOEDUS itself.

Current Build Status on Jenkins (For FOEDUS Developers)
--------
[![Build Status: release](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-release/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-release/) *release*

[![Build Status: relwithdbginfo](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-relwithdbginfo/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-relwithdbginfo/) *relwithdbginfo*

[![Build Status: debug](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-debug/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-debug/) *debug*

[![Build Status: release-valgrind](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-release-valgrind/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-release-valgrind/) *release-valgrind*

[![Build Status: doxygen](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-doxygen/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-doxygen/) *doxygen*

Building Development Environment (For FOEDUS Developers)
--------
We recommend newer Fedora, Ubuntu/Debian, etc.
In Fedora/RedHat/CentOS etc, run the following:

    sudo yum install gcc gcc-c++ libstdc* cmake glibc glibc-* valgrind valgrind-devel
    sudo yum install libunwind libunwind-devel
    sudo yum install numactl numactl-devel google-perftools google-perftools-devel
    sudo yum install python python-*
    sudo yum install doxygen graphviz mscgen sloccount kdevelop

For valgrind, check its version after installation.
If it is not 3.9 or later, we recommend installing a newer one. See the section below.

If you want to generate doxygen-pdf, also run the following:

    sudo yum install texlive texlive-* okular

If you are the person to compile our rpm packages ("make package"), also run the following:

    sudo yum install rpm-build

If you are the admin to maintain our gitlab installation.
    # see https://fedoraproject.org/wiki/User:Axilleas/GitLab
    sudo yum groupinstall 'Development Tools' 'Development Libraries'
    sudo yum install zlib-devel libyaml-devel openssl-devel gdbm-devel readline-devel ncurses-devel libffi-devel curl git openssh-server redis libxml2-devel libxslt-devel libcurl-devel libicu-devel python

For Ubuntu/Debian, install equivalent modules.
TBD: Ubuntu/Debian user, please provide an equivalent command.
Especially, I know little about .deb packaging.

Compilation (For FOEDUS Developers)
--------
To compile this project, simply build it as a CMake project. For example:

    # Suppose you are at foedus_code.
    # We prohibit in-source build, so you have to create a build folder and compile there.
    mkdir build
    cd build
    # Note, this is equivalent to -DCMAKE_BUILD_TYPE=Debug. You can also use Release/RelWithDbgInfo.
    cmake ../
    make

Or, import it to C++ IDE, such as kdevelop. Any IDEs that support CMake build should work.

If you use kdevelop, don't forget to increase the degree of parallel compilation after importing
the CMake project. The default is 2. You must get a new machine if this is a good number.
Right click project, Click "Open Configuration", Click "Make" icon, "Number of simultaneous jobs".


Running Tests (For FOEDUS Developers)
--------
Go to build folder, and:

    ctest

or

    ctest -j4
    # Pick a test parallelization level according to your machine power. Remember some tests
    # run many threads in them. 4 should be a good number.

In order to skip valgrind versions of the tests (because it takes *long* time!),

    ctest -E valgrind

On the other hand, if you want to run only valgrind versions,

    ctest -R valgrind

We strongly recommend to use valgrind 3.9 or later to run all tests on valgrind due to a
performance issue fixed in valgrind 3.9. See the section below.

If valgrind reports a false positive or third party's bug, add them to
foedus-core/tools/valgrind.supp.

    valgrind --leak-check=full --suppressions=<path_to_valgrind.supp> --gen-suppressions=all ./<your_program>

For more details, check out [CTEST](http://www.vtk.org/Wiki/CMake/Testing_With_CTest)/[CMAKE](http://www.cmake.org/) documentation.


Notes for valgrind and installing the latest version of valgrind (For FOEDUS Developers)
--------
Valgrind is a powerful tool to debug programs, and we keep our program free from memory-leak
and bogus memory accesses by regularly running valgrind tests (once per hour on Jenkins).

You are also encouraged to run valgrind versions of tests on your machine.
However, there is one issue in valgrind ~3.8 that makes it quite troublesome.

Valgrind executes programs in a single-threaded fashion. Thus, if your program has an infinite
loop (eg spinlock) without yielding to other threads, valgrind never finishes the execution.
This is why we must use our SPINLOCK_WHILE macro and/or put foedus::assorted::spinlock_yield()
calls in such places (not too much to avoid unnecessary overhead, of course).

Even with these yielding, valgrind ~3.8 sometimes causes an infinite or semi-infinite loop
in condition variables, or std::condition_variables::wait()/pthread_cond_wait().
This problem is fixed in valgrind 3.9, and you can see the difference by running
tests-core/src/foedus/assorted/test_raw_atomics on valgrind 3.8.1 (almost always infinite loop)
and valgrind 3.9.0 (always within a few sec).

If you are using an older linux distro (eg Fedora 19 whose latest valgrind in yum repo is 3.8.1),
we strongly recommend to install latest valgrind from source. Follow these steps:

* Download the source from [here](http://valgrind.org/downloads/current.html).
* Usual triplet: "./configure --prefix=/home/yourname/local; make; make install"
* Cleanly rebuild foedus so that our cmake script finds the newer valgrind installation.
(the cmake script searches in this order: ~/local, /usr/local, /usr)
* (Optional) Edit your environment variable to see /home/yourname/local/bin before /usr/bin.
This is useful when you type "valgrind" in terminal, which might not happen often.


Enabling Transparent Hugepages (For FOEDUS Developers)
--------
Make sure you enable THP (Transparent Huge Page) in *always* mode.
See the section in [foedus-core](foedus-core).


Git Push/Branch Convention (For FOEDUS Developers)
--------
We follow the [git-flow](http://nvie.com/posts/a-successful-git-branching-model/) convention.
Never ever directly push to *master* branch (most likely you do not have the permission anyways).

Each person should usually work on her/his own branch made from *develop* branch.
On your own branch, do what you want. We recommend to run at least non-valgrind testcases
either on *relwithdebinfo* or *debug* before commit, but it is really up to you.

Before pushing to *develop* branch, you must pass all testcases on *relwithdebinfo*, *debug*,
and *release*, preferrably including valgrind versions (not mandatory if you
are in hurry). But, if Jenkins reports an error on develop branch, others will yell at you.

Hideaki will push to *master* from *develop*.

Coding Convention (For FOEDUS Developers)
--------
We fully conform to [Google C++ Style Guide](http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml)
except the arguable rule on streams. See [the discussions](http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml#Streams)
if you are interested.

Max 100 characters per line, 4-spaces indent, no tab, and several other rules. Read the guide.
We enforce the coding convention by cpplint.py.
All projects run cpplint for every build and report violations as warnings.



kdevelop-specific Recommendations (For FOEDUS Developers)
--------
Only if you use kdevelop, and not if you have your own configuration (which is totally okay).

* Settings, Configure Editor, Appearance, Borders, Enable "Show folding markers", "Show line numbers"
* Settings, Configure Editor, Editing, "Show static word wrap marker" with 100 characters.
* Settings, Configure Editor, Editing, Indentation, "Spaces" 4 characters.
* Settings, Configure Editor, Open/Save, General, Append newline at end of file.

We also have a template file for creating new classes in kdevelop.

Right click a folder, "Create From Template", "Load Template From File", choose
kdevtemplate.desktop under foedus-core/tools.

When you use it, specify your class name such as foedus::storage::masstree::MyNewClass.
Unfortunately, kdevelop template has limited flexibility in a few things:

* Manually edit the generated file name so that words are separated by "\_" (eg my\_new\_class.cpp)
* Manually edit the generated cpp/hpp path so that hpp is under include, cpp is under src.
* Don't let the wizard to add new cpp to a target. Most likely it puts it in a stupid place. Add it to a right place yourself.
