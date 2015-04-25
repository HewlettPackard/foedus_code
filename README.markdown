[Super-build] FOEDUS: Fast Optimistic Engine for Data Unification Services
=================================

Overview
--------
FOEDUS is a new transactional key-value library developed at Hewlett-Packard Labs that is optimized
for a large number of CPU cores and NVRAM storage. It is a handy C++ library you can
either include in your source code (by invoking CMake script) or dynamically link to.
In a nutshell, it is something like BerkeleyDB, but it is much more efficient on new hardware.

For more details, take a look at the [overview paper](https://github.com/hkimura/foedus_oa_papers/raw/master/foedus_sigmod2015_cr.pdf).


Alpha-Version WARNING
--------
The repository is currently in **PRE-ALPHA** state.
Nothing is guaranteed.
Please expect that many parts of the code are unstable and might lack critical features.
We are working hard to move on to next steps hinted below, but without any promises.
If you want to expedite the development, **PLEASE JOIN US**. That's the spirit of open-source.

* Alpha Version (aka first open sourcing): This is supposed to be at the beginning of June 2015
at/before SIGMOD conference. By this time, we should have most of critical features.
But, still no guarantee for stable behavior, data migration to next version, etc.
Important APIs and even the library name might change in next versions.
This version is for people who want to take a look at FOEDUS, and for early adopters who
are okay to adjust their programs when the APIs significantly change.
* Beta Version: This is supposed to be released sometime in 2016, hopefully early 2016.
We should fix most of critical issues/features by this time so that users can start
developing their programs on top of FOEDUS.
We will start release versioning from this point, probably from ver 0.1.
* Stable Version (aka ver 1.0): Some time between 2018 to 2020.
We really need more people to make this happen on time,
especially for stabilizing/documenting FOEDUS and for establishing/helping the community.

Again, we are in **PRE-ALPHA** now.
**BE REALLY WARNED. I TOLD YOU, OKAY?**


Folder Structure (For Everyone)
--------
This root project contains a few sub-projects.
Some of them are **NOT** supposed to be directly linked from client programs (_your_ programs).

* [foedus-core](foedus-core) : Key-value store library.
* [foedus-util](foedus-util) : A series of utility programs to help use libfoedus.
* foedus-rdb (not_exist_yet) : Relational database engine on top of foedus-core.
* tests-[core/util/rdb] : Unit testcase projects.
* experiments-[core/util/rdb] : Performance experiments projects.
* [third_party](third_party) : Third party source code used in our programs.

You are supposed to link only to **foedus-core** and *foedus-rdb*.
Other projects are for internal use or to provide executables, rather than libraries.
You can still contain all projects (or this folder's CMakeLists.txt) in your source code,
but note that some restrictions on compiler options apply if you do so.

libfoedus-core (For FOEDUS Users)
-----------
For more details of how your client program links to and uses our library,
start from [foedus-core](foedus-core) and
[its API document](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-doxygen/doxygen/).
Licensing, short get-started examples, etc for **users** are there.
The sections below are for people developing FOEDUS itself.

Current Build Status on Jenkins (For FOEDUS Developers)
--------
|    Build Type            |     *master* Branch     |        *develop* Branch       |
|--------------------------|:-----------------------:|:-----------------------------:|
| x86 release              | [![Build Status: master-release](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-release/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-release/) | [![Build Status: develop-release](http://243-1.bfc.hpl.hp.com:8080/job/foedus-develop-release/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-develop-release/) |
| x86 relwithdebinfo       | [![Build Status: master-relwithdebinfo](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-relwithdbginfo/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-relwithdbginfo/) | [![Build Status: develop-relwithdebinfo](http://243-1.bfc.hpl.hp.com:8080/job/foedus-develop-relwithdbginfo/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-develop-relwithdbginfo/) |
| x86 debug                | [![Build Status: master-debug](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-debug/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-debug/) | [![Build Status: develop-debug](http://243-1.bfc.hpl.hp.com:8080/job/foedus-develop-debug/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-develop-debug/) |
| x86 release-valgrind     | [![Build Status: master-release-valgrind](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-release-valgrind/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-release-valgrind/) | [![Build Status: develop-release-valgrind](http://243-1.bfc.hpl.hp.com:8080/job/foedus-develop-release-valgrind/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-develop-release-valgrind/) |
| x86 doxygen              | [![Build Status: master-doxygen](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-doxygen/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-master-doxygen/) | [![Build Status: develop-doxygen](http://243-1.bfc.hpl.hp.com:8080/job/foedus-develop-doxygen/badge/icon)](http://243-1.bfc.hpl.hp.com:8080/job/foedus-develop-doxygen/) |
| aarch64 release          | [![Build Status: master-release](http://ms01915-003.hpl.hp.com:8080/job/foedus-master-aarch64-release/badge/icon)](http://ms01915-003.hpl.hp.com:8080/job/foedus-master-aarch64-release/) | [![Build Status: develop-release](http://ms01915-003.hpl.hp.com:8080/job/foedus-develop-aarch64-release/badge/icon)](http://ms01915-003.hpl.hp.com:8080/job/foedus-develop-aarch64-release/) |
| aarch64 relwithdebinfo   | [![Build Status: master-relwithdebinfo](http://ms01915-003.hpl.hp.com:8080/job/foedus-master-aarch64-relwithdbginfo/badge/icon)](http://ms01915-003.hpl.hp.com:8080/job/foedus-master-aarch64-relwithdbginfo/) | [![Build Status: develop-relwithdebinfo](http://ms01915-003.hpl.hp.com:8080/job/foedus-develop-aarch64-relwithdbginfo/badge/icon)](http://ms01915-003.hpl.hp.com:8080/job/foedus-develop-aarch64-relwithdbginfo/) |
| aarch64 debug            | [![Build Status: master-debug](http://ms01915-003.hpl.hp.com:8080/job/foedus-master-aarch64-debug/badge/icon)](http://ms01915-003.hpl.hp.com:8080/job/foedus-master-aarch64-debug/) | [![Build Status: develop-debug](http://ms01915-003.hpl.hp.com:8080/job/foedus-develop-aarch64-debug/badge/icon)](http://ms01915-003.hpl.hp.com:8080/job/foedus-develop-aarch64-debug/) |
| aarch64 release-valgrind | [![Build Status: master-release-valgrind](http://ms01915-003.hpl.hp.com:8080/job/foedus-master-aarch64-release-valgrind/badge/icon)](http://ms01915-003.hpl.hp.com:8080/job/foedus-master-aarch64-release-valgrind/) | [![Build Status: develop-release-valgrind](http://ms01915-003.hpl.hp.com:8080/job/foedus-develop-aarch64-release-valgrind/badge/icon)](http://ms01915-003.hpl.hp.com:8080/job/foedus-develop-aarch64-release-valgrind/) |

Building Development Environment (For FOEDUS Developers)
--------
We recommend newer Fedora, Ubuntu/Debian, etc.
There are a few things you have to configure with sudo permission.
See the *Environment Setup* section in [foedus-core](foedus-core).

In Fedora/RedHat/CentOS etc, run the following:

    sudo yum install gcc gcc-c++ libstdc* cmake glibc glibc-* valgrind valgrind-devel
    sudo yum install libunwind libunwind-devel
    sudo yum install numactl numactl-devel google-perftools google-perftools-devel
    sudo yum install papi papi-devel papi-static
    sudo yum install python python-*
    sudo yum install doxygen texlive-eps* graphviz mscgen texlive-epspdf sloccount kdevelop

For valgrind, check its version after installation.
If it is not 3.9 or later, we recommend installing a newer one. See the section below.

If you want to generate doxygen-pdf, also run the following:

    sudo yum install texlive texlive-* okular

If you are the person to compile our rpm packages ("make package"), also run the following:

    sudo yum install rpm-build

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
    # You can also use Release/RelWithDebInfo just like usual CMake projects.
    cmake ../  -DCMAKE_BUILD_TYPE=Debug
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
This is why we must use our SPINLOCK_WHILE macro in such places, which occasionally calls
foedus::assorted::spinlock_yield() (not too much to avoid unnecessary overhead, of course).

Even with these yielding, valgrind ~3.8 sometimes causes an infinite or semi-infinite loop
in condition variables, or std::condition_variables::wait()/pthread_cond_wait().
This problem is fixed in valgrind 3.9, and you can see the difference by running
tests-core/src/foedus/assorted/test_raw_atomics on valgrind 3.8.1 (almost always infinite loop)
and valgrind 3.9.0 (always within a few sec).

If you are using an older linux distro (eg Fedora 19 whose latest valgrind in yum repo is 3.8.1),
we strongly recommend to install latest valgrind from source. Follow these steps:

* Download the source from [here](http://valgrind.org/downloads/current.html).
* Usual triplet: "./configure --prefix=$HOME/local; make; make install"
or "./configure --prefix=/usr/local; make; sudo make install" if you are a sudoer and others
on the machine would like it.
* Cleanly rebuild foedus so that our cmake script finds the newer valgrind installation.
(the cmake script searches in this order: ~/local, /usr/local, /usr)
* (Optional) Edit your environment variable to see $HOME/local/bin before /usr/bin.
This is useful when you type "valgrind" in terminal, which might not happen often.

Notes for PAPI on Ivy Bridge (For FOEDUS Developers)
--------
PAPI 5.1 (which is the version on FC19) does not support Ivy Bridge Family 6.
You should source-build the latest PAPI.
Make sure you have gfortran

    sudo yum install gcc-gfortran
    tar -xf papi-5.3.2.tar.gz
    cd papi-5.3.2/src
    ./configure --prefix=$HOME/local; make; make install

Then cleanly build FOEDUS so that it picks up the latest version.


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
We conform to [Google C++ Style Guide](http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml)
except the arguable rule on streams. See [the discussions](http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml#Streams)
if you are interested (Dec 2014: I have realized that the latest cpplint actually disables this
warning as well as allowing most C++11 features. The guide seems thoroughly revised around Sep14.
Great!). Other minor differences from the convention:

* Max 100 characters per line rather than 80 (you are welcomed to keep it within 80, though).
* C++ file names are ".cpp" rather than ".cc", header files are ".hpp" rather than ".h".

We enforce the coding convention by cpplint.py.
All projects run cpplint for every build and report violations as warnings.

In addition to the Google c++ convention, we have the following house-rules:

* cpp/hpp are placed in folders that fully correspond to namespace hierarchy like a Java project.
* Header include order: Same as what Google style defines, but there is something unclear in
the guide; "alphabetical in each category". What cpplint.py enforces is actually "ASCII order".
So, "aaa.hpp" comes before "aaaa.hpp". "aaa_abc.hpp" comes before "aaazabc.hpp".
Also, we place headers under folders in a hierarchically consistent order.
"aaa/a.hpp", "aaa/z.hpp", "aaa/b/foo.hpp", "aaa/b/hoge.hpp", "aaa/c/ccc.hpp" in this order.
This is a bit different from original cpplint.py implementation (we modified the script for this).
* We also force a blank line between categories of headers. So, it should be
Include own-header (hpp with the same path as the cpp file), \<blank line\>,
Include C system headers (eg \<stdint.h\>, \<numa.h\>), \<blank line\>,
Include C++ system headers (eg \<string\>, \<iostream\>) \<blank line\>,
Include other our headers (eg "foedus/memory/engine_memory.hpp").
Notice that we always use angle brackets for system headers and double quotes for our headers.
* No importing or aliasing ("using") of namespace at all, even in c++ files. You might initially
feel this results in lengthy code, but you will soon find it easier to understand others' code and
not requiring additional typing as much as you thought.
* If you are calling classes/methods in global namespace (which shouldn't exist in our code, so
third party's), put "::" as prefix to clarify it's in global namespace (eg "::posix_memalign(foo)").
* Class/function/variable comments must be in [Doxygen format](http://www.stack.nl/~dimitri/doxygen/manual/index.html).
Be beefy.
* Each folder (== package, == namespace) has a header file named "namespace-info.hpp" which gives
Doxygen documentation of the folder, just like "package-info.java" in Java projects.
* Each folder has a header file named "fwd.hpp" which gives forward declarations of classes in
the package. As the Google style guide recommends, prefer forward declarations as much as possible.
* In addition to general C++ coding conventions, there are several foedus-specific programming
idioms. Read our Doxygen document first to get familiar with them (see "FOEDUS Programming Idioms").

kdevelop-specific Recommendations (For FOEDUS Developers)
--------
Only if you use kdevelop, and not if you have your own configuration (which is totally okay).

* Settings, Configure Editor, Appearance, Borders, Enable "Show folding markers", "Show line numbers"
* Settings, Configure Editor, Editing, "Show static word wrap marker" with 100 characters.
* Settings, Configure Editor, Editing, Indentation, "Spaces" 2 characters.
* Settings, Configure Editor, Open/Save, General, Append newline at end of file.

We also have a template file for creating new classes in kdevelop.

Right click a folder, "Create From Template", "Load Template From File", choose
kdevtemplate.desktop under foedus-core/tools.

When you use it, specify your class name such as foedus::storage::masstree::MyNewClass.
Unfortunately, kdevelop template has limited flexibility in a few things:

* Manually edit the generated file name so that words are separated by "\_" (eg my\_new\_class.cpp)
* Manually edit the generated cpp/hpp path so that hpp is under include, cpp is under src.
* Don't let the wizard add new cpp to a target. Most likely it puts it in a stupid place. Add it to a right place yourself.

At least kdevelop up to 4.x didn't like the almost-standard style where .cpp and .h are placed in
separate folders (src and include).

eclipse-specific Recommendations (For FOEDUS Developers)
--------
While eclipse-CDT doesn't support CMake projects as nicely as kdevelop, we know it's a great IDE
in general. If you prefer eclipse, follow the following tips
[recommended here](http://www.nightshadesoftware.org/projects/nightshade/wiki/CMake_and_Eclipse)
 (again, we strongly prefer out-of-source builds, but you need a bit of trick to do it in eclipse):

    # http://www.nightshadesoftware.org/projects/nightshade/wiki/CMake_and_Eclipse
    # Suppose you are at foedus_code.
    cd ..
    mkdir foedus_code_eclipse_build   # build directory root at the same level as the source root
    cd foedus_code_eclipse_build
    cmake ../foedus_code -G"Eclipse CDT4 - Unix Makefiles" -DCMAKE_BUILD_TYPE=Debug


Now start up Eclipse and do the following to import the project.

* File->Import...
* General->Existing Projects into Workspace
* For the root directory enter the build root directory, not the source root
* Leave other options unchecked and click Finish

If you want to *edit* CMakeLists.txt or add new ones, we recommend repeating the same process.
Eclipse sometimes works without it, sometimes not. We let yourself figure out other eclipse
configurations for ctest/cpplint/git/etc and find cool plugins for them.
Good luck, and let us know if there were some gotchas.
You might find [this](https://code.google.com/p/google-styleguide/source/browse/trunk/eclipse-cpp-google-style.xml) helpful.
