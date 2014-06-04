[Super-build] FOEDUS: Fast Optimistic Engine for Data Unification Services
=================================

Overview
--------
FOEDUS is a new transactional key-value library developped at Hewlett-Packard Labs that is optimized
for a large number of CPU cores and NVRAM storage. It is a handy C++ library you can
either include in your source code (by invoking CMake script) or dynamically link to.
In a nutshell, it is something like BerkeleyDB, but it is much more efficient on new hardware.

Folder Structure
--------
This root project contains a few sub-projects.
Some of them are *NOT* supposed to be directly linked from client programs (_your_ programs).

* foedus-core : Key-value store library.
* foedus-util : A series of utility programs to help use libfoedus.
* foedus-rdb : Relational database engine on top of foedus-core.
* tests-[core/util/rdb] : Unit testcase projects.
* experiments-[core/util/rdb] : Performance experiments projects.
* third\_party : Third party source code used in our programs.

You are supposed to link only to *foedus-core* and *foedus-rdb*.
Other projects are for internal use or to provide executables, rather than libraries.
You can still contain all projects (or this folder's CMakeLists.txt) in your source code,
but note that some restrictions on compiler options apply if you do so.

libfoedus-core
-----------
For more details of how your client program links to our library, API document, Licensing, etc,
see [foedus-core](foedus-core/README.markdown)

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

If you find a false positive or third party's bug, add them to foedus-core/tools/valgrind.supp.

    valgrind --leak-check=full --show-leak-kinds=all --suppressions=<path_to_valgrind.supp> --gen-suppressions=all ./<your_program>

For more details, check out CTEST/CMAKE documentation.

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
