FOEDUS Core Library
=================================

Introduction
--------
This project is the gut of FOEDUS as a transactional key-value storage system.
Your client program can use this library by containing the CMakeLists.txt or linking to the shared
library.


Hardware/Compiler Requeirements
--------

* We support only 64-bits CPUs. More specifically, x86_64 and ARMv8.
* We assume Linux/Unix so far. No MacOS, Windows, nor Solaris.
* We assume fsync(2) penetrates all the way through the device. [^1]
* We require reasonably modern C++ compilers.
* We depend on CMake.

[^1]: If this is not the case, check your write-cache settings in the filesystem and device driver.
Unfortunately, even if users configure it right, some storage device sacrifices durability for the
sake of performance. So is some file system. For those environments, we cannot guarantee ACID.


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

    #include <foedus/engine.hpp>
    #include <foedus/engine_options.hpp>
    #include <foedus/epoch.hpp>
    #include <foedus/thread/thread_pool.hpp>
    #include <foedus/thread/thread.hpp>
    #include <foedus/storage/storage_manager.hpp>
    #include <foedus/storage/array/array_storage.hpp>
    #include <foedus/xct/xct_manager.hpp>
    #include <iostream>

    const uint16_t PAYLOAD = 16;
    const uint32_t RECORDS = 1 << 20;
    const char* NAME = "myarray";

    class MyTask : public foedus::thread::ImpersonateTask {
    public:
        foedus::ErrorStack run(foedus::thread::Thread* context) {
            foedus::storage::array::ArrayStorage *array =
                dynamic_cast<foedus::storage::array::ArrayStorage*>(
                    context->get_engine()->get_storage_manager().get_storage(NAME));

            foedus::xct::XctManager& xct_manager = engine->get_xct_manager();
            CHECK_ERROR(xct_manager.begin_xct(context, foedus::xct::SERIALIZABLE));
            char buf[PAYLOAD];
            CHECK_ERROR(array->get_record(context, 123, buf));
            foedus::Epoch commit_epoch;
            CHECK_ERROR(xct_manager.precommit_xct(context, &commit_epoch));
            CHECK_ERROR(xct_manager.wait_for_commit(commit_epoch));
            return foedus::RET_OK;
        }
    };

    int main(int argc, char **argv) {
        foedus::EngineOptions options;
        foedus::Engine engine(options);
        COERCE_ERROR(engine.initialize());
        COERCE_ERROR(engine.get_storage_manager().create_array_impersonate(
            NAME, PAYLOAD, RECORDS, &out));
        MyTask task;
        foedus::thread::ImpersonateSession session = engine.get_thread_pool().impersonate(&task);
        std::cout << "session: result=" << session.get_result() << std::endl;
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

We use none of boost libraries. We might consider using some of the header-only boost libraries,
but we will surely stay away from non-header-only ones (eg filesystem).

Licensing
--------
See [LICENSE.txt](LICENSE.txt) for the license term of libfoedus-core itself.
libfoedus-core uses a few open source libraries listed below.

|    Library   | License |    Linking/Distribution in libfoedus-core    |
|:------------:|---------|----------------------------------------------|
| glog         | BSD     | Static-link. Contains source code.           |
| tinyxml2     | ZLIB    | Static-link. Contains source code.           |
| gperftools   | BSD     | Optional dynamic-link. Distributes nothing.  |
| libnuma      | LGPL    | Dynamic-link. Distributes nothing.           |
| glibc/stdc++ | LGPL    | Dynamic-link. Distributes nothing.           |

For more details, see COPYING/LICENSE.txt/etc in the third-party folder.
