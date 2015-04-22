This folder contains xxHash, a super-fast C/C++ hasing library published under BSD license:
  https://github.com/Cyan4973/xxHash/

Below are the changes we (HP) made.

* Removed xxhsum.c, which seems under GPL.
* Added CMakeLists.txt, which exports "xxhash" as shared library and "xxhashstatic" as static library (with BUILD_STATIC_LIBS ON).

