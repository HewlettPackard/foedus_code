#ifndef GLOG_CONFIG_H_
#define GLOG_CONFIG_H_

/* Generated from config.h.cmake during build configuration using CMake. */

// Note: This header file is only used internally. It is not part of public interface!

// ---------------------------------------------------------------------------
// TODOs

// TODO: WHAT's THIS???
#cmakedefine HAVE_STACKTRACE

// TODO: WHAT's THIS???
#cmakedefine HAVE_SYMBOLIZE

// ---------------------------------------------------------------------------
// System checks

// Define if you build this library for a MS Windows OS.
#cmakedefine OS_WINDOWS

// Define if you have dladdr().
#cmakedefine HAVE_DLADDR

// Define if you have execinfo.h.
#cmakedefine HAVE_EXECINFO_H

// Define if you have fcntl().
#cmakedefine HAVE_FCNTL

// Define if you have glob.h.
#cmakedefine HAVE_GLOB_H

// Define if you have gmock library.
#cmakedefine HAVE_LIB_GMOCK

// Define if you have gtest library.
#cmakedefine HAVE_LIB_GTEST

// Define if you have the gflags library.
#cmakedefine HAVE_LIB_GFLAGS

// Define if you have the <gflags/gflags.h> header file.
#cmakedefine HAVE_GFLAGS_H

// Define if you have the <inttypes.h> header file.
#cmakedefine HAVE_INTTYPES_H

// Define if you have the <pthread.h> header file.
#cmakedefine HAVE_PTHREAD

// Define if you have the <pwd.h> header file.
#cmakedefine HAVE_PWD_H

// Define if your pthread library defines the type pthread_rwlock_t
#cmakedefine HAVE_RWLOCK

// Define if you have the <shlwapi.h> header file (Windows 2000/XP).
#cmakedefine HAVE_SHLWAPI_H

// Define if you have sigaltstack().
#cmakedefine HAVE_SIGALTSTACK

// Define if you have the <stdint.h> header file.
#cmakedefine HAVE_STDINT_H

// Define if you have the <syscall.h> header file.
#cmakedefine HAVE_SYSCALL_H

// Define if you have the <syslog.h> header file.
#cmakedefine HAVE_SYSLOG_H

// Define if you have the <sys/stat.h> header file.
#cmakedefine HAVE_SYS_STAT_H

// Define if you have the <sys/syscall.h> header file.
#cmakedefine HAVE_SYS_SYSCALL_H

// Define if you have the <sys/types.h> header file.
#cmakedefine HAVE_SYS_TYPES_H

// Define if you have the <sys/time.h> header file.
#cmakedefine HAVE_SYS_TIME_H

// Define if you have sys/ucontext.h.
#cmakedefine HAVE_SYS_UCONTEXT_H

// Define if you have the <sys/utsname.h> header file.
#cmakedefine HAVE_SYS_UTSNAME_H

// Define if you have ucontext.h.
#cmakedefine HAVE_UCONTEXT_H

// Define if you have the <unistd.h> header file.
#cmakedefine HAVE_UNISTD_H

// Define if you have unwind.h.
#cmakedefine HAVE_UNWIND_H

// Define if you have libunwind.
#cmakedefine HAVE_LIB_UNWIND

// ---------------------------------------------------------------------------
// Type checks

// Define if you have the __uint16 builtin type.
#cmakedefine HAVE___UINT16

// Define if you have the u_int16_t builtin type.
#cmakedefine HAVE_U_INT16_T

// Define if you have the uint16_t builtin type.
#cmakedefine HAVE_UINT16_T

// ---------------------------------------------------------------------------
// Compiler checks

// Define if you have __sync_val_compare_and_swap()
#cmakedefine HAVE___SYNC_VAL_COMPARE_AND_SWAP

// Define if the compiler supports using ::operator<<
#cmakedefine HAVE_USING_OPERATOR

#if __GNUC__
#define HAVE__BUILTIN_EXPECT
#define HAVE___ATTRIBUTE__
#define HAVE_ALWAYS_INLINE
#define HAVE_ATTRIBUTE_NOINLINE
#define ATTRIBUTE_NORETURN __attribute__ ((noreturn))
#define ATTRIBUTE_NOINLINE __attribute__ ((noinline))
#define ATTRIBUTE_PRINTF_4_5 __attribute__((__format__ (__printf__, 4, 5)))
#else  // __GNUC__
#undef HAVE__BUILTIN_EXPECT
#undef HAVE___ATTRIBUTE__
#undef HAVE_ALWAYS_INLINE
#undef HAVE_ATTRIBUTE_NOINLINE
#define ATTRIBUTE_NORETURN
#define ATTRIBUTE_NOINLINE
#define ATTRIBUTE_PRINTF_4_5
#endif  // __GNUC__

// ---------------------------------------------------------------------------
// Package information

// Name of package.
#define PACKAGE @PROJECT_NAME@

// Define to the full name of this package.
#define PACKAGE_NAME @PACKAGE_NAME@

// Define to the full name and version of this package.
#define PACKAGE_STRING @PACKAGE_STRING@

// Define to the one symbol short name of this package.
#define PACKAGE_TARNAME @PACKAGE_TARNAME@

// Define to the version of this package.
#define PACKAGE_VERSION @PACKAGE_VERSION@

// Version number of package.
#define VERSION PACKAGE_VERSION

// Define to the address where bug reports for this package should be sent.
#define PACKAGE_BUGREPORT @PACKAGE_BUGREPORT@

// Namespace of glog library symbols.
#define GOOGLE_NAMESPACE @GOOGLE_NAMESPACE@

/* Stops putting the code inside the Google namespace */
#define _END_GOOGLE_NAMESPACE_ }

/* Puts following code inside the Google namespace */
#define _START_GOOGLE_NAMESPACE_ namespace @GOOGLE_NAMESPACE@ {

#endif  // GLOG_CONFIG_H_
