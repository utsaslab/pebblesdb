# Copyright (c) 2013
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperLevelDB nor the names of its contributors may
#       be used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

# AC_PREREQ: Ensure that a recent enough version of Autoconf is being used.
AC_PREREQ([2.62])

# mandatory initialization step
# https://www.gnu.org/savannah-checkouts/gnu/autoconf/manual/autoconf-2.69/html_node/Initializing-configure.html
# syntax: AC_INIT (package, version, [bug-report], [tarname], [url])
AC_INIT([pebblesdb], [1.0.dev], [vijay@cs.utexas.edu])

# m4: https://en.wikipedia.org/wiki/M4_(computer_language)
# https://www.gnu.org/software/m4/manual/m4.html
# m4_define: define macro in m4 language
# m4_esyscmd: this macro expands to the result of running command in a shell
# https://www.gnu.org/software/autoconf/manual/autoconf-2.68/html_node/Redefined-M4-Macros.html
m4_define([serial_tests], [
    m4_esyscmd([case `automake --version | head -n 1` in
                *1.11*);;
                *) echo serial-tests;;
                esac])
])

# `AM_INIT_AUTOMAKE`: Runs many macros required for proper operation of the generated Makefiles.
# Option list: https://www.gnu.org/software/automake/manual/html_node/List-of-Automake-options.html#List-of-Automake-options
# - The `foreign` option tells Automake that this package will not follow the GNU Standards
# - The `serial-tests`: Enable the older serial test suite harness for TESTS
# - The `subdir-objects`: objects are placed into the subdirectory of the
#   build directory corresponding to the subdirectory of the source file.
#   For instance, if the source file is subdir/file.cxx, then the output file would be subdir/file.o.
# - The `dist-bzip2`: Hook dist-bzip2 to dist. `dist` is related to package distribution.
AM_INIT_AUTOMAKE(foreign serial_tests subdir-objects dist-bzip2)

# `m4_ifdef` check the existence of a macro (e.g. AM_SILENT_RULES):
# syntax: m4_ifdef([macro], [if-defined], [if-not-defined])
# AM_SILENT_RULES: silent rules <- make the `make` process less verbose
# https://autotools.io/automake/silent.html
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

# Specify the Libtool version
# https://www.gnu.org/software/libtool/manual/html_node/LT_005fINIT.html
LT_PREREQ([2.2])

# initialize Libtool, which a wrapper that wrapps around different
# shared library format with standard extension: libfoo.la. Libtool
# will take care of translating and linking the shared libraries specific
# to the target platform
# https://www.lrde.epita.fr/~adl/dl/autotools.pdf (slide 115 -)
LT_INIT

# AC_CONFIG_SRCDIR: sanity check that the specified file actually exists
# Usually, people put to be distributed source file here. In this case,
# we include `db.h` so that user can use db itself in their program
# - https://www.gnu.org/savannah-checkouts/gnu/autoconf/manual/autoconf-2.69/html_node/Input.html
# - https://renenyffenegger.ch/notes/development/GNU/toolchain/Build-System/Autoconf/macros/AC_CONFIG_SRCDIR
# - http://freesoftwaremagazine.com/articles/configuring_a_project_with_autoconf/
# - https://eklitzke.org/how-to-autotools
AC_CONFIG_SRCDIR([include/pebblesdb/db.h])

# AC_CONFIG_HEADERS: automatically generate the head file: config.h from
# config.h.in
# https://www.gnu.org/software/autoconf/manual/autoconf-2.66/html_node/Configuration-Headers.html
AC_CONFIG_HEADERS([config.h])

# AC_CONFIG_MACRO_DIR: decalre where additional macro files are to be put and found
# https://autotools.io/autoconf/macros.html
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
# AC_PROG_CXX: This is required if any C++ source is included
AC_PROG_CXX

# AC_PROG_CC: causes the configure script to search for a C compiler and define the variable `CC` with its name
# https://www.gnu.org/software/automake/manual/html_node/amhello_0027s-configure_002eac-Setup-Explained.html
AC_PROG_CC

# AC_LANG(C++): Do compilation tests using CXX and CXXCPP and use extension .C for test programs.
# Use compilation flags: CPPFLAGS with CXXCPP, and both CPPFLAGS and CXXFLAGS with CXX.
# https://www.gnu.org/software/autoconf/manual/autoconf-2.63/html_node/Language-Choice.html
AC_LANG(C++)

# A macro defined inside `m4/anal_warnings.m4`, which
# enables many compiler warnings for C++ that generally catch bugs in
# code.
ANAL_WARNINGS

# EXTRA_CFLAGS: User can provide extra command line flags through this variable
# https://stackoverflow.com/questions/27862627/what-is-the-purpose-of-extra-cflags
EXTRA_CFLAGS=""

# AX_CHECK_COMPILE_FLAG: A macro defined inside `m4/ax_check_compile_flag.m4`
# check whether `-fno-builtin-memcmp` works with the current compiler, if so
# we add `EXTRA_CFLAGS="-fno-builtin-memcmp"` as extra flags to the compiler
AX_CHECK_COMPILE_FLAG([-fno-builtin-memcmp],[EXTRA_CFLAGS="-fno-builtin-memcmp"],,)
AX_CHECK_COMPILE_FLAG([-fno-builtin-memmove],[EXTRA_CFLAGS="${EXTRA_CFLAGS} -fno-builtin-memcmp"],,)
AX_CHECK_COMPILE_FLAG([-Qunused-arguments],[EXTRA_CFLAGS="${EXTRA_CFLAGS} -Qunused-arguments"],,)

# AC_SUBST: write a variable into the Makefile. In this case, Makefile will have
# variable `EXTRA_CFLAGS` with value `${EXTRA_CFLAGS}`.
# https://renenyffenegger.ch/notes/development/GNU/toolchain/Build-System/Autoconf/macros/AC_SUBST
AC_SUBST([EXTRA_CFLAGS], [${EXTRA_CFLAGS}])

# Checks for libraries.

# Checks for header files.
# AC_CHECK_HEADERS: check the existence of header files. Usually, we want to use some C/C++ library
# in the program. It is necessary to check the existence of those library header files.
# https://autotools.io/autoconf/finding.html
AC_CHECK_HEADERS([endian.h machine/endian.h sys/endian.h sys/isa_defs.h sys/types.h])

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.
# AC_FUNC_ERROR_AT_LINE: check the existence of function `error_at_line`
# https://www.gnu.org/software/autoconf/manual/autoconf-2.60/html_node/Particular-Functions.html
# https://linux.die.net/man/3/error_at_line
AC_FUNC_ERROR_AT_LINE

# AC_FUNC_MMAP: If the `mmap` function exists and works correctly, define HAVE_MMAP. 
# This checks only private fixed mapping of already-mapped memory.
# https://www.gnu.org/software/autoconf/manual/autoconf-2.60/html_node/Particular-Functions.html
AC_FUNC_MMAP

# AC_CHECK_FUNCS: check the existence of functions. If exits, define HAVE_function (e.g. HAVE_ALARM)
# https://www.gnu.org/software/autoconf/manual/autoconf-2.67/html_node/Generic-Functions.html
AC_CHECK_FUNCS([alarm clock_gettime mach_absolute_time ftruncate fsync])
AC_CHECK_FUNCS([fread fread_unlocked fwrite fwrite_unlocked fflush])
AC_CHECK_FUNCS([fflush_unlocked memmove mkdir munmap rmdir socket])

# AC_CHECK_DECLS: check the existence of symbols. If exists, 
# define HAVE_DECL_symbol (in all capitals) to 1 if symbol is declared, otherwise to 0.
# ftp://ftp.gnu.org/old-gnu/Manuals/autoconf-2.53/html_node/Generic-Declarations.html
AC_CHECK_DECLS([fdatasync])

# Optional components
snappy_detect_hdr=yes
snappy_detect_lib=yes

# AC_CHECK_LIB: Test whether the library *library* is available by trying to
# link a test program that calls function *function* with the library.
# *function* should be a function provided by the library.
# syntax: AC_CHECK_LIB (library, function, [action-if-found], [action-if-not-found], [other-libraries])
# In this case, we check existence of library `snappy` by asking the test program call
# the function `snappy_compress`. If the function exists, we set `snappy_detect_hdr=yes`
# otherwise, we set `snappy_detect_hdr=no`.
# https://www.gnu.org/savannah-checkouts/gnu/autoconf/manual/autoconf-2.69/html_node/Libraries.html
AC_CHECK_LIB([snappy], [snappy_compress], [snappy_detect_hdr=yes], [snappy_detect_hdr=no])

# syntax:  AC_CHECK_HEADERS (header-file..., [action-if-found], [action-if-not-found], [includes = `AC_INCLUDES_DEFAULT'])
# In this case, if we cannot find `snappy.h`, we set `snappy_detect_lib=no`
AC_CHECK_HEADER([snappy.h],,[snappy_detect_lib=no])

# syntax: AC_ARG_ENABLE (feature, help-string, [action-if-given], [action-if-not-given])
# If the user gave configure the option `--enable-feature` or `--disable-feature`,
# run shell commands action-if-given. If neither option was given, run shell commands action-if-not-given.
AC_ARG_ENABLE([snappy], [AS_HELP_STRING([--enable-snappy],
              [build with Snappy @<:@default: auto@:>@])],
              [snappy=${enableval}], [snappy=auto])

# check the value of variable "snappy":
# https://superuser.com/questions/935374/difference-between-and-in-shell-script
# https://stackoverflow.com/questions/174119/why-do-shell-script-comparisons-often-use-xvar-xyes
if test x"${snappy}" = xno; then
snappy_detect_hdr=no
snappy_detect_lib=no
fi
if test x"${snappy}" = xyes; then
    if test x"${snappy_detect_hdr}" != xyes; then
        AC_MSG_ERROR([
-------------------------------------------------
LevelDB configured with the Snappy library.
libsnappy.so not found
Please install Snappy to continue.
-------------------------------------------------])
    fi
    if test x"${snappy_detect_lib}" != xyes; then
        AC_MSG_ERROR([
-------------------------------------------------
LevelDB configured with the Snappy library.
snappy.h not found
Please install Snappy to continue.
-------------------------------------------------])
    fi
fi
if test x"${snappy_detect_hdr}" = xyes -a x"${snappy_detect_lib}" = xyes; then
SNAPPY_FLAGS=-DSNAPPY
SNAPPY_LIBS=-lsnappy
else
SNAPPY_FLAGS=
SNAPPY_LIBS=
fi

AC_SUBST(SNAPPY_FLAGS)
AC_SUBST(SNAPPY_LIBS)

# AC_CONFIG_FILES: Make AC_OUTPUT create each file by copying an input file
# (by default file.in), substituting the output variable values. In this case,
# we create `Makefile` by copying `Makefile.in`, and `libpebblesdb.pc` by
# `libpebblesdb.pc.in` and substitutes the output variable values inside .in files.
# AC_OUTPUT: generates files that are required for building the packages
# https://www.gnu.org/savannah-checkouts/gnu/autoconf/manual/autoconf-2.69/html_node/Configuration-Files.html
# http://www.shlomifish.org/lecture/Autotools/slides/common_macros/AC_OUTPUT.html
AC_CONFIG_FILES([Makefile libpebblesdb.pc])

# Disable Debug by define NDEBUG preprocessor
# Use it when you run db_bench
AC_DEFINE([NDEBUG], [1], ['Disable debug'])

# Enable TIMER_LOG when you want to have a detailed
# statistics of operations inside micro-benchmark
AC_DEFINE([TIMER_LOG], [0], ['Start timer log'])

# AC_CANONICAL_HOST is needed to access the 'host_os' variable   
# https://stackoverflow.com/questions/38898591/how-to-check-the-os-with-automake 
AC_CANONICAL_HOST

build_linux=no
build_windows=no
build_mac=no

# Detect the target system
case "${host_os}" in
    linux*)
        build_linux=yes
        ;;
    cygwin*|mingw*)
        build_windows=yes
        ;;
    darwin*)
        build_mac=yes
        AC_DEFINE([__APPLE__], [1], ['Disable Mac-incompatible trace function'])
        ;;
    *)
        AC_MSG_ERROR(["OS $host_os is not supported"])
        ;;
esac

# Pass the conditionals to automake
AM_CONDITIONAL([LINUX], [test "$build_linux" = "yes"])
AM_CONDITIONAL([WINDOWS], [test "$build_windows" = "yes"])
AM_CONDITIONAL([OSX], [test "$build_mac" = "yes"])

# ftp://ftp.gnu.org/old-gnu/Manuals/autoconf-2.13/html_chapter/autoconf_3.html
AC_OUTPUT
