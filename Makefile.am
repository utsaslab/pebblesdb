## Copyright (c) 2013
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
##     * Redistributions of source code must retain the above copyright notice,
##       this list of conditions and the following disclaimer.
##     * Redistributions in binary form must reproduce the above copyright
##       notice, this list of conditions and the following disclaimer in the
##       documentation and/or other materials provided with the distribution.
##     * Neither the name of nb nor the names of its contributors may be used to
##		 endorse or promote products derived from this software without specific
##		 prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
## IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
## ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
## LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
## CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
## SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
## INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
## CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
## POSSIBILITY OF SUCH DAMAGE.

# ACLOCAL_AMFLAGS: specify where the m4 macros are; need to sync with
# `AC_CONFIG_MACRO_DIR` in configure.ac.
# - https://www.gnu.org/software/libtool/manual/html_node/Invoking-libtoolize.html
# - https://stackoverflow.com/questions/16657991/glibtoolize-on-macos-tells-me-to-consider-adding-i-m4-to-aclocal-amflags-but
ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}

# AM_CPPFLAGS: it is a list of arguments to the preprocessor
# https://www.gnu.org/software/automake/manual/html_node/Program-Variables.html
# https://www.gnu.org/software/automake/manual/html_node/Flag-Variables-Ordering.html
# abs_top_srcdir: Absolute name of `top_srcdir`, which is the name of the top-level source code directory for the package,
# which is the name of the directory that contains the source code for that makefile.
# In this case, `abs_top_srcdir` is abs. path to 'HyperPebblesDB' (e.g.
# /Users/zeyuan/Documents/projects/HyperPebblesDB)
# - https://www.gnu.org/software/autoconf/manual/autoconf-2.62/html_node/Preset-Output-Variables.html
AM_CPPFLAGS = -I${abs_top_srcdir}/include

# AM_CFLAGS: This is the variable the Makefile.am author can use to pass in additional C compiler flags
# - https://www.gnu.org/software/automake/manual/html_node/Program-Variables.html
AM_CFLAGS = -DLEVELDB_PLATFORM_POSIX $(SNAPPY_FLAGS) ${EXTRA_CFLAGS} $(WANAL_CFLAGS)

# AM_CXXFLAGS: any flags to pass to the C++ compiler.
# Note: variable names start with 'AM' are the variables for Makefile.am
# - https://www.gnu.org/software/automake/manual/html_node/C_002b_002b-Support.html
AM_CXXFLAGS = -DLEVELDB_PLATFORM_POSIX $(SNAPPY_FLAGS) ${EXTRA_CFLAGS} $(WANAL_CXXFLAGS) -Wno-variadic-macros -std=c++11

# AM_MAKEFLAGS: set this variable if there are flags you must always pass to `make`
# - https://www.gnu.org/software/automake/manual/html_node/Subdirectories.html
AM_MAKEFLAGS = --no-print-directory

# pkgconfigdir: the directory to pkgconfig
# libdir: installation directory for lib/
# - https://www.gnu.org/software/automake/manual/html_node/Standard-Directory-Variables.html
pkgconfigdir = $(libdir)/pkgconfig

# pkgconfig_DATA: the content goes inside 'pkgconfig'
# - https://autotools.io/pkgconfig/file-format.html
pkgconfig_DATA = libpebblesdb.pc

# EXTRA_DIST: sometimes there are files that must be distributed,
# but which are not covered in the automatic rules.
# These files should be listed in the EXTRA_DIST variable
# - https://www.gnu.org/software/automake/manual/html_node/Basics-of-Distribution.html
EXTRA_DIST =
EXTRA_DIST += AUTHORS
EXTRA_DIST += doc/benchmark.html
EXTRA_DIST += doc/doc.css
EXTRA_DIST += doc/impl.html
EXTRA_DIST += doc/index.html
EXTRA_DIST += doc/log_format.txt
EXTRA_DIST += doc/table_format.txt
EXTRA_DIST += helpers/memenv/memenv.cc
EXTRA_DIST += helpers/memenv/memenv_test.cc
EXTRA_DIST += LICENSE
EXTRA_DIST += NEWS
EXTRA_DIST += port/README
EXTRA_DIST += README
EXTRA_DIST += TODO

# pkginclude_HEADERS: the header files included in the installation of the package
# (e.g. the following header files reside in /usr/local/include/pebblesdb; /usr/local
# is the default path `prefix` variable); 
# headers to install in system-wide includedir/yourprogram subdir
# - https://www.gnu.org/software/automake/manual/html_node/Basics-of-Distribution.html
# - https://realmike.org/blog/2010/07/18/gnu-automake-by-example/
# - https://modelingwithdata.org/pdfs/111-tip-automake.pdf
pkginclude_HEADERS =
pkginclude_HEADERS += include/pebblesdb/cache.h
pkginclude_HEADERS += include/pebblesdb/c.h
pkginclude_HEADERS += include/pebblesdb/comparator.h
pkginclude_HEADERS += include/pebblesdb/db.h
pkginclude_HEADERS += include/pebblesdb/env.h
pkginclude_HEADERS += include/pebblesdb/filter_policy.h
pkginclude_HEADERS += include/pebblesdb/iterator.h
pkginclude_HEADERS += include/pebblesdb/options.h
pkginclude_HEADERS += include/pebblesdb/slice.h
pkginclude_HEADERS += include/pebblesdb/replay_iterator.h
pkginclude_HEADERS += include/pebblesdb/status.h
pkginclude_HEADERS += include/pebblesdb/table_builder.h
pkginclude_HEADERS += include/pebblesdb/table.h
pkginclude_HEADERS += include/pebblesdb/write_batch.h
pkginclude_HEADERS += third-party/SuRF/include/surf.hpp

# noinst_HEADERS: the header files that will *NOT* be installed
# - ftp://ftp.gnu.org/old-gnu/Manuals/automake/html_node/automake_64.html
# - https://modelingwithdata.org/pdfs/111-tip-automake.pdf
noinst_HEADERS =
noinst_HEADERS += db/builder.h
noinst_HEADERS += db/dbformat.h
noinst_HEADERS += db/db_impl.h
noinst_HEADERS += db/murmurhash3.h
noinst_HEADERS += db/db_iter.h
noinst_HEADERS += db/filename.h
noinst_HEADERS += db/log_format.h
noinst_HEADERS += db/log_reader.h
noinst_HEADERS += db/log_writer.h
noinst_HEADERS += db/memtable.h
noinst_HEADERS += db/skiplist.h
noinst_HEADERS += db/replay_iterator.h
noinst_HEADERS += db/snapshot.h
noinst_HEADERS += db/table_cache.h
noinst_HEADERS += db/version_edit.h
noinst_HEADERS += db/version_set.h
noinst_HEADERS += db/write_batch_internal.h
noinst_HEADERS += helpers/memenv/memenv.h
noinst_HEADERS += port/atomic_pointer.h
noinst_HEADERS += port/port_example.h
noinst_HEADERS += port/port.h
noinst_HEADERS += port/port_posix.h
noinst_HEADERS += port/thread_annotations.h
noinst_HEADERS += port/win/stdint.h
noinst_HEADERS += table/block_builder.h
noinst_HEADERS += table/block.h
noinst_HEADERS += table/filter_block.h
noinst_HEADERS += table/format.h
noinst_HEADERS += table/iterator_wrapper.h
noinst_HEADERS += table/merger.h
noinst_HEADERS += table/two_level_iterator.h
noinst_HEADERS += util/arena.h
noinst_HEADERS += util/atomic.h
noinst_HEADERS += util/coding.h
noinst_HEADERS += util/crc32c.h
noinst_HEADERS += util/hash.h
noinst_HEADERS += util/histogram.h
noinst_HEADERS += util/logging.h
noinst_HEADERS += util/mutexlock.h
noinst_HEADERS += util/posix_logger.h
noinst_HEADERS += util/random.h
noinst_HEADERS += util/string_builder.h
noinst_HEADERS += util/testharness.h
noinst_HEADERS += util/testutil.h

# lib_LTLIBRARIES: the list of libtool libraries to build; .la represents 'libtool archive',
# which a libtool archive to all the shared libraries format on differet platforms
# - https://www.gnu.org/software/automake/manual/html_node/Libtool-Libraries.html
# - https://www.lrde.epita.fr/~adl/dl/autotools.pdf (Slide p.116)
lib_LTLIBRARIES = libpebblesdb.la

# specify what source files should be included when build the
# shared library 'libpebblesdb.la'
libpebblesdb_la_SOURCES =
libpebblesdb_la_SOURCES += db/builder.cc
libpebblesdb_la_SOURCES += db/c.cc
libpebblesdb_la_SOURCES += db/dbformat.cc
libpebblesdb_la_SOURCES += db/db_impl.cc
libpebblesdb_la_SOURCES += db/db_iter.cc
libpebblesdb_la_SOURCES += db/filename.cc
libpebblesdb_la_SOURCES += db/log_reader.cc
libpebblesdb_la_SOURCES += db/log_writer.cc
libpebblesdb_la_SOURCES += db/memtable.cc
libpebblesdb_la_SOURCES += db/murmurhash3.cc
libpebblesdb_la_SOURCES += db/repair.cc
libpebblesdb_la_SOURCES += db/replay_iterator.cc
libpebblesdb_la_SOURCES += db/table_cache.cc
libpebblesdb_la_SOURCES += db/version_edit.cc
libpebblesdb_la_SOURCES += db/version_set.cc
libpebblesdb_la_SOURCES += db/write_batch.cc
libpebblesdb_la_SOURCES += table/block_builder.cc
libpebblesdb_la_SOURCES += table/block.cc
libpebblesdb_la_SOURCES += table/filter_block.cc
libpebblesdb_la_SOURCES += table/format.cc
libpebblesdb_la_SOURCES += table/iterator.cc
libpebblesdb_la_SOURCES += table/merger.cc
libpebblesdb_la_SOURCES += table/table_builder.cc
libpebblesdb_la_SOURCES += table/table.cc
libpebblesdb_la_SOURCES += table/two_level_iterator.cc
libpebblesdb_la_SOURCES += util/arena.cc
libpebblesdb_la_SOURCES += util/atomic.cc
libpebblesdb_la_SOURCES += util/bloom.cc
libpebblesdb_la_SOURCES += util/surf.cc
libpebblesdb_la_SOURCES += util/cache.cc
libpebblesdb_la_SOURCES += util/coding.cc
libpebblesdb_la_SOURCES += util/comparator.cc
libpebblesdb_la_SOURCES += util/crc32c.cc
libpebblesdb_la_SOURCES += util/env.cc
libpebblesdb_la_SOURCES += util/env_posix.cc
libpebblesdb_la_SOURCES += util/filter_policy.cc
libpebblesdb_la_SOURCES += util/hash.cc
libpebblesdb_la_SOURCES += util/histogram.cc
libpebblesdb_la_SOURCES += util/logging.cc
libpebblesdb_la_SOURCES += util/options.cc
libpebblesdb_la_SOURCES += util/status.cc
libpebblesdb_la_SOURCES += port/port_posix.cc

# _LIBADD: add the dependency library. In this case, `libpebblesdb.la`
# depends on pthread and possibly the snappy library. We include them
# when we build `libpebblesdb.la`.
# - https://stackoverflow.com/questions/23685981/what-is-the-difference-between-ldadd-and-libadd
libpebblesdb_la_LIBADD = $(SNAPPY_LIBS) -lpthread -lsnappy #TODO: -lsnappy should be removed because we explicitly set the variable `SNAPPY_LIBS` in configure.ac

# _LDFLAGS: Specify the linker flags for `libpebblesdb.la`
# - https://www.gnu.org/software/automake/manual/html_node/Flag-Variables-Ordering.html
libpebblesdb_la_LDFLAGS = -lpthread -lsnappy $(AM_LDFLAGS) $(LDFLAGS) #TODO: -lsnappy should be removed for the same reasoning; if there are test cases associated with snappy library, we should include those test cases only when snappy library is present

# We declare two variables as representative of test-related source files, which
# will be used later in this file
TESTUTIL = util/testutil.cc
TESTHARNESS = util/testharness.cc $(TESTUTIL)

# noinst_PROGRAMS: a program or programs build in the local directory that should be compiled, linked but not installed
# - http://www.javahotchocolate.com/notes/makefile.html
noinst_PROGRAMS =
noinst_PROGRAMS += db_bench
noinst_PROGRAMS += leveldbutil
noinst_PROGRAMS += leveldb-verify

EXTRA_PROGRAMS =
EXTRA_PROGRAMS += benchmark
EXTRA_PROGRAMS += leveldb-dump-all
EXTRA_PROGRAMS += db_bench_sqlite3
EXTRA_PROGRAMS += db_bench_tree_db

# check_PROGRAMS: Lists the programs that are needed to run `make check`
# - http://www.adp-gmbh.ch/misc/tools/configure/index.html
# `make check`: https://stackoverflow.com/questions/1735473/what-does-make-check-do
check_PROGRAMS =
check_PROGRAMS += autocompact_test
check_PROGRAMS += arena_test
check_PROGRAMS += bloom_test
check_PROGRAMS += surf_test
check_PROGRAMS += c_test
check_PROGRAMS += cache_test
check_PROGRAMS += coding_test
check_PROGRAMS += corruption_test
check_PROGRAMS += crc32c_test
check_PROGRAMS += db_test
check_PROGRAMS += dbformat_test
check_PROGRAMS += env_test
check_PROGRAMS += filename_test
check_PROGRAMS += filter_block_test
check_PROGRAMS += log_test
check_PROGRAMS += skiplist_test
check_PROGRAMS += table_test
check_PROGRAMS += version_edit_test
check_PROGRAMS += version_set_test
check_PROGRAMS += write_batch_test
check_PROGRAMS += issue178_test
check_PROGRAMS += issue200_test
check_PROGRAMS += testPebblesDB

# TESTS: Lists the programs that are run `make check`.
# Because those programs sometimes need to be built themselves, usually one wants to write
# the line below.
TESTS = $(check_PROGRAMS)

# Specify the source files, the dependency libraries, and the link flags for the
# `benchmark` program, which specified in the `EXTRA_PROGRAMS` above.
benchmark_SOURCES = benchmark.cc
benchmark_LDADD = libpebblesdb.la -lpthread -lsnappy -le -lpopt -lygor
benchmark_LDFLAGS = -no-install

# similar as above. `db_bench` program this time instead.
db_bench_SOURCES = db/db_bench.cc $(TESTUTIL)
db_bench_LDADD = libpebblesdb.la -lpthread -lsnappy

# similar as above. `db_bench_sqlite3` program this time instead.
db_bench_sqlite3_SOURCES = doc/bench/db_bench_sqlite3.cc $(TESTUTIL)
db_bench_sqlite3_LDADD = -lsqlite3

# similar as above. `db_bench_tree_db` program this time instead.
db_bench_tree_db_SOURCES = doc/bench/db_bench_tree_db.cc $(TESTUTIL)
db_bench_tree_db_LDADD = -lkyotocabinet

# similar as above. `leveldbutil` program this time instead.
leveldbutil_SOURCES = db/leveldb_main.cc
leveldbutil_LDADD = libpebblesdb.la -lpthread -lsnappy

# similar as above. `leveldb_dump_all` program this time instead.
leveldb_dump_all_SOURCES = leveldb-dump-all.cc
leveldb_dump_all_LDADD = libpebblesdb.la -lpthread

# similar as above. `leveldb_verify` program this time instead.
leveldb_verify_SOURCES = leveldb-verify.cc
leveldb_verify_LDADD = libpebblesdb.la -lpthread

# Now we specify the source files and dependency library for the test
# programs specified in `check_PROGRAMS`
autocompact_test_SOURCES = db/autocompact_test.cc $(TESTHARNESS)
autocompact_test_LDADD = libpebblesdb.la -lpthread

arena_test_SOURCES = util/arena_test.cc $(TESTHARNESS)
arena_test_LDADD = libpebblesdb.la -lpthread

bloom_test_SOURCES = util/bloom_test.cc $(TESTHARNESS)
bloom_test_LDADD = libpebblesdb.la -lpthread

surf_test_SOURCES = util/surf_test.cc $(TESTHARNESS)
surf_test_LDADD = libpebblesdb.la -lpthread

c_test_SOURCES = db/c_test.c $(TESTHARNESS)
c_test_LDADD = libpebblesdb.la -lpthread

cache_test_SOURCES = util/cache_test.cc $(TESTHARNESS)
cache_test_LDADD = libpebblesdb.la -lpthread

coding_test_SOURCES = util/coding_test.cc $(TESTHARNESS)
coding_test_LDADD = libpebblesdb.la -lpthread

corruption_test_SOURCES = db/corruption_test.cc $(TESTHARNESS)
corruption_test_LDADD = libpebblesdb.la -lpthread

crc32c_test_SOURCES = util/crc32c_test.cc $(TESTHARNESS)
crc32c_test_LDADD = libpebblesdb.la -lpthread

db_test_SOURCES = db/db_test.cc $(TESTHARNESS)
db_test_LDADD = libpebblesdb.la -lpthread

dbformat_test_SOURCES = db/dbformat_test.cc $(TESTHARNESS)
dbformat_test_LDADD = libpebblesdb.la -lpthread

env_test_SOURCES = util/env_test.cc $(TESTHARNESS)
env_test_LDADD = libpebblesdb.la -lpthread

filename_test_SOURCES = db/filename_test.cc $(TESTHARNESS)
filename_test_LDADD = libpebblesdb.la -lpthread

filter_block_test_SOURCES = table/filter_block_test.cc $(TESTHARNESS)
filter_block_test_LDADD = libpebblesdb.la -lpthread

log_test_SOURCES = db/log_test.cc $(TESTHARNESS)
log_test_LDADD = libpebblesdb.la -lpthread

table_test_SOURCES = table/table_test.cc $(TESTHARNESS)
table_test_LDADD = libpebblesdb.la -lpthread

skiplist_test_SOURCES = db/skiplist_test.cc $(TESTHARNESS)
skiplist_test_LDADD = libpebblesdb.la -lpthread

version_edit_test_SOURCES = db/version_edit_test.cc $(TESTHARNESS)
version_edit_test_LDADD = libpebblesdb.la -lpthread

version_set_test_SOURCES = db/version_set_test.cc $(TESTHARNESS)
version_set_test_LDADD = libpebblesdb.la -lpthread

write_batch_test_SOURCES = db/write_batch_test.cc $(TESTHARNESS)
write_batch_test_LDADD = libpebblesdb.la -lpthread

issue178_test_SOURCES = issues/issue178_test.cc $(TESTHARNESS)
issue178_test_LDADD = libpebblesdb.la -lpthread

issue200_test_SOURCES = issues/issue200_test.cc $(TESTHARNESS)
issue200_test_LDADD = libpebblesdb.la -lpthread

testPebblesDB_SOURCES = learning/testPebblesDB.cc
testPebblesDB_LDADD = libpebblesdb.la -lpthread

