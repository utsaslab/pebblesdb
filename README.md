## HyperPebblesDB 

[![Build Status](https://travis-ci.org/xxks-kkk/HyperPebblesDB.svg?branch=master)](https://travis-ci.org/xxks-kkk/HyperPebblesDB)

This is a fork of [PebblesDB](https://github.com/utsaslab/pebblesdb), a write-optimized key-value store built with FLSM (Fragmented Log-Structured Merge Tree) data structure. FLSM is a modification of the standard log-structured merge tree data structure which
aims at achieving higher write throughput and lower write amplification without compromising on read throughput.

HyperPebblesDB is part of [LevelDB](https://github.com/google/leveldb) family, and thus it is API-compatible with LevelDB and 
[HyperLevelDB](https://github.com/rescrv/HyperLevelDB). Please read [full paper on PebblesDB](http://www.cs.utexas.edu/~vijay/papers/sosp17-pebblesdb.pdf) and corresponding [slides](http://www.cs.utexas.edu/~vijay/papers/pebblesdb-sosp17-slides.pdf) for further details.

### Features not in PebblesDB

- Continuous code integration using Travis-CI
- New `emptyGuards` option in `db_bench` that gives detailed SSTables distribution under each guard across all levels
- `db_bench` enables "fillseq" and "fillrandom" even when using existing database
- `db_bench` support "ycsb" option by reading a trace file generated from the YCSB benchmark (check "scripts/trace_gen.sh" for details)
- Support guard-based parallel compaction, which can be turn on/off with `options.parallel_guard_compaction`
- Support Succinct Range Filter (SuRF) as an alternative to bloom filter. Check [SuRF paper](http://www.cs.cmu.edu/~huanche1/publications/surf_paper.pdf) for details
- Better internal documentation
- Verified portability on both Mac & Linux using g++-5

### Dependencies

HyperPebblesDB is fully-tested on Linux. It requires `libsnappy` and `libtool`. 

- To install on Linux, please use `sudo apt-get install libsnappy-dev libtool`. 
- For MacOSX, use `brew install snappy` and instead of `ldconfig`, use `update_dyld_shared_cache`.

HyperPebblesDB was built, compiled, and tested with g++-4.7, g++-4.9, and g++-5. It may not work with other versions of g++ and other C++ compilers. 

### Installation

```
$ cd HyperPebblesDB/ 
$ autoreconf -i  
$ ./configure
$ make 
$ make install  
$ldconfig
```

### Caveat

- HyperPebblesDB is not fully-compatible with RocksDB. It lacks of features list under [Features Not in LevelDB](https://github.com/facebook/rocksdb/wiki/Features-Not-in-LevelDB).
 
